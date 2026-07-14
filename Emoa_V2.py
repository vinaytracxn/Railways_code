"""
extract_moa_to_sheet.py

Reads PDF links from column E of a Google Sheet, downloads each PDF using a
saved Tracxn browser session (cookies), extracts the objects clause, and writes
the result into column F of the same row.

Also writes the extraction status (e.g., "Clause3a", "Skipped", "Error") into
column G.
"""

import os
import json
import re
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

import fitz  # PyMuPDF
import requests
import gspread
from google.oauth2.service_account import Credentials

# ------------------------- CONFIG -------------------------
GOOGLE_SERVICE_ACCOUNT_INFO = json.loads(os.environ["GOOGLE_SERVICE_ACCOUNT_JSON"])
TRACXN_SESSION_DATA = json.loads(os.environ["TRACXN_SESSION_JSON"])

INPUT_SPREADSHEET_ID = os.environ["SHEET_ID"]
INPUT_SHEET_NAME = "Altered MOA"

CIN_COL_LETTER = "A"
LINK_COL_LETTER = "E"
OUTPUT_COL_LETTER = "F"
STATUS_COL_LETTER = "G"
START_ROW = 2
BATCH_SIZE = 100
WRITE_BATCH_SIZE = 100
MAX_WORKERS = 8
REQUEST_DELAY_SECONDS = 0.3

# Limit to 30,000 characters per cell to prevent API errors and excessive cell sizes
MAX_CELL_CHARS = 30000
# ------------------------------------------------------------

_thread_local = threading.local()
successful_cins = set()


def col_letter_to_index(letter: str) -> int:
    return ord(letter.upper()) - ord("A") + 1


def build_requests_session(cookies: list[dict]) -> requests.Session:
    sess = requests.Session()
    for c in cookies:
        sess.cookies.set(
            c["name"],
            c["value"],
            domain=c.get("domain", ""),
            path=c.get("path", "/"),
        )
    sess.headers.update(
        {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/124.0.0.0 Safari/537.36"
            )
        }
    )
    return sess


FALLBACK_STOP_PATTERNS = [
    re.compile(r"^4\b.*LIABILITY OF THE MEMBER"),
    re.compile(r"^SUBSCRIBER DETAILS"),
    re.compile(r"^SIGNED BEFORE ME"),
]

MAX_CAPTURED_LINES_IF_NO_STOP_MARKER = 1000
PAGE_FOOTER_PATTERN = re.compile(r"^PAGE\s+\d+\s+OF\s+\d+$")
RESTART_AT_ONE_PATTERN = re.compile(r"^1[\.\)]")
LABEL_CONTINUATION_PATTERN = re.compile(r"CLAUSE\s*3\s*\(\s*A\s*\)", re.IGNORECASE)
NUMERIC_ITEM_PATTERN = re.compile(r"^(\d{1,2})[\.\)](?:\s|$)")
NO_NUMERIC_3A_GAP_THRESHOLD = 10


def extract_main_objects_from_pdf_bytes(pdf_bytes: bytes) -> tuple[str, str | None]:
    doc = fitz.open(stream=pdf_bytes, filetype="pdf")
    try:
        lines = []
        for page in doc:
            for raw in page.get_text("text").splitlines():
                s = raw.strip()
                if s:
                    lines.append(s)
    finally:
        doc.close()

    start_idx = None
    stop_idx = None
    last_item_num = 0
    for i, line in enumerate(lines):
        u = line.upper()
        if start_idx is None:
            if "REGISTERED OFFICE OF THE COMPANY" in u:
                start_idx = i + 1
            continue

        m = NUMERIC_ITEM_PATTERN.match(line)
        if m:
            num = int(m.group(1))
            if num == 1:
                if last_item_num >= 2:
                    stop_idx = i
                    break
                if last_item_num == 0 and (i - start_idx) > NO_NUMERIC_3A_GAP_THRESHOLD:
                    stop_idx = i
                    break
            last_item_num = num

    if start_idx is not None and stop_idx is not None and stop_idx > start_idx:
        out = []
        for line in lines[start_idx:stop_idx]:
            u = line.upper()
            if PAGE_FOOTER_PATTERN.match(u):
                continue
            if "OBJECTS TO BE PURSUED BY THE COMPANY" in u:
                continue
            out.append(line)
        result = " ".join(out).strip()
        if result:
            return result, None

    capture = False
    out = []
    for line in lines:
        u = line.upper()
        if not capture:
            if "(A)" in u and "OBJECT" in u:
                capture = True
            continue
        if u.startswith("(B)"):
            break
        if any(p.match(u) for p in FALLBACK_STOP_PATTERNS):
            break
        if PAGE_FOOTER_PATTERN.match(u):
            continue
        out.append(line)

    result = " ".join(out).strip()
    if result:
        return result, None
    return "", "NOT FOUND"


def fetch_pdf_bytes(sess: requests.Session, url: str) -> bytes:
    resp = sess.get(url, timeout=30)
    resp.raise_for_status()

    if not resp.content[:1024].lstrip().startswith(b"%PDF") and b"%PDF" not in resp.content[:2048]:
        raise RuntimeError(
            "Response doesn't look like a PDF (likely redirected to a login page)"
        )
    return resp.content


def get_thread_session(cookies: list[dict]) -> requests.Session:
    if not hasattr(_thread_local, "session"):
        _thread_local.session = build_requests_session(cookies)
    return _thread_local.session


def process_cin_group(cin: str, rows_list: list[tuple[int, str]], cookies: list[dict]) -> list[tuple[int, str, str]]:
    results = []
    sess = get_thread_session(cookies)

    for row, link in rows_list:
        if cin and cin in successful_cins:
            results.append((row, "Skipped - CIN already extracted", "Skipped"))
            continue

        try:
            pdf_bytes = fetch_pdf_bytes(sess, link)
            extracted, warning = extract_main_objects_from_pdf_bytes(pdf_bytes)

            if warning:
                cell_value = f"[{warning}]\n{extracted}" if extracted else f"[{warning}]"
                status_value = f"Warning: {warning}"
            else:
                if extracted:
                    cell_value = extracted
                    status_value = "Clause3a"
                else:
                    cell_value = "NOT FOUND"
                    status_value = "Not Found"

            is_success = bool(extracted and not warning)
            if is_success and cin:
                successful_cins.add(cin)

        except Exception as exc:
            cell_value = f"ERROR: {exc}"
            status_value = "Error"

        results.append((row, cell_value, status_value))
        time.sleep(REQUEST_DELAY_SECONDS)

    return results


def main():
    creds = Credentials.from_service_account_info(
        GOOGLE_SERVICE_ACCOUNT_INFO,
        scopes=["https://www.googleapis.com/auth/spreadsheets"],
    )
    gc = gspread.authorize(creds)
    ws = gc.open_by_key(INPUT_SPREADSHEET_ID).worksheet(INPUT_SHEET_NAME)

    cin_col_idx = col_letter_to_index(CIN_COL_LETTER)
    link_col_idx = col_letter_to_index(LINK_COL_LETTER)
    out_col_idx = col_letter_to_index(OUTPUT_COL_LETTER)

    col_cin_values = ws.col_values(cin_col_idx)
    col_link_values = ws.col_values(link_col_idx)
    col_out_values = ws.col_values(out_col_idx)

    cookies = TRACXN_SESSION_DATA.get("cookies", [])

    for i, out_val in enumerate(col_out_values):
        out_str = out_val.strip()
        if out_str and not out_str.startswith("Skipped") and not out_str.startswith("[") and not out_str.startswith(
                "ERROR"):
            if i < len(col_cin_values):
                cin_val = col_cin_values[i].strip()
                if cin_val:
                    successful_cins.add(cin_val)

    rows_to_process = []
    for row in range(START_ROW, len(col_link_values) + 1):
        cin = col_cin_values[row - 1].strip() if row - 1 < len(col_cin_values) else ""
        link = col_link_values[row - 1].strip() if row - 1 < len(col_link_values) else ""
        already_done = col_out_values[row - 1].strip() if row - 1 < len(col_out_values) else ""

        if link and not already_done:
            rows_to_process.append((row, cin, link))

    print(f"{len(rows_to_process)} row(s) to process out of {len(col_link_values)} total.")
    print(f"Pre-loaded {len(successful_cins)} already successful CINs.")

    for chunk_start in range(0, len(rows_to_process), BATCH_SIZE):
        chunk = rows_to_process[chunk_start: chunk_start + BATCH_SIZE]
        print(f"\n=== Window: rows {chunk[0][0]}-{chunk[-1][0]} ({len(chunk)} row(s)) ===")

        for sub_start in range(0, len(chunk), WRITE_BATCH_SIZE):
            sub_chunk = chunk[sub_start: sub_start + WRITE_BATCH_SIZE]
            print(f"\n--- Sub-batch: rows {sub_chunk[0][0]}-{sub_chunk[-1][0]} ---")

            cin_groups = {}
            empty_cin_counter = 0
            for row, cin, link in sub_chunk:
                group_key = cin
                if not group_key:
                    group_key = f"__empty_{empty_cin_counter}__"
                    empty_cin_counter += 1

                if group_key not in cin_groups:
                    cin_groups[group_key] = []
                cin_groups[group_key].append((row, link))

            results_dict = {}
            with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
                futures = []
                for c_key, r_list in cin_groups.items():
                    actual_cin = "" if c_key.startswith("__empty_") else c_key
                    futures.append(executor.submit(process_cin_group, actual_cin, r_list, cookies))

                for future in as_completed(futures):
                    try:
                        group_results = future.result()
                        for row, text_val, status_val in group_results:
                            results_dict[row] = (text_val, status_val)
                    except Exception as exc:
                        print(f"Unexpected error in thread: {exc}")

            batch_updates = []
            for row, cin, link in sub_chunk:
                cell_value, status_value = results_dict.get(row, ("ERROR: Missing result", "Error"))

                # --- TRUNCATE TEXT IF OVER 30,000 CHARS ---
                if len(cell_value) > MAX_CELL_CHARS:
                    cell_value = cell_value[:MAX_CELL_CHARS] + "\n\n...[TRUNCATED DUE TO 30K CHAR LIMIT]"
                    if status_value == "Clause3a":
                        status_value = "Clause3a (Truncated)"
                    elif not status_value.endswith("(Truncated)"):
                        status_value = f"{status_value} (Truncated)"
                # ----------------------------------------

                preview = cell_value[:60].replace("\n", " ")

                print(f"  Row {row}: [{status_value}] {preview}...")

                batch_updates.append({"range": f"{OUTPUT_COL_LETTER}{row}", "values": [[cell_value]]})
                batch_updates.append({"range": f"{STATUS_COL_LETTER}{row}", "values": [[status_value]]})

            ws.batch_update(batch_updates, value_input_option="RAW")
            print(f"  -> wrote {len(batch_updates)} cell(s)")

    print("\nDone.")


if __name__ == "__main__":
    main()