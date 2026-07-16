import os
import io
import re
import json
import time
import string
import threading
import requests
import PyPDF2
import gspread

try:
    import pymupdf as fitz
except ImportError:
    import fitz
import pytesseract
from PIL import Image

Image.MAX_IMAGE_PIXELS = None
from concurrent.futures import ThreadPoolExecutor, as_completed
from playwright.sync_api import sync_playwright
from google.oauth2.service_account import Credentials

# ---------------- CONFIG / ENV ----------------
# Load authentication and session data from environment variables
GOOGLE_SERVICE_ACCOUNT_INFO = json.loads(os.environ["GOOGLE_SERVICE_ACCOUNT_JSON"])
TRACXN_SESSION_DATA = json.loads(os.environ["TRACXN_SESSION_JSON"])

INPUT_SPREADSHEET_ID = os.environ["SHEET_ID"]
INPUT_SHEET_NAME = "Altered MOA"

# Read the start row from env, default to 2 (first data row) if not provided
START_ROW = int(os.environ.get("START_ROW", "2"))

START_MARKERS = [
    "MEMORANDUM OF ASSOCIATION OF A COMPANY LIMITED BY SHARES"
]

END_MARKERS = [
    "Matters which are necessary for furtherance of the objects specified in clause",
    "The furtherence of the object specified in clause",
    "The Objects incidental or ancillary to the attainment of the above main objects",
    "The Objects incidental or ancillary to the attainment of the main objects",
    "Objects incidental or ancillary to the attainment of the main objects",
    "Objects incidental and ancillary to the attainment of the main objects",
    "Objects incidental to the attainment of the main objects",
    "The other objects not included in objects",
    "Objects and ancillary or",
    "Objects, ancillary or",
]

CIN_HEADER = "CIN"
DISPLAY_NAME_HEADER = "Display Name"
DATE_HEADER = "Date"
LINK_HEADER = "Link"
EXTRACTION_HEADER = "extraction"
STATUS_HEADER = "extraction_status"

STATUS_CLAUSE_MATCHED = "clause 3a"
STATUS_FULL_EXTRACT = "Full Extract"
STATUS_NONE = ""
STATUS_SKIPPED = "Skipped - Same CIN"

PREFETCH_WORKERS = 8
INPUT_BATCH_SIZE = 100
EXTRACTION_WORKERS = 25

SHEETS_MAX_RETRIES = 5
SHEETS_RETRY_BASE_DELAY = 5
# -----------------------------------------

USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0 Safari/537.36"
)


def call_with_retry(func, *args, **kwargs):
    last_exc = None
    for attempt in range(1, SHEETS_MAX_RETRIES + 1):
        try:
            return func(*args, **kwargs)
        except (
                requests.exceptions.RequestException,
                OSError,
                gspread.exceptions.APIError,
        ) as e:
            last_exc = e
            if attempt == SHEETS_MAX_RETRIES:
                break
            delay = SHEETS_RETRY_BASE_DELAY * (2 ** (attempt - 1))
            print(f"  [retry] Sheets API call failed (attempt {attempt}/"
                  f"{SHEETS_MAX_RETRIES}): {e}")
            print(f"  [retry] Retrying in {delay}s...")
            time.sleep(delay)
    raise last_exc


# ---------------- AUTH / SESSION ----------------

def gspread_auth():
    scope = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = Credentials.from_service_account_info(GOOGLE_SERVICE_ACCOUNT_INFO, scopes=scope)
    return gspread.authorize(creds)


def load_cookies_from_session_data(state: dict) -> dict:
    return {c["name"]: c["value"] for c in state.get("cookies", [])}


# ---------------- PDF FETCHING ----------------

def try_direct_download(url: str, cookies: dict):
    try:
        response = requests.get(
            url,
            cookies=cookies,
            headers={"User-Agent": USER_AGENT},
            timeout=30,
            allow_redirects=True,
        )
        if response.status_code == 200 and response.content.startswith(b"%PDF"):
            return io.BytesIO(response.content)
    except requests.RequestException:
        pass
    return None


def looks_like_pdf_response(response) -> bool:
    url = response.url
    content_type = response.headers.get("content-type", "")
    if "application/pdf" in content_type:
        return True
    if "/fm/dl/" in url:
        return True
    if "amazonaws.com" in url and "pdf" in url.lower():
        return True
    return False


def fetch_pdf_via_direct_download(context, doc_url: str) -> io.BytesIO:
    page = context.new_page()
    try:
        with page.expect_download(timeout=60000) as download_info:
            try:
                page.goto(doc_url, timeout=60000)
            except Exception:
                pass
        download_path = download_info.value.path()
        with open(download_path, "rb") as f:
            content = f.read()
    finally:
        page.close()

    if not content.startswith(b"%PDF"):
        raise Exception("Downloaded content is not a valid PDF")

    return io.BytesIO(content)


def fetch_pdf_via_page(context, doc_url: str) -> io.BytesIO:
    if "/fm/dl/" in doc_url:
        return fetch_pdf_via_direct_download(context, doc_url)

    captured = {}

    def handle_response(response):
        if "data" in captured: return
        if looks_like_pdf_response(response):
            try:
                body = response.body()
                if body and body.startswith(b"%PDF"):
                    captured["data"] = body
                    captured["url"] = response.url
            except Exception:
                pass

    page = context.new_page()
    page.on("response", handle_response)
    try:
        page.goto(doc_url, wait_until="networkidle", timeout=60000)
        page.wait_for_timeout(2000)
    finally:
        page.close()

    if "data" not in captured:
        raise Exception("No PDF response was captured while loading the document page")

    return io.BytesIO(captured["data"])


def fetch_pdf(context, cookies: dict, doc_url: str, prefetched_bytes: bytes = None) -> io.BytesIO:
    if prefetched_bytes is not None:
        return io.BytesIO(prefetched_bytes)

    pdf_file = try_direct_download(doc_url, cookies)
    if pdf_file is not None:
        return pdf_file

    return fetch_pdf_via_page(context, doc_url)


# ---------------- PDF TEXT EXTRACTION ----------------

PLACEHOLDER_SIGNATURE = "If this message is not eventually replaced by the proper contents"
MIN_TEXT_CHARS_FOR_TEXT_PDF = 30
MAX_CELL_CHARS = 49500
TRUNCATION_SUFFIX = " ...[TRUNCATED -- exceeded Google Sheets 50,000 char cell limit]"


def truncate_for_sheet_cell(text: str) -> str:
    if len(text) <= MAX_CELL_CHARS:
        return text
    cutoff = MAX_CELL_CHARS - len(TRUNCATION_SUFFIX)
    return text[:cutoff] + TRUNCATION_SUFFIX


def is_unsupported_xfa_placeholder(text: str) -> bool:
    return PLACEHOLDER_SIGNATURE.lower() in text.lower()


def is_scanned_photo_pdf(text: str) -> bool:
    non_whitespace_chars = len(re.sub(r"\s+", "", text))
    return non_whitespace_chars < MIN_TEXT_CHARS_FOR_TEXT_PDF


# ---------------- OCR FALLBACK ----------------

OCR_DPI = 300
OCR_LANGUAGE = "eng"


def ocr_pdf_bytes(pdf_bytes: bytes) -> str:
    text_parts = []
    doc = fitz.open(stream=pdf_bytes, filetype="pdf")
    try:
        zoom = OCR_DPI / 72
        matrix = fitz.Matrix(zoom, zoom)
        for page in doc:
            pix = page.get_pixmap(matrix=matrix)
            img = Image.open(io.BytesIO(pix.tobytes("png")))
            page_text = pytesseract.image_to_string(img, lang=OCR_LANGUAGE)
            text_parts.append(page_text)
    finally:
        doc.close()
    return "\n".join(text_parts)


def extract_pdf_text(pdf_file: io.BytesIO) -> str:
    reader = PyPDF2.PdfReader(pdf_file, strict=False)
    text = ""
    for page in reader.pages:
        try:
            page_text = page.extract_text()
            if page_text:
                text += page_text + "\n"
        except Exception:
            pass
    return text


def extract_main_objects(text: str) -> str:
    def to_flexible_pattern(marker: str) -> str:
        return r"\s+".join(re.escape(word) for word in marker.split())

    start_alternation = "|".join(to_flexible_pattern(m) for m in START_MARKERS)
    end_alternation = "|".join(to_flexible_pattern(m) for m in END_MARKERS)

    pattern = re.compile(
        rf"(?:{start_alternation})\s*(.*?)\s*(?:{end_alternation})",
        re.IGNORECASE | re.DOTALL,
    )
    match = pattern.search(text)
    if not match:
        return ""
    return re.sub(r"\s+", " ", match.group(1)).strip()


def build_extraction_result(raw_text: str) -> tuple:
    objects_text = extract_main_objects(raw_text)
    if objects_text:
        return objects_text, STATUS_CLAUSE_MATCHED

    full_text = re.sub(r"\s+", " ", raw_text).strip()
    return full_text, STATUS_FULL_EXTRACT


def extract_doc_text_parallel(context, cookies: dict, browser_lock: threading.Lock,
                              doc_url: str, prefetched_bytes: bytes = None) -> tuple:
    if prefetched_bytes is not None:
        pdf_bytes = prefetched_bytes
    else:
        pdf_file = try_direct_download(doc_url, cookies)
        if pdf_file is not None:
            pdf_bytes = pdf_file.getvalue()
        else:
            with browser_lock:
                pdf_file = fetch_pdf_via_page(context, doc_url)
            pdf_bytes = pdf_file.getvalue()

    pdf_text = extract_pdf_text(io.BytesIO(pdf_bytes))

    if is_unsupported_xfa_placeholder(pdf_text):
        return "Content not available", STATUS_NONE

    if is_scanned_photo_pdf(pdf_text):
        try:
            ocr_text = ocr_pdf_bytes(pdf_bytes)
        except Exception as e:
            return "Photo pdf", STATUS_NONE

        if is_scanned_photo_pdf(ocr_text):
            return "Photo pdf", STATUS_NONE
        return build_extraction_result(ocr_text)

    return build_extraction_result(pdf_text)


def prefetch_direct_downloads(links, cookies: dict) -> dict:
    results = {}
    if not links: return results

    with ThreadPoolExecutor(max_workers=PREFETCH_WORKERS) as executor:
        future_to_link = {
            executor.submit(try_direct_download, link, cookies): link
            for link in links
        }
        for future in as_completed(future_to_link):
            link = future_to_link[future]
            try:
                pdf_file = future.result()
            except Exception:
                pdf_file = None
            if pdf_file is not None:
                results[link] = pdf_file.getvalue()
    return results


# ---------------- UTILS ----------------

def parse_header(header_row):
    indices = {}
    for i, col in enumerate(header_row):
        name = col.strip()
        if name == CIN_HEADER and CIN_HEADER not in indices:
            indices[CIN_HEADER] = i
        elif name == DISPLAY_NAME_HEADER and DISPLAY_NAME_HEADER not in indices:
            indices[DISPLAY_NAME_HEADER] = i
        elif name == DATE_HEADER and DATE_HEADER not in indices:
            indices[DATE_HEADER] = i
        elif name == LINK_HEADER and LINK_HEADER not in indices:
            indices[LINK_HEADER] = i
        elif name == EXTRACTION_HEADER and EXTRACTION_HEADER not in indices:
            indices[EXTRACTION_HEADER] = i
        elif name == STATUS_HEADER and STATUS_HEADER not in indices:
            indices[STATUS_HEADER] = i

    required = [CIN_HEADER, LINK_HEADER, EXTRACTION_HEADER, STATUS_HEADER]
    missing = [h for h in required if h not in indices]
    if missing:
        raise ValueError(f"Could not find column(s) {missing} in header row.")
    return indices


def safe_get(row, idx):
    return row[idx].strip() if idx is not None and idx < len(row) and row[idx] else ""


def col_num_to_letter(n: int) -> str:
    letters = ""
    while n > 0:
        n, remainder = divmod(n - 1, 26)
        letters = string.ascii_uppercase[remainder] + letters
    return letters


def write_column_values(input_sheet, batch_start, batch_end, col_idx,
                        chunk_rows, new_values_by_row, label):
    col_letter = col_num_to_letter(col_idx + 1)
    values = []
    for offset, sheet_row in enumerate(range(batch_start, batch_end + 1)):
        if sheet_row in new_values_by_row:
            values.append([new_values_by_row[sheet_row]])
        else:
            existing = safe_get(chunk_rows[offset], col_idx) if offset < len(chunk_rows) else ""
            values.append([existing])

    range_name = f"{col_letter}{batch_start}:{col_letter}{batch_end}"
    call_with_retry(
        input_sheet.update,
        range_name=range_name, values=values, value_input_option="RAW",
    )
    print(f"  --> Wrote {len(new_values_by_row)} {label} result(s) to column {col_letter} ({batch_start}-{batch_end})")


# ---------------- MAIN ----------------

def process_sheet():
    client = gspread_auth()
    input_sheet = client.open_by_key(INPUT_SPREADSHEET_ID).worksheet(INPUT_SHEET_NAME)

    header_row = call_with_retry(input_sheet.row_values, 1)
    if not header_row:
        print("Input sheet has no header row.")
        return

    indices = parse_header(header_row)
    cin_idx = indices[CIN_HEADER]
    link_idx = indices[LINK_HEADER]
    extraction_idx = indices[EXTRACTION_HEADER]
    status_idx = indices[STATUS_HEADER]

    cookies = load_cookies_from_session_data(TRACXN_SESSION_DATA)
    last_col_letter = col_num_to_letter(max(indices.values()) + 1)
    total_rows_in_sheet = input_sheet.row_count

    # Pre-scan the sheet to build a set of CINs that ALREADY have clause 3a
    successful_cins = set()
    print("Pre-scanning sheet to map already successful CINs...")
    try:
        all_cins = call_with_retry(input_sheet.col_values, cin_idx + 1)
        all_statuses = call_with_retry(input_sheet.col_values, status_idx + 1)
        for c, s in zip(all_cins[1:], all_statuses[1:]):  # skip header
            if s.strip() == STATUS_CLAUSE_MATCHED:
                successful_cins.add(c.strip())
        print(f"Found {len(successful_cins)} CINs that already have '{STATUS_CLAUSE_MATCHED}'.")
    except Exception as e:
        print(f"Warning: Initial status pre-scan failed ({e}). Proceeding without seeding memory.")

    extracted_result_cache = {}

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(storage_state=TRACXN_SESSION_DATA)
        browser_lock = threading.Lock()

        current_row = START_ROW
        print(f"\n--- Starting processing from row {START_ROW} ---")

        while current_row <= total_rows_in_sheet:
            batch_start = current_row
            batch_end = min(current_row + INPUT_BATCH_SIZE - 1, total_rows_in_sheet)

            range_name = f"A{batch_start}:{last_col_letter}{batch_end}"
            chunk_rows = call_with_retry(input_sheet.get, range_name)

            if not chunk_rows:
                break

            print(f"\n=== Batch: input rows {batch_start}-{batch_end} ({len(chunk_rows)} fetched) ===")

            row_infos = []  # (sheet_row_number, cin, link)
            rows_to_skip = []  # (sheet_row_number, cin) -> to be written as "Skipped"
            unique_links = set()
            any_data_in_chunk = False
            skipped_already_processed = 0
            skipped_no_link = 0

            for offset, row in enumerate(chunk_rows):
                sheet_row_number = batch_start + offset
                cin = safe_get(row, cin_idx)
                if not cin:
                    continue
                any_data_in_chunk = True

                status_val = safe_get(row, status_idx)
                extracted_val = safe_get(row, extraction_idx)

                # If this row is the one that succeeded, add its CIN to our skip list
                if status_val == STATUS_CLAUSE_MATCHED:
                    successful_cins.add(cin)
                    skipped_already_processed += 1
                    continue

                # If we already have a success for this CIN, skip this row entirely
                if cin in successful_cins:
                    if extracted_val != "Skipped":
                        rows_to_skip.append((sheet_row_number, cin))
                    else:
                        skipped_already_processed += 1
                    continue

                if bool(extracted_val):
                    skipped_already_processed += 1
                    continue

                link = safe_get(row, link_idx)
                if not link:
                    skipped_no_link += 1
                    continue

                row_infos.append((sheet_row_number, cin, link))
                unique_links.add(link)

            if not any_data_in_chunk:
                print("  No CIN values found in this batch -- stopping.")
                break

            if skipped_already_processed:
                print(f"  Skipping {skipped_already_processed} row(s) already processed.")
            if rows_to_skip:
                print(f"  Skipping {len(rows_to_skip)} row(s) because their CIN already has clause 3a.")
            if skipped_no_link:
                print(f"  Skipping {skipped_no_link} row(s) with no Link value.")

            if not row_infos and not rows_to_skip:
                print("  Nothing new to process or mark in this batch.")
                current_row = batch_end + 1
                continue

            # PREFETCH & EXTRACT
            if unique_links:
                prefetch_cache = prefetch_direct_downloads(unique_links, cookies)
                docs_to_extract = sorted(link for link in unique_links if link not in extracted_result_cache)

                if docs_to_extract:
                    with ThreadPoolExecutor(max_workers=EXTRACTION_WORKERS) as executor:
                        future_to_link = {
                            executor.submit(
                                extract_doc_text_parallel,
                                context, cookies, browser_lock, link,
                                prefetch_cache.get(link),
                            ): link
                            for link in docs_to_extract
                        }
                        for future in as_completed(future_to_link):
                            link = future_to_link[future]
                            try:
                                extract_text, status_value = future.result()
                            except Exception as e:
                                extract_text, status_value = "Content not available", STATUS_NONE
                            extracted_result_cache[link] = (extract_text, status_value)

            # ASSEMBLE NEW VALUES
            new_extraction_values = {}
            new_status_values = {}

            # Apply skip logic for CINs that were successful before this batch started
            for sheet_row_number, cin in rows_to_skip:
                new_extraction_values[sheet_row_number] = "Skipped"
                new_status_values[sheet_row_number] = STATUS_SKIPPED

            # Apply results and in-batch skip logic
            for sheet_row_number, cin, link in row_infos:
                # Re-check if this CIN became successful during THIS batch's write-back loop
                if cin in successful_cins:
                    new_extraction_values[sheet_row_number] = "Skipped"
                    new_status_values[sheet_row_number] = STATUS_SKIPPED
                    continue

                extract_text, status_value = extracted_result_cache.get(
                    link, ("Content not available", STATUS_NONE)
                )
                new_extraction_values[sheet_row_number] = truncate_for_sheet_cell(extract_text)
                new_status_values[sheet_row_number] = status_value

                # If this row successfully extracted clause 3a, mark CIN to skip remaining docs
                if status_value == STATUS_CLAUSE_MATCHED:
                    successful_cins.add(cin)

            write_column_values(
                input_sheet, batch_start, batch_end, extraction_idx,
                chunk_rows, new_extraction_values, EXTRACTION_HEADER,
            )
            write_column_values(
                input_sheet, batch_start, batch_end, status_idx,
                chunk_rows, new_status_values, STATUS_HEADER,
            )

            current_row = batch_end + 1

        browser.close()

    print("\nDone.")


if __name__ == "__main__":
    process_sheet()