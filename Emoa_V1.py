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
from collections import OrderedDict
from concurrent.futures import ThreadPoolExecutor, as_completed
from playwright.sync_api import sync_playwright
from google.oauth2.service_account import Credentials

# ---------------- CONFIG ----------------
SERVICE_ACCOUNT_FILE = json.loads(os.environ["GOOGLE_SERVICE_ACCOUNT_JSON"])
INPUT_SPREADSHEET_ID = os.environ["SHEET_ID"]
INPUT_SHEET_NAME = "Altered MOA"
SESSION_FILE = json.loads(os.environ["TRACXN_SESSION_JSON"])

CIN_HEADER = "CIN"
DISPLAY_NAME_HEADER = "Display Name"
DATE_HEADER = "Date"
LINK_HEADER = "Link"
EXTRACTION_HEADER = "extraction"
STATUS_HEADER = "extraction_status"

STATUS_CLAUSE_MATCHED = "clause 3a"
STATUS_FULL_EXTRACT = "Full Extract"
STATUS_NONE = ""
STATUS_SKIPPED = "Skipped"

SKIPPED_EXTRACTION_TEXT = "Skipped - CIN already extracted"
FAILURE_EXTRACTION_VALUES = {"Content not available", "Photo pdf"}

PREFETCH_WORKERS = 8
INPUT_BATCH_SIZE = 100
EXTRACTION_WORKERS = 25

SHEETS_MAX_RETRIES = 5
SHEETS_RETRY_BASE_DELAY = 5

USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0 Safari/537.36"
)


def call_with_retry(func, *args, **kwargs):
    last_exc = None
    for attempt in range(1, SHEETS_MAX_RETRIES + 1):
        try:
            return func(*args, **kwargs)
        except (requests.exceptions.RequestException, OSError, gspread.exceptions.APIError) as e:
            last_exc = e
            if attempt == SHEETS_MAX_RETRIES:
                break
            delay = SHEETS_RETRY_BASE_DELAY * (2 ** (attempt - 1))
            print(f"  [retry] Sheets API call failed (attempt {attempt}/{SHEETS_MAX_RETRIES}): {e}")
            print(f"  [retry] Retrying in {delay}s...")
            time.sleep(delay)
    raise last_exc


# ---------------- AUTH / SESSION ----------------

def gspread_auth():
    scope = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]

    # Use the pre-loaded dictionary directly instead of a file path
    creds = Credentials.from_service_account_info(
        SERVICE_ACCOUNT_FILE,  # Pass the loaded JSON dict here
        scopes=scope
    )

    return gspread.authorize(creds)


def load_cookies_from_session():
    storage = json.loads(os.environ["TRACXN_SESSION_JSON"])
    cookie_dict = {}
    for c in storage["cookies"]:
        cookie_dict[c["name"]] = str(c["value"])
    return cookie_dict


# ---------------- PDF FETCHING ----------------

def try_direct_download(url: str, cookies: dict):
    try:
        response = requests.get(url, cookies=cookies, headers={"User-Agent": USER_AGENT}, timeout=30,
                                allow_redirects=True)
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
        download = download_info.value
        download_path = download.path()
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
        if "data" in captured:
            return
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
pytesseract.pytesseract.tesseract_cmd = "/opt/homebrew/bin/tesseract"


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


# ---------------- NEW EXTRACT LOGIC ----------------

def extract_main_objects(text: str) -> str:
    """
    Overhauled sequential parser that cleanly targets the Objects clause
    using exact string positioning and structural token signals.
    """
    lines = [line.strip() for line in text.split('\n')]
    FOOTER_PATTERN = re.compile(r'^PAGE\s+\d+\s+OF\s+\d+', re.IGNORECASE)

    # === 1. New INC-33 Format ===
    start_idx = None
    for i, line in enumerate(lines):
        if line == "REGISTERED OFFICE OF THE COMPANY":
            start_idx = i + 1
            break

    if start_idx is not None:
        NUMERIC_ITEM_PATTERN = re.compile(r'^(\d+)(?:\.|\))')
        NO_NUMERIC_3A_GAP_THRESHOLD = 10
        tracked_numbers = []
        stop_idx = None

        for i in range(start_idx, len(lines)):
            line = lines[i]
            match = NUMERIC_ITEM_PATTERN.match(line)
            if match:
                num = int(match.group(1))
                if num == 1:
                    if len(tracked_numbers) > 0:
                        # Condition 1: Numbering restarts at 1
                        stop_idx = i
                        break
                    else:
                        # Condition 2: First "1." found after generous gap
                        if (i - start_idx) > NO_NUMERIC_3A_GAP_THRESHOLD:
                            stop_idx = i
                            break
                tracked_numbers.append(num)

        if stop_idx is not None:
            extracted_lines = []
            for i in range(start_idx, stop_idx):
                line = lines[i]
                if FOOTER_PATTERN.match(line):
                    continue
                if "OBJECTS TO BE PURSUED BY THE COMPANY" in line:
                    continue
                extracted_lines.append(line)

            res = " ".join(extracted_lines).strip()
            if res:
                return re.sub(r"\s+", " ", res)

    # === 2. Old Format (and Fallback) ===
    capture = False
    extracted_lines = []

    FALLBACK_STOP_PATTERNS = [
        r'^4\s+.*LIABILITY OF THE MEMBER',
        r'^SUBSCRIBER DETAILS',
        r'^SIGNED BEFORE ME'
    ]

    for line in lines:
        if not capture:
            if "(A)" in line and "OBJECT" in line:
                capture = True
                continue
        else:
            if line.startswith("(B)"):
                break
            if any(re.search(pat, line, re.IGNORECASE) for pat in FALLBACK_STOP_PATTERNS):
                break
            if FOOTER_PATTERN.match(line):
                continue
            extracted_lines.append(line)

    if capture and extracted_lines:
        res = " ".join(extracted_lines).strip()
        return re.sub(r"\s+", " ", res)

    return ""


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
        except Exception:
            return "Photo pdf", STATUS_NONE

        if is_scanned_photo_pdf(ocr_text):
            return "Photo pdf", STATUS_NONE

        return build_extraction_result(ocr_text)

    return build_extraction_result(pdf_text)


def prefetch_direct_downloads(links, cookies: dict) -> dict:
    results = {}
    if not links:
        return results

    with ThreadPoolExecutor(max_workers=PREFETCH_WORKERS) as executor:
        future_to_link = {executor.submit(try_direct_download, link, cookies): link for link in links}
        for future in as_completed(future_to_link):
            link = future_to_link[future]
            try:
                pdf_file = future.result()
            except Exception:
                pdf_file = None
            if pdf_file is not None:
                results[link] = pdf_file.getvalue()
    return results


def extract_links_parallel(context, cookies: dict, browser_lock: threading.Lock,
                           links, extracted_result_cache: dict):
    links_to_fetch = sorted(set(links) - set(extracted_result_cache.keys()))
    if not links_to_fetch:
        return

    prefetch_cache = prefetch_direct_downloads(links_to_fetch, cookies)

    with ThreadPoolExecutor(max_workers=EXTRACTION_WORKERS) as executor:
        future_to_link = {
            executor.submit(
                extract_doc_text_parallel, context, cookies, browser_lock, link, prefetch_cache.get(link)
            ): link for link in links_to_fetch
        }
        for future in as_completed(future_to_link):
            link = future_to_link[future]
            try:
                extract_text, status_value = future.result()
            except Exception:
                extract_text, status_value = "Content not available", STATUS_NONE
            extracted_result_cache[link] = (extract_text, status_value)


# ---------------- HEADER PARSING & WRITE BACK ----------------

def parse_header(header_row):
    indices = {}
    for i, col in enumerate(header_row):
        name = col.strip()
        if name in (CIN_HEADER, DISPLAY_NAME_HEADER, DATE_HEADER, LINK_HEADER, EXTRACTION_HEADER, STATUS_HEADER):
            indices[name] = i

    required = [CIN_HEADER, LINK_HEADER, EXTRACTION_HEADER, STATUS_HEADER]
    missing = [h for h in required if h not in indices]
    if missing:
        raise ValueError(f"Missing column(s): {missing}")
    return indices


def safe_get(row, idx):
    return row[idx].strip() if idx is not None and idx < len(row) and row[idx] else ""


def col_num_to_letter(n: int) -> str:
    letters = ""
    while n > 0:
        n, remainder = divmod(n - 1, 26)
        letters = string.ascii_uppercase[remainder] + letters
    return letters


def write_column_values(input_sheet, batch_start, batch_end, col_idx, chunk_rows, new_values_by_row, label):
    col_letter = col_num_to_letter(col_idx + 1)
    values = []
    for offset, sheet_row in enumerate(range(batch_start, batch_end + 1)):
        if sheet_row in new_values_by_row:
            values.append([new_values_by_row[sheet_row]])
        else:
            existing = safe_get(chunk_rows[offset], col_idx) if offset < len(chunk_rows) else ""
            values.append([existing])

    range_name = f"{col_letter}{batch_start}:{col_letter}{batch_end}"
    call_with_retry(input_sheet.update, range_name=range_name, values=values, value_input_option="RAW")


# ---------------- MAIN ----------------

def process_sheet():
    client = gspread_auth()
    input_sheet = client.open_by_key(INPUT_SPREADSHEET_ID).worksheet(INPUT_SHEET_NAME)

    header_row = call_with_retry(input_sheet.row_values, 1)
    if not header_row:
        return

    indices = parse_header(header_row)
    cin_idx = indices[CIN_HEADER]
    link_idx = indices[LINK_HEADER]
    extraction_idx = indices[EXTRACTION_HEADER]
    status_idx = indices[STATUS_HEADER]

    cookies = load_cookies_from_session()
    last_col_needed = max(indices.values())
    last_col_letter = col_num_to_letter(last_col_needed + 1)
    total_rows_in_sheet = input_sheet.row_count

    extracted_result_cache = {}
    cin_satisfied = {}

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(storage_state=SESSION_FILE)
        browser_lock = threading.Lock()

        current_row = 2
        while current_row <= total_rows_in_sheet:
            batch_start = current_row
            batch_end = min(current_row + INPUT_BATCH_SIZE - 1, total_rows_in_sheet)

            range_name = f"A{batch_start}:{last_col_letter}{batch_end}"
            chunk_rows = call_with_retry(input_sheet.get, range_name)
            if not chunk_rows:
                break

            cin_docs = OrderedDict()
            any_data_in_chunk = False

            for offset, row in enumerate(chunk_rows):
                sheet_row_number = batch_start + offset
                cin = safe_get(row, cin_idx)
                if not cin:
                    continue
                any_data_in_chunk = True

                existing_extraction = safe_get(row, extraction_idx)
                if existing_extraction:
                    existing_status = safe_get(row, status_idx)
                    if existing_status in (STATUS_CLAUSE_MATCHED, STATUS_FULL_EXTRACT, STATUS_SKIPPED):
                        cin_satisfied[cin] = True
                    continue

                link = safe_get(row, link_idx)
                if not link:
                    continue

                cin_docs.setdefault(cin, []).append({"row": sheet_row_number, "link": link})

            if not any_data_in_chunk:
                break
            if not cin_docs:
                current_row = batch_end + 1
                continue

            new_extraction_values = {}
            new_status_values = {}
            pending = {cin: list(docs) for cin, docs in cin_docs.items()}

            for cin in list(pending.keys()):
                if cin_satisfied.get(cin):
                    for d in pending[cin]:
                        new_extraction_values[d["row"]] = SKIPPED_EXTRACTION_TEXT
                        new_status_values[d["row"]] = STATUS_SKIPPED
                    del pending[cin]

            while pending:
                round_targets = {}
                row_to_cin = {}
                for cin, docs in pending.items():
                    head = docs[0]
                    round_targets[head["row"]] = head["link"]
                    row_to_cin[head["row"]] = cin

                extract_links_parallel(context, cookies, browser_lock, round_targets.values(), extracted_result_cache)

                for row, link in round_targets.items():
                    cin = row_to_cin[row]
                    extract_text, status_value = extracted_result_cache.get(link,
                                                                            ("Content not available", STATUS_NONE))
                    pending[cin].pop(0)

                    if extract_text in FAILURE_EXTRACTION_VALUES and pending[cin]:
                        new_extraction_values[row] = extract_text
                        new_status_values[row] = status_value
                        continue

                    new_extraction_values[row] = truncate_for_sheet_cell(extract_text)
                    new_status_values[row] = status_value
                    if extract_text not in FAILURE_EXTRACTION_VALUES:
                        cin_satisfied[cin] = True
                        for d in pending[cin]:
                            new_extraction_values[d["row"]] = SKIPPED_EXTRACTION_TEXT
                            new_status_values[d["row"]] = STATUS_SKIPPED
                    del pending[cin]

            write_column_values(input_sheet, batch_start, batch_end, extraction_idx, chunk_rows, new_extraction_values,
                                EXTRACTION_HEADER)
            write_column_values(input_sheet, batch_start, batch_end, status_idx, chunk_rows, new_status_values,
                                STATUS_HEADER)

            current_row = batch_end + 1
        browser.close()


if __name__ == "__main__":
    process_sheet()