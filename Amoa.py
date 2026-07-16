import os
import io
import re
import gc
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

# ---------------- CONFIG ----------------
GOOGLE_SERVICE_ACCOUNT_INFO = json.loads(os.environ["GOOGLE_SERVICE_ACCOUNT_JSON"])
TRACXN_SESSION_DATA = json.loads(os.environ["TRACXN_SESSION_JSON"])

INPUT_SPREADSHEET_ID = os.environ["SHEET_ID"]
INPUT_SHEET_NAME = "Sheet1"

START_MARKERS = [
    "the registered office of the company"
]

OBJECTS_INCIDENTAL_TEMPLATE = (
    r"(?:the[\s\W]*)?"
    r"object[s]?[\s\W]*"
    r"(?:incidental[\s\W]*)?"
    r"(?:(?:or|and)[\s\W]*)?"
    r"(?:ancillary[\s\W]*)?"
    r"to[\s\W]*"
    r"(?:the[\s\W]*)?"
    r"attainment[\s\W]*of[\s\W]*"
    r"(?:the[\s\W]*)?"
    r"(?:above[\s\W]*)?"
    r"(?:main[\s\W]*)?"
    r"objects"
)

INCIDENTAL_FIRST_TEMPLATE = (
    r"incidental[\s\W]*"
    r"(?:(?:or|and)[\s\W]*)?"
    r"(?:ancillary[\s\W]*)?"
    r"object[s]?[\s\W]*"
    r"to[\s\W]*"
    r"(?:the[\s\W]*)?"
    r"attainment[\s\W]*of[\s\W]*"
    r"(?:the[\s\W]*)?"
    r"(?:main[\s\W]*)?"
    r"object[s]?"
)

OTHER_END_MARKERS = [
    "Matters which are necessary for furtherance of the objects specified in clause",
    "The furtherence of the object specified in clause",
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
STATUS_SKIPPED = "Skipped"

# --- MEMORY OPTIMIZATIONS ---
PREFETCH_WORKERS = 5  # Reduced from 8
INPUT_BATCH_SIZE = 40  # Reduced from 100 to prevent holding too many PDFs in RAM
EXTRACTION_WORKERS = 5  # Reduced from 25 (OCR & Playwright are highly memory intensive)
# ----------------------------

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


def gspread_auth():
    scope = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = Credentials.from_service_account_info(GOOGLE_SERVICE_ACCOUNT_INFO, scopes=scope)
    return gspread.authorize(creds)


def load_cookies_from_session(session_data: dict) -> dict:
    return {c["name"]: c["value"] for c in session_data.get("cookies", [])}


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
        download = download_info.value
        download_path = download.path()
        with open(download_path, "rb") as f:
            content = f.read()
    finally:
        page.close()
        gc.collect()  # Free Playwright page memory

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
        gc.collect()

    if "data" not in captured:
        raise Exception("No PDF response was captured while loading the document page")

    return io.BytesIO(captured["data"])


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


def is_pdf_scanned_precheck(pdf_bytes: bytes) -> bool:
    try:
        reader = PyPDF2.PdfReader(io.BytesIO(pdf_bytes), strict=False)
        total_chars = 0
        pages_to_check = min(3, len(reader.pages))
        for i in range(pages_to_check):
            page_text = reader.pages[i].extract_text()
            if page_text:
                total_chars += len(re.sub(r"\s+", "", page_text))
            if total_chars >= MIN_TEXT_CHARS_FOR_TEXT_PDF:
                return False
        return True
    except Exception:
        return True


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

            # --- MEMORY OPTIMIZATION ---
            # Aggressively delete the heavy uncompressed images page-by-page
            del img
            del pix
            gc.collect()
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


def get_full_extract_playwright(context, doc_url: str) -> str:
    page = context.new_page()
    try:
        page.goto(doc_url, wait_until="networkidle", timeout=45000)
        page.wait_for_timeout(2000)
        text = page.evaluate("document.body.innerText")
        return text if text else ""
    except Exception:
        return ""
    finally:
        page.close()


def extract_main_objects(text: str) -> str:
    def to_flexible_pattern(marker: str) -> str:
        return r"\s+".join(re.escape(word) for word in marker.split())

    start_alternation = "|".join(to_flexible_pattern(m) for m in START_MARKERS)
    other_end_alternation = "|".join(to_flexible_pattern(m) for m in OTHER_END_MARKERS)
    end_alternation = f"(?:{OBJECTS_INCIDENTAL_TEMPLATE})|(?:{INCIDENTAL_FIRST_TEMPLATE})|(?:{other_end_alternation})"

    pattern = re.compile(
        rf"(?:{start_alternation})\s*(.*?)\s*(?:{end_alternation})",
        re.IGNORECASE | re.DOTALL,
    )
    match = pattern.search(text)

    if not match:
        return ""
    extracted = re.sub(r"\s+", " ", match.group(1)).strip()
    return clean_marker_artifacts(extracted)


def clean_marker_artifacts(text: str) -> str:
    text = text.strip()
    text = re.sub(r"^[:\-\u2013\u2014.)\]\s]+", "", text)
    text = re.sub(r"[(\[]?\s*[a-z]\s*[)\]]?\s*\*?\s*$", "", text, flags=re.IGNORECASE)
    return text.strip()


def build_extraction_result(raw_text: str) -> tuple:
    raw_text = raw_text.lower()
    objects_text = extract_main_objects(raw_text)
    if objects_text:
        return objects_text, STATUS_CLAUSE_MATCHED

    full_text = re.sub(r"\s+", " ", raw_text).strip()
    return full_text, STATUS_FULL_EXTRACT


def extract_doc_text_parallel(context, cookies: dict, browser_lock: threading.Lock,
                              doc_url: str, prefetched_bytes: bytes = None) -> tuple:
    pdf_bytes = None
    final_text = ""

    if prefetched_bytes is not None:
        pdf_bytes = prefetched_bytes
    else:
        pdf_file = try_direct_download(doc_url, cookies)
        if pdf_file is not None:
            pdf_bytes = pdf_file.getvalue()
            pdf_file.close()  # Free buffer
        else:
            with browser_lock:
                try:
                    pdf_file = fetch_pdf_via_page(context, doc_url)
                    pdf_bytes = pdf_file.getvalue()
                    pdf_file.close()
                except Exception as e:
                    print(f"  [Info] Failed to capture PDF bytes")
                    pdf_bytes = None

    if pdf_bytes:
        is_scanned = is_pdf_scanned_precheck(pdf_bytes)

        if is_scanned:
            try:
                ocr_text = ocr_pdf_bytes(pdf_bytes)
                if not is_scanned_photo_pdf(ocr_text):
                    final_text = ocr_text
            except Exception as e:
                print(f"  OCR failed")
        else:
            final_text = extract_pdf_text(io.BytesIO(pdf_bytes))

            if is_unsupported_xfa_placeholder(final_text):
                return "Content not available", STATUS_NONE

            if is_scanned_photo_pdf(final_text):
                try:
                    ocr_text = ocr_pdf_bytes(pdf_bytes)
                    if not is_scanned_photo_pdf(ocr_text):
                        final_text = ocr_text
                except Exception as e:
                    pass

    # Free memory used by raw PDF bytes
    del pdf_bytes
    gc.collect()

    if not final_text or is_scanned_photo_pdf(final_text):
        with browser_lock:
            pw_text = get_full_extract_playwright(context, doc_url)

        if not is_scanned_photo_pdf(pw_text):
            final_text = pw_text
        else:
            return "Content not available", STATUS_NONE

    return build_extraction_result(final_text)


def prefetch_direct_downloads(links, cookies: dict) -> dict:
    results = {}
    if not links:
        return results

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
                pdf_file.close()  # Memory cleanup

    return results


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


def process_sheet():
    client = gspread_auth()
    input_sheet = client.open_by_key(INPUT_SPREADSHEET_ID).worksheet(INPUT_SHEET_NAME)

    header_row = call_with_retry(input_sheet.row_values, 1)
    indices = parse_header(header_row)

    cin_idx = indices[CIN_HEADER]
    link_idx = indices[LINK_HEADER]
    extraction_idx = indices[EXTRACTION_HEADER]
    status_idx = indices[STATUS_HEADER]

    cookies = load_cookies_from_session(TRACXN_SESSION_DATA)

    last_col_needed = max(indices.values())
    last_col_letter = col_num_to_letter(last_col_needed + 1)
    total_rows_in_sheet = input_sheet.row_count

    extracted_result_cache = {}
    successful_cins = set()

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(storage_state=TRACXN_SESSION_DATA)
        browser_lock = threading.Lock()

        current_row = 2
        while current_row <= total_rows_in_sheet:
            batch_start = current_row
            batch_end = min(current_row + INPUT_BATCH_SIZE - 1, total_rows_in_sheet)

            range_name = f"A{batch_start}:{last_col_letter}{batch_end}"
            chunk_rows = call_with_retry(input_sheet.get, range_name)

            if not chunk_rows:
                break

            print(f"\n=== Batch: input rows {batch_start}-{batch_end} ===")

            row_infos = []
            unique_links = set()

            rows_to_mark_skipped = []

            for offset, row in enumerate(chunk_rows):
                sheet_row_number = batch_start + offset
                cin = safe_get(row, cin_idx)
                if not cin: continue

                already_processed = bool(safe_get(row, extraction_idx))
                if already_processed:
                    if safe_get(row, status_idx) == STATUS_CLAUSE_MATCHED:
                        successful_cins.add(cin)
                    continue

                if cin in successful_cins:
                    rows_to_mark_skipped.append(sheet_row_number)
                    continue

                link = safe_get(row, link_idx)
                if not link: continue

                row_infos.append((sheet_row_number, link, cin))
                unique_links.add(link)

            if not row_infos:
                if rows_to_mark_skipped:
                    new_ex = {r: "Skipped" for r in rows_to_mark_skipped}
                    new_st = {r: STATUS_SKIPPED for r in rows_to_mark_skipped}
                    write_column_values(input_sheet, batch_start, batch_end, extraction_idx, chunk_rows, new_ex,
                                        EXTRACTION_HEADER)
                    write_column_values(input_sheet, batch_start, batch_end, status_idx, chunk_rows, new_st,
                                        STATUS_HEADER)
                current_row = batch_end + 1
                continue

            prefetch_cache = prefetch_direct_downloads(unique_links, cookies)

            docs_to_extract = sorted(link for link in unique_links if link not in extracted_result_cache)

            if docs_to_extract:
                with ThreadPoolExecutor(max_workers=EXTRACTION_WORKERS) as executor:
                    future_to_link = {}
                    for link in docs_to_extract:
                        # Memory Optimization: Use pop() so prefetch_cache drops the bytes the moment they are passed in
                        bytes_data = prefetch_cache.pop(link, None)
                        future = executor.submit(
                            extract_doc_text_parallel,
                            context, cookies, browser_lock, link,
                            bytes_data
                        )
                        future_to_link[future] = link

                    for future in as_completed(future_to_link):
                        link = future_to_link[future]
                        try:
                            extract_text, status_value = future.result()
                        except Exception as e:
                            extract_text, status_value = "Content not available", STATUS_NONE
                        extracted_result_cache[link] = (extract_text, status_value)

            new_extraction_values = {}
            new_status_values = {}

            for sheet_row_number in rows_to_mark_skipped:
                new_extraction_values[sheet_row_number] = "Skipped"
                new_status_values[sheet_row_number] = STATUS_SKIPPED

            for sheet_row_number, link, cin in row_infos:
                if cin in successful_cins:
                    new_extraction_values[sheet_row_number] = "Skipped"
                    new_status_values[sheet_row_number] = STATUS_SKIPPED
                    continue

                extract_text, status_value = extracted_result_cache.get(
                    link, ("Content not available", STATUS_NONE)
                )
                new_extraction_values[sheet_row_number] = truncate_for_sheet_cell(extract_text)
                new_status_values[sheet_row_number] = status_value

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

            # --- MEMORY OPTIMIZATION ---
            # Clear cache to avoid memory bloating over thousands of rows
            extracted_result_cache.clear()
            gc.collect()

        browser.close()


if __name__ == "__main__":
    process_sheet()