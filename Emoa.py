import os
import json
import io
import re
import gc
import json
import time
import string
import requests
import PyPDF2
import gspread

try:
    import pymupdf as fitz
except ImportError:
    import fitz
import pytesseract
from PIL import Image

# Keep decompression-bomb protection ON (removed unlimited override) to avoid
# unbounded memory allocation on malformed/huge PDF pages.
Image.MAX_IMAGE_PIXELS = 200_000_000  # ~200MP safety ceiling instead of unlimited

from concurrent.futures import ThreadPoolExecutor, as_completed
from google.oauth2.service_account import Credentials
import google.auth.transport.requests as google_requests

# ---------------- CONFIG (edit these directly for local runs) ----------------

# Path to the downloaded Google service-account JSON key file.
# Needs: Sheets API + Drive API enabled in its GCP project, edit access to
# the target spreadsheet, and read access to whatever Drive files/folders
# hold the PDFs referenced in the "Drive link" column (share those with the
# service account's email, or use a shared drive).
# GOOGLE_SERVICE_ACCOUNT_FILE = "/Users/vinay/Desktop/json/ss.json"
#
# # The Google Sheet to read/write.
# INPUT_SPREADSHEET_ID = "1G55fLiITwFTI826GAHFJkxC8ve19vVKaiyH8B-aTjWk"
# INPUT_SHEET_NAME = "Sheet1"
#
# # First data row to start processing from (row 1 is assumed to be the header).
# START_ROW = 2
# ---------------- Railway Configuration ----------------

GOOGLE_SERVICE_ACCOUNT_INFO = json.loads(
    os.environ["GOOGLE_SERVICE_ACCOUNT_JSON"]
)

INPUT_SPREADSHEET_ID = os.environ["SHEET_ID"]
INPUT_SHEET_NAME = os.environ["SHEET_NAME"]

START_ROW = int(os.getenv("START_ROW", "2"))

with open(GOOGLE_SERVICE_ACCOUNT_INFO, "r") as _f:
    GOOGLE_SERVICE_ACCOUNT_INFO = json.load(_f)

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

# ---- Sheet header names (must match your header row exactly) ----
CIN_HEADER = "CIN"
LE_NAME_HEADER = "LE Name"
DISPLAY_NAME_HEADER = "Display Name"
DATE_HEADER = "Date"
LINK_HEADER = "Link"                 # old Tracxn link column -- kept, not used for fetching
DRIVE_LINK_HEADER = "Drive link"     # this is what we now fetch PDFs from
EXTRACTION_HEADER = "extraction"
STATUS_HEADER = "extraction_status"

STATUS_CLAUSE_MATCHED = "clause 3a"
STATUS_FULL_EXTRACT = "Full Extract"
STATUS_NONE = ""
STATUS_SKIPPED = "Skipped - Same CIN"
STATUS_NO_LINK = "No Drive link"
STATUS_BAD_LINK = "Could not parse Drive link"

# ---- MEMORY / CONCURRENCY TUNING ----
PREFETCH_WORKERS = 4
EXTRACTION_WORKERS = 6
INPUT_BATCH_SIZE = 40

SHEETS_MAX_RETRIES = 5
SHEETS_RETRY_BASE_DELAY = 5

DRIVE_MAX_RETRIES = 4
DRIVE_RETRY_BASE_DELAY = 3
# -----------------------------------------

DRIVE_SCOPES = [
    "https://www.googleapis.com/auth/drive.readonly",
]
SHEETS_SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]


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


# ---------------- AUTH ----------------

def gspread_auth():
    creds = Credentials.from_service_account_info(GOOGLE_SERVICE_ACCOUNT_INFO, scopes=SHEETS_SCOPES)
    return gspread.authorize(creds)


def drive_credentials():
    """Separate Credentials object (readonly Drive scope) that we refresh
    manually and reuse across threads for downloading file bytes."""
    creds = Credentials.from_service_account_info(GOOGLE_SERVICE_ACCOUNT_INFO, scopes=DRIVE_SCOPES)
    creds.refresh(google_requests.Request())
    return creds


def drive_auth_header(creds: Credentials) -> dict:
    # Refresh if close to/at expiry so long-running batches don't 401 midway.
    if not creds.valid:
        creds.refresh(google_requests.Request())
    return {"Authorization": f"Bearer {creds.token}"}


# ---------------- DRIVE FILE FETCHING ----------------

_DRIVE_ID_PATTERNS = [
    r"/file/d/([a-zA-Z0-9_-]{10,})",   # https://drive.google.com/file/d/<ID>/view
    r"[?&]id=([a-zA-Z0-9_-]{10,})",    # https://drive.google.com/open?id=<ID> or uc?id=<ID>
    r"/document/d/([a-zA-Z0-9_-]{10,})",
    r"/uc\?export=download&id=([a-zA-Z0-9_-]{10,})",
]


def extract_drive_file_id(link: str) -> str:
    """Pulls the Drive file ID out of any common Google Drive share-link
    format. Falls back to treating the whole string as an ID if it already
    looks like a bare ID (no slashes/scheme)."""
    if not link:
        return ""
    for pattern in _DRIVE_ID_PATTERNS:
        match = re.search(pattern, link)
        if match:
            return match.group(1)
    if "/" not in link and "://" not in link:
        return link.strip()
    return ""


def download_drive_file_bytes(creds: Credentials, file_id: str) -> bytes:
    url = f"https://www.googleapis.com/drive/v3/files/{file_id}"
    params = {"alt": "media", "supportsAllDrives": "true"}

    last_exc = None
    for attempt in range(1, DRIVE_MAX_RETRIES + 1):
        try:
            response = requests.get(
                url, headers=drive_auth_header(creds), params=params, timeout=60
            )
            response.raise_for_status()
            return response.content
        except requests.exceptions.RequestException as e:
            last_exc = e
            if attempt == DRIVE_MAX_RETRIES:
                raise
            time.sleep(DRIVE_RETRY_BASE_DELAY * (2 ** (attempt - 1)))
    raise last_exc


def prefetch_drive_files(creds: Credentials, links: list) -> dict:
    """Downloads bytes for each distinct Drive link in parallel.
    Returns {link: bytes} -- links that fail to parse/download are omitted."""
    results = {}
    if not links:
        return results

    def _fetch_one(link):
        file_id = extract_drive_file_id(link)
        if not file_id:
            print(f"  [drive] Could not parse a file ID from link: {link}")
            return link, None
        try:
            return link, download_drive_file_bytes(creds, file_id)
        except Exception as e:
            print(f"  [drive] Download failed for {link} (file_id={file_id}): {e}")
            return link, None

    with ThreadPoolExecutor(max_workers=PREFETCH_WORKERS) as executor:
        futures = [executor.submit(_fetch_one, link) for link in links]
        for future in as_completed(futures):
            link, pdf_bytes = future.result()
            if pdf_bytes is not None:
                results[link] = pdf_bytes

    return results


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

OCR_DPI = 200
OCR_LANGUAGE = "eng"


def ocr_pdf_bytes(pdf_bytes: bytes) -> str:
    text_parts = []
    doc = fitz.open(stream=pdf_bytes, filetype="pdf")
    try:
        zoom = OCR_DPI / 72
        matrix = fitz.Matrix(zoom, zoom)
        for page in doc:
            pix = page.get_pixmap(matrix=matrix, colorspace=fitz.csGRAY)
            img = Image.open(io.BytesIO(pix.tobytes("png")))
            try:
                page_text = pytesseract.image_to_string(img, lang=OCR_LANGUAGE)
            finally:
                img.close()
            text_parts.append(page_text)
            pix = None
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


def extract_doc_text(pdf_bytes: bytes) -> tuple:
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


# ---------------- UTILS ----------------

def parse_header(header_row):
    indices = {}
    for i, col in enumerate(header_row):
        name = col.strip()
        if name == CIN_HEADER and CIN_HEADER not in indices:
            indices[CIN_HEADER] = i
        elif name == LE_NAME_HEADER and LE_NAME_HEADER not in indices:
            indices[LE_NAME_HEADER] = i
        elif name == DISPLAY_NAME_HEADER and DISPLAY_NAME_HEADER not in indices:
            indices[DISPLAY_NAME_HEADER] = i
        elif name == DATE_HEADER and DATE_HEADER not in indices:
            indices[DATE_HEADER] = i
        elif name == LINK_HEADER and LINK_HEADER not in indices:
            indices[LINK_HEADER] = i
        elif name == DRIVE_LINK_HEADER and DRIVE_LINK_HEADER not in indices:
            indices[DRIVE_LINK_HEADER] = i
        elif name == EXTRACTION_HEADER and EXTRACTION_HEADER not in indices:
            indices[EXTRACTION_HEADER] = i
        elif name == STATUS_HEADER and STATUS_HEADER not in indices:
            indices[STATUS_HEADER] = i

    required = [CIN_HEADER, DRIVE_LINK_HEADER, EXTRACTION_HEADER, STATUS_HEADER]
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
    drive_creds = drive_credentials()

    header_row = call_with_retry(input_sheet.row_values, 1)
    if not header_row:
        print("Input sheet has no header row.")
        return

    indices = parse_header(header_row)
    cin_idx = indices[CIN_HEADER]
    drive_link_idx = indices[DRIVE_LINK_HEADER]
    extraction_idx = indices[EXTRACTION_HEADER]
    status_idx = indices[STATUS_HEADER]

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
        del all_cins, all_statuses
    except Exception as e:
        print(f"Warning: Initial status pre-scan failed ({e}). Proceeding without seeding memory.")

    current_row = START_ROW
    print(f"\n--- Starting processing from row {START_ROW} ---")

    while current_row <= total_rows_in_sheet:
        # Scoped per-batch so extracted text for previous batches is freed.
        extracted_result_cache = {}  # drive_link -> (extract_text, status_value)

        batch_start = current_row
        batch_end = min(current_row + INPUT_BATCH_SIZE - 1, total_rows_in_sheet)

        range_name = f"A{batch_start}:{last_col_letter}{batch_end}"
        chunk_rows = call_with_retry(input_sheet.get, range_name)

        if not chunk_rows:
            break

        print(f"\n=== Batch: input rows {batch_start}-{batch_end} ({len(chunk_rows)} fetched) ===")

        row_infos = []  # (sheet_row_number, cin, drive_link)
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

            drive_link = safe_get(row, drive_link_idx)
            if not drive_link:
                skipped_no_link += 1
                continue

            row_infos.append((sheet_row_number, cin, drive_link))
            unique_links.add(drive_link)

        if not any_data_in_chunk:
            print("  No CIN values found in this batch -- stopping.")
            break

        if skipped_already_processed:
            print(f"  Skipping {skipped_already_processed} row(s) already processed.")
        if rows_to_skip:
            print(f"  Skipping {len(rows_to_skip)} row(s) because their CIN already has clause 3a.")
        if skipped_no_link:
            print(f"  Skipping {skipped_no_link} row(s) with no Drive link value.")

        if not row_infos and not rows_to_skip:
            print("  Nothing new to process or mark in this batch.")
            current_row = batch_end + 1
            del chunk_rows, row_infos, rows_to_skip, unique_links, extracted_result_cache
            gc.collect()
            continue

        # PREFETCH & EXTRACT
        if unique_links:
            prefetch_cache = prefetch_drive_files(drive_creds, unique_links)

            unresolved_links = unique_links - set(prefetch_cache.keys())
            for link in unresolved_links:
                extracted_result_cache[link] = (
                    STATUS_BAD_LINK if not extract_drive_file_id(link) else "Content not available",
                    STATUS_NONE,
                )

            docs_to_extract = sorted(link for link in prefetch_cache.keys())

            if docs_to_extract:
                with ThreadPoolExecutor(max_workers=EXTRACTION_WORKERS) as executor:
                    future_to_link = {
                        executor.submit(extract_doc_text, prefetch_cache[link]): link
                        for link in docs_to_extract
                    }
                    for future in as_completed(future_to_link):
                        link = future_to_link[future]
                        try:
                            extract_text, status_value = future.result()
                        except Exception as e:
                            print(f"  [extract] Extraction failed for {link}: {e}")
                            extract_text, status_value = "Content not available", STATUS_NONE
                        extracted_result_cache[link] = (extract_text, status_value)

            prefetch_cache.clear()
            del prefetch_cache

        # ASSEMBLE NEW VALUES
        new_extraction_values = {}
        new_status_values = {}

        for sheet_row_number, cin in rows_to_skip:
            new_extraction_values[sheet_row_number] = "Skipped"
            new_status_values[sheet_row_number] = STATUS_SKIPPED

        for sheet_row_number, cin, drive_link in row_infos:
            if cin in successful_cins:
                new_extraction_values[sheet_row_number] = "Skipped"
                new_status_values[sheet_row_number] = STATUS_SKIPPED
                continue

            extract_text, status_value = extracted_result_cache.get(
                drive_link, ("Content not available", STATUS_NONE)
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

        del (
            chunk_rows,
            row_infos,
            rows_to_skip,
            unique_links,
            extracted_result_cache,
            new_extraction_values,
            new_status_values,
        )
        gc.collect()

        current_row = batch_end + 1

    print("\nDone.")


if __name__ == "__main__":
    process_sheet()