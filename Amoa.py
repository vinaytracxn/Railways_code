"""
Reads an input sheet where each row is ONE filed document:
    CIN | Display Name | Date | Link | extraction | extraction_status

For every row that has a CIN and a Link, downloads the PDF at Link (same
fast-download-then-Playwright-fallback approach as before) and extracts the
"Objects" clause (3.(a)) from the text, then writes:

  - into the `extraction` column (col E) of THAT SAME ROW:
        * the isolated Objects-clause text, when the START/END markers
          matched cleanly, OR
        * the FULL extracted text of the document (whatever text we got --
          normal extraction or OCR), when the markers did NOT match but we
          still got usable text, OR
        * "Content not available" / "Photo pdf" for the failure cases below

  - into the `extraction_status` column (col F) of THAT SAME ROW:
        * "clause 3a"     -- markers matched, `extraction` holds the
                              isolated Objects clause
        * "Full Extract"  -- markers did NOT match, `extraction` holds the
                              full raw extracted text instead
        * ""              -- for "Content not available" / "Photo pdf" rows
                              (nothing meaningful to label)

SCANNED/PHOTO PDFs: when a PDF has no embedded text layer, each page is
rasterized with PyMuPDF (fitz) and OCR'd with pytesseract automatically,
and the OCR output is searched for the Objects clause same as any other
document. OCR is much slower than normal text extraction (seconds per page
vs. near-instant), so a batch with a lot of scanned documents will take
noticeably longer.

There is no tier-based filtering anymore: every row with a document link
gets processed, regardless of what its Display Name says.

RESUME SUPPORT: a row is skipped if its `extraction` cell is already
non-empty, so re-running the script only fills in rows that don't have a
result yet.

PROCESSING MODE (batched):
  Input rows are read in chunks of BATCH_SIZE (100) at a time instead of
  loading the whole sheet into memory. Each chunk is matched, prefetched,
  extracted, and its `extraction` / `extraction_status` column values are
  written back before the next chunk is read.

IMPORTANT: run login_and_save_session.py first to create tracxn_session.json
(or re-run it if this script starts failing with auth/login-page errors --
that means the saved session has expired).

Install dependencies first:
    pip install playwright requests PyPDF2 gspread google-auth PyMuPDF pytesseract Pillow --break-system-packages
    playwright install chromium

Also install the Tesseract OCR engine itself (the pytesseract package is
just a Python wrapper around it):
    macOS:          brew install tesseract
    Ubuntu/Debian:  sudo apt-get install tesseract-ocr
    Windows:        https://github.com/UB-Mannheim/tesseract/wiki
"""

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
    # PyMuPDF's newer canonical import name -- avoids a name collision with
    # an unrelated PyPI package that is also literally called "fitz".
    import pymupdf as fitz
except ImportError:
    import fitz  # PyMuPDF -- rasterizes PDF pages to images for OCR
import pytesseract
from PIL import Image

# We rasterize our own PDFs at high DPI for OCR accuracy -- large-format
# pages (A2/A3-sized scans, etc.) at 300 DPI can legitimately exceed PIL's
# default decompression-bomb pixel threshold. This isn't an untrusted image
# upload, so it's safe to disable that safety cap here.
Image.MAX_IMAGE_PIXELS = None
from concurrent.futures import ThreadPoolExecutor, as_completed
from playwright.sync_api import sync_playwright
from google.oauth2.service_account import Credentials

# ---------------- CONFIG ----------------
# SERVICE_ACCOUNT_FILE = "/Users/vinay/Desktop/json/ss.json"
#
# INPUT_SPREADSHEET_ID = "1siNgkqlYwQpROQbf6mp8uIKXmyvCg3oFm8_L32PiJZw"
# INPUT_SHEET_NAME = "Altered MOA"
#
# SESSION_FILE = "tracxn_session.json"  # created by login_and_save_session.py

GOOGLE_SERVICE_ACCOUNT_INFO = json.loads(os.environ["GOOGLE_SERVICE_ACCOUNT_JSON"])
TRACXN_SESSION_DATA = json.loads(os.environ["TRACXN_SESSION_JSON"])

INPUT_SPREADSHEET_ID = os.environ["SHEET_ID"]
INPUT_SHEET_NAME = "Altered MOA"

# MOA documents phrase the "Objects" clause boundary differently depending on
# the era/format of the filing. START_MARKERS and END_MARKERS below list every
# known variant seen so far (leading numbering like "3 (a)", "III.", trailing
# punctuation/labels like ":", "3rd (a) are:", "(A)" are deliberately left out
# of these core phrases -- they're not needed for matching since we only
# search for the core phrase itself, wherever it sits in the surrounding
# numbering/punctuation).
START_MARKERS = [
    "The objects to be pursued by the company on its incorporation are",
    "The Objects for which the company is established are",
    "Main objects to be pursued by the company on its incorporation are",
    "The main objects to be pursued by the company on its incorporation are",
    "The main Objects for which the company is established are",
    "Main objects to be pursued on incorporation",
    "The objects to be pusued by the company are",
    "The object for which the company is established are",
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

# Column header names as they appear in row 1 of the input sheet
CIN_HEADER = "CIN"
DISPLAY_NAME_HEADER = "Display Name"
DATE_HEADER = "Date"
LINK_HEADER = "Link"
EXTRACTION_HEADER = "extraction"
STATUS_HEADER = "extraction_status"  # NEW -- col F: "clause 3a" / "Full Extract" / ""

# Status labels written to the STATUS_HEADER column
STATUS_CLAUSE_MATCHED = "clause 3a"
STATUS_FULL_EXTRACT = "Full Extract"
STATUS_NONE = ""

PREFETCH_WORKERS = 8    # parallel threads used for the fast direct-download pass
INPUT_BATCH_SIZE = 100  # how many input rows to read/process/write at a time
EXTRACTION_WORKERS = 25  # parallel threads used to extract/parse documents per batch

# Retry settings for Google Sheets API calls, which can fail transiently
# due to local network drops ("No route to host"), DNS hiccups, Google-side
# rate limiting (APIError 429), or brief outages (5xx).
SHEETS_MAX_RETRIES = 5
SHEETS_RETRY_BASE_DELAY = 5  # seconds; doubles each retry (5, 10, 20, 40, 80)
# -----------------------------------------

USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0 Safari/537.36"
)


def call_with_retry(func, *args, **kwargs):
    """Calls func(*args, **kwargs), retrying on transient network/API
    failures with exponential backoff. Used to wrap every Google Sheets API
    call so a brief network drop (e.g. 'No route to host') or a momentary
    Sheets API error doesn't kill an entire run after expensive extraction
    work has already been done for that batch."""
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

    credentials_info = json.loads(os.environ["GOOGLE_SERVICE_ACCOUNT_JSON"])

    creds = Credentials.from_service_account_info(
        credentials_info,
        scopes=scope
    )

    return gspread.authorize(creds)


def load_cookies_from_session():
    storage = json.loads(os.environ["TRACXN_SESSION_JSON"])

    cookie_dict = {}

    for c in storage["cookies"]:
        cookie_dict[c["name"]] = str(c["value"])

    return cookie_dict


# ---------------- PDF FETCHING (unchanged logic) ----------------

def try_direct_download(url: str, cookies: dict):
    """Fast path: a plain HTTP GET with the session cookies attached. Works
    for direct download links (e.g. /fm/dl/...). Returns a BytesIO if it
    got a real PDF, or None if it didn't (so we can fall back to Playwright)."""
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
    """Heuristics to spot the response that actually carries the PDF, without
    needing to know the exact endpoint in advance."""
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
    """For /fm/dl/... links opened in a real browser: the browser treats
    these as a direct file download rather than a page load, so we capture
    it via the download event instead of page.goto()."""
    page = context.new_page()
    try:
        with page.expect_download(timeout=60000) as download_info:
            try:
                page.goto(doc_url, timeout=60000)
            except Exception:
                pass  # goto() raises because navigation turned into a download -- expected
        download = download_info.value
        download_path = download.path()
        with open(download_path, "rb") as f:
            content = f.read()
    finally:
        page.close()

    if not content.startswith(b"%PDF"):
        print("  WARNING: Downloaded content does not look like a PDF.")
        print(f"  First 300 bytes: {content[:300]!r}")
        raise Exception("Downloaded content is not a valid PDF")

    return io.BytesIO(content)


def fetch_pdf_via_page(context, doc_url: str) -> io.BytesIO:
    """Loads the document page in a real browser tab and captures whichever
    network response turns out to be the actual PDF."""
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

    print(f"  Captured PDF from: {captured['url']}")
    return io.BytesIO(captured["data"])


def fetch_pdf(context, cookies: dict, doc_url: str, prefetched_bytes: bytes = None) -> io.BytesIO:
    """Tries the fast plain-HTTP path first; falls back to a real browser
    only if that doesn't yield a real PDF. If prefetched_bytes is provided
    (from the parallel prefetch pass), skips the direct-download attempt
    entirely since it's already known to have succeeded."""
    if prefetched_bytes is not None:
        return io.BytesIO(prefetched_bytes)

    pdf_file = try_direct_download(doc_url, cookies)
    if pdf_file is not None:
        print("  Downloaded directly via requests (no browser needed)")
        return pdf_file

    print("  Direct download didn't return a PDF -- falling back to Playwright...")
    return fetch_pdf_via_page(context, doc_url)


# ---------------- PDF TEXT EXTRACTION (unchanged logic) ----------------

PLACEHOLDER_SIGNATURE = "If this message is not eventually replaced by the proper contents"

# If PyPDF2 pulls out fewer than this many non-whitespace characters across
# the whole document, treat it as a scanned/photo PDF (no real text layer --
# i.e. the page images were never OCR'd) rather than a "markers not found"
# case.
MIN_TEXT_CHARS_FOR_TEXT_PDF = 30

# Google Sheets hard-caps a single cell at 50,000 characters. Stay safely
# under that so a long "Objects" clause (or the full-document fallback text)
# doesn't crash the write with a 400 error.
MAX_CELL_CHARS = 49500
TRUNCATION_SUFFIX = " ...[TRUNCATED -- exceeded Google Sheets 50,000 char cell limit]"


def truncate_for_sheet_cell(text: str) -> str:
    """Clips text so it fits in a single Google Sheets cell, appending a
    visible marker if it had to cut anything."""
    if len(text) <= MAX_CELL_CHARS:
        return text
    cutoff = MAX_CELL_CHARS - len(TRUNCATION_SUFFIX)
    return text[:cutoff] + TRUNCATION_SUFFIX


def is_unsupported_xfa_placeholder(text: str) -> bool:
    """Some PDFs are dynamic XFA forms with no real extractable text -- just
    a placeholder telling you your viewer can't render them. Detect that so
    we can report it clearly instead of a confusing 'markers not found'."""
    return PLACEHOLDER_SIGNATURE.lower() in text.lower()


def is_scanned_photo_pdf(text: str) -> bool:
    """A scanned/photographed document (no embedded text layer) will yield
    little to no text from PyPDF2's extraction -- as opposed to a normal
    text-based PDF where the Objects clause just doesn't match our markers.
    Distinguishing the two lets us report 'Photo pdf' instead of a
    misleading full-text dump."""
    non_whitespace_chars = len(re.sub(r"\s+", "", text))
    return non_whitespace_chars < MIN_TEXT_CHARS_FOR_TEXT_PDF


# ---------------- OCR FALLBACK (for scanned/photo PDFs) ----------------

OCR_DPI = 300          # higher = more accurate OCR but slower/more memory
OCR_LANGUAGE = "eng"

# If `tesseract --version` works in your terminal but the script still can't
# find it (e.g. a venv/IDE not inheriting your shell's PATH), uncomment and
# set this to the exact path from `which tesseract`:
pytesseract.pytesseract.tesseract_cmd = "/opt/homebrew/bin/tesseract"


def ocr_pdf_bytes(pdf_bytes: bytes) -> str:
    """Rasterizes every page of a PDF (via PyMuPDF) and runs Tesseract OCR
    on each page image, concatenating the results. Used as a fallback for
    scanned/photographed documents that have no embedded text layer."""
    text_parts = []
    doc = fitz.open(stream=pdf_bytes, filetype="pdf")
    try:
        zoom = OCR_DPI / 72  # fitz's default render is 72 DPI
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
    """Extract text between the earliest matching start marker and the
    earliest following end marker, tolerant of the inconsistent
    whitespace/line-breaks PDF extraction tends to introduce. Tries every
    known phrasing variant in START_MARKERS / END_MARKERS -- MOA documents
    from different eras/formats word this boundary differently.

    Returns "" if no start/end marker pair matched."""

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
        norm = re.sub(r"\s+", " ", text)
        idx = norm.lower().find("objects to be pursued")
        if idx == -1:
            idx = norm.lower().find("object")
        if idx != -1:
            print(f"    [debug] Nearby text: ...{norm[max(0, idx-30):idx+200]}...")
        else:
            print(f"    [debug] 'object' not found anywhere in extracted text "
                  f"({len(norm)} chars total). PDF text extraction may have failed.")
        return ""
    extracted = re.sub(r"\s+", " ", match.group(1)).strip()
    return clean_marker_artifacts(extracted)


def clean_marker_artifacts(text: str) -> str:
    """Strips stray boundary punctuation that sits just inside the captured
    span but outside the core marker phrases themselves -- e.g. a leftover
    ':' or '-' right after "...incorporation are:", or a leftover clause
    letter fragment like "(b)", "[ b ]", "*" right before the next clause's
    heading (e.g. "...above. (b) *Matters which are necessary...")."""
    text = text.strip()
    # Leading: colon / dash / closing bracket / bullet left over right after
    # a start marker like "...on its incorporation are:"
    text = re.sub(r"^[:\-\u2013\u2014.)\]\s]+", "", text)
    # Trailing: a stray single-letter clause marker like "(b)", "[ b ]", "*"
    # left over right before an end marker like "(b) *Matters which are..."
    text = re.sub(r"[(\[]?\s*[a-z]\s*[)\]]?\s*\*?\s*$", "", text, flags=re.IGNORECASE)
    return text.strip()


def build_extraction_result(raw_text: str) -> tuple:
    """Given the full extracted (or OCR'd) text of a document, decides what
    goes into the `extraction` column and the `extraction_status` column.

    Returns (extraction_value, status_value):
      - markers matched      -> (isolated Objects clause, "clause 3a")
      - markers didn't match -> (full raw text,            "Full Extract")
    """
    objects_text = extract_main_objects(raw_text)
    if objects_text:
        return objects_text, STATUS_CLAUSE_MATCHED

    full_text = re.sub(r"\s+", " ", raw_text).strip()
    return full_text, STATUS_FULL_EXTRACT


def extract_doc_text_parallel(context, cookies: dict, browser_lock: threading.Lock,
                               doc_url: str, prefetched_bytes: bytes = None) -> tuple:
    """Thread-safe extraction pipeline for use inside a ThreadPoolExecutor.

    Playwright's sync API is NOT thread-safe, so any call that touches the
    shared `context`/browser (i.e. the Playwright fallback path) must be
    serialized via `browser_lock`. Everything else -- prefetched bytes
    already in memory, or a fresh plain-HTTP direct-download attempt, plus
    all PDF parsing/regex/OCR work -- is pure Python and safe to run fully
    in parallel across threads without holding the lock.

    Returns (extraction_value, status_value) -- see build_extraction_result()
    and the module docstring for what each status value means."""
    if prefetched_bytes is not None:
        pdf_bytes = prefetched_bytes
    else:
        # Try a fresh direct download first -- still just plain HTTP via
        # `requests`, so no lock needed here either.
        pdf_file = try_direct_download(doc_url, cookies)
        if pdf_file is not None:
            pdf_bytes = pdf_file.getvalue()
        else:
            # Only the actual browser fallback needs to be serialized.
            with browser_lock:
                pdf_file = fetch_pdf_via_page(context, doc_url)
            pdf_bytes = pdf_file.getvalue()

    pdf_text = extract_pdf_text(io.BytesIO(pdf_bytes))

    if is_unsupported_xfa_placeholder(pdf_text):
        return "Content not available", STATUS_NONE

    if is_scanned_photo_pdf(pdf_text):
        # No embedded text layer -- fall back to OCR on the rasterized pages.
        try:
            ocr_text = ocr_pdf_bytes(pdf_bytes)
        except Exception as e:
            print(f"  OCR failed: {e}")
            return "Photo pdf", STATUS_NONE

        if is_scanned_photo_pdf(ocr_text):
            # OCR also came back essentially empty -- genuinely unreadable
            # scan (blank page, too low quality, etc.), not a code failure.
            return "Photo pdf", STATUS_NONE

        return build_extraction_result(ocr_text)

    return build_extraction_result(pdf_text)


def prefetch_direct_downloads(links, cookies: dict) -> dict:
    """Attempts the fast plain-HTTP download for many links concurrently.
    Pure network I/O with no browser involved, so this is safe to
    parallelize. Returns {link: bytes} for links that succeeded; links
    that fail (need the Playwright fallback) are simply absent from the
    returned dict."""
    results = {}
    if not links:
        return results

    print(f"  Prefetching {len(links)} unique document(s) via direct download "
          f"({PREFETCH_WORKERS} parallel workers)...")

    with ThreadPoolExecutor(max_workers=PREFETCH_WORKERS) as executor:
        future_to_link = {
            executor.submit(try_direct_download, link, cookies): link
            for link in links
        }
        done = 0
        for future in as_completed(future_to_link):
            link = future_to_link[future]
            done += 1
            try:
                pdf_file = future.result()
            except Exception:
                pdf_file = None
            if pdf_file is not None:
                results[link] = pdf_file.getvalue()
            if done % 25 == 0 or done == len(links):
                print(f"    Prefetch progress: {done}/{len(links)} "
                      f"({len(results)} succeeded direct)")

    print(f"  Prefetch done: {len(results)}/{len(links)} downloaded directly; "
          f"{len(links) - len(results)} will use the Playwright fallback.")
    return results


# ---------------- HEADER PARSING ----------------

def parse_header(header_row):
    """Finds the column index of each expected header, by name, so column
    order in the sheet doesn't matter. Raises if any required header is
    missing."""
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
        raise ValueError(f"Could not find column(s) {missing} in header row: {header_row}. "
                          f"Make sure the sheet has an '{STATUS_HEADER}' column (col F) "
                          f"in addition to '{EXTRACTION_HEADER}' (col E).")

    return indices


def safe_get(row, idx):
    return row[idx].strip() if idx is not None and idx < len(row) and row[idx] else ""


# ---------------- COLUMN LETTER HELPER ----------------

def col_num_to_letter(n: int) -> str:
    """1-indexed column number -> A1-style column letter(s)."""
    letters = ""
    while n > 0:
        n, remainder = divmod(n - 1, 26)
        letters = string.ascii_uppercase[remainder] + letters
    return letters


# ---------------- IN-PLACE COLUMN WRITE ----------------

def write_column_values(input_sheet, batch_start, batch_end, col_idx,
                         chunk_rows, new_values_by_row, label):
    """Writes a single column back for rows batch_start..batch_end.
    Rows present in new_values_by_row get their freshly computed value;
    any other row in the range keeps whatever was already in that cell
    (so skipped/already-processed/blank rows are left untouched)."""
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
    print(f"  --> Wrote {len(new_values_by_row)} {label} result(s) to column "
          f"{col_letter} ({batch_start}-{batch_end})")


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
    print(f"Header parsed: CIN col {cin_idx}, Link col {link_idx}, "
          f"extraction col {extraction_idx}, extraction_status col {status_idx}")

    cookies = load_cookies_from_session(SESSION_FILE)

    last_col_needed = max(indices.values())
    last_col_letter = col_num_to_letter(last_col_needed + 1)
    total_rows_in_sheet = input_sheet.row_count  # includes header + possible trailing blank rows

    # doc_link -> (extraction_value, status_value), shared across all batches
    extracted_result_cache = {}

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(storage_state=SESSION_FILE)
        browser_lock = threading.Lock()

        current_row = 2  # first data row (row 1 is header)
        while current_row <= total_rows_in_sheet:
            batch_start = current_row
            batch_end = min(current_row + INPUT_BATCH_SIZE - 1, total_rows_in_sheet)

            range_name = f"A{batch_start}:{last_col_letter}{batch_end}"
            chunk_rows = call_with_retry(input_sheet.get, range_name)

            if not chunk_rows:
                # No more data at all -- stop.
                break

            print(f"\n=== Batch: input rows {batch_start}-{batch_end} "
                  f"({len(chunk_rows)} fetched) ===")

            # ---- Pass 1 (in-memory): figure out which rows in this chunk
            # need extracting. A row is skipped if it has no CIN (trailing
            # blank row), no Link (nothing to extract), or already has a
            # non-empty `extraction` value (resume support).
            row_infos = []  # (sheet_row_number, link)
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

                already_processed = bool(safe_get(row, extraction_idx))
                if already_processed:
                    skipped_already_processed += 1
                    continue

                link = safe_get(row, link_idx)
                if not link:
                    skipped_no_link += 1
                    continue

                row_infos.append((sheet_row_number, link))
                unique_links.add(link)

            if not any_data_in_chunk:
                # Reached trailing blank rows -- nothing left to process.
                print("  No CIN values found in this batch -- stopping.")
                break

            if skipped_already_processed:
                print(f"  Skipping {skipped_already_processed} row(s) that already "
                      f"have an '{EXTRACTION_HEADER}' value (already processed).")
            if skipped_no_link:
                print(f"  Skipping {skipped_no_link} row(s) with no Link value.")

            if not row_infos:
                # Everything in this chunk was already processed or had no
                # link -- move on without touching prefetch/extraction/output.
                print("  Nothing new to process in this batch.")
                current_row = batch_end + 1
                continue

            print(f"  {len(row_infos)} document rows in this batch; "
                  f"{len(unique_links)} unique documents to extract.")

            # ---- Pass 2: prefetch this batch's unique links in parallel.
            prefetch_cache = prefetch_direct_downloads(unique_links, cookies)

            # ---- Pass 3: extract every not-yet-cached unique document in
            # this batch, up to EXTRACTION_WORKERS at a time. Most docs hit
            # the prefetch cache (or a fresh direct download), which is
            # pure in-memory/HTTP work and runs fully in parallel; only the
            # rare Playwright fallback is serialized via browser_lock inside
            # extract_doc_text_parallel.
            docs_to_extract = sorted(
                link for link in unique_links if link not in extracted_result_cache
            )
            print(f"  Extracting {len(docs_to_extract)} not-yet-cached document(s) "
                  f"with up to {EXTRACTION_WORKERS} parallel workers...")

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
                    done = 0
                    for future in as_completed(future_to_link):
                        link = future_to_link[future]
                        try:
                            extract_text, status_value = future.result()
                        except Exception as e:
                            extract_text, status_value = "Content not available", STATUS_NONE
                            print(f"  Error extracting doc (PDF/Playwright failure): {e}")
                        extracted_result_cache[link] = (extract_text, status_value)
                        done += 1
                        if done % 10 == 0 or done == len(docs_to_extract):
                            print(f"    Extraction progress: {done}/{len(docs_to_extract)}")

            # ---- Pass 4: assemble the new extraction/status values per row
            # from the (now fully populated) result cache. Pure in-memory work.
            new_extraction_values = {}
            new_status_values = {}
            for sheet_row_number, link in row_infos:
                extract_text, status_value = extracted_result_cache.get(
                    link, ("Content not available", STATUS_NONE)
                )
                new_extraction_values[sheet_row_number] = truncate_for_sheet_cell(extract_text)
                new_status_values[sheet_row_number] = status_value

            # ---- Write this batch's `extraction` and `extraction_status`
            # values in-place before moving to the next chunk of input rows.
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