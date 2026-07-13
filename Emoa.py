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
        * "Skipped"       -- this row's CIN already had a usable result from
                              an earlier document, so this document was
                              never even downloaded; `extraction` holds
                              "Skipped - CIN already extracted"

PER-CIN CASCADE: a company (CIN) is often filed multiple times, so each CIN
can have several document rows. Rather than extracting every document for
every CIN, rows for the same CIN are tried in sheet order, one at a time --
as soon as one document yields a usable result ("clause 3a" or
"Full Extract"), the remaining documents for that CIN are marked "Skipped"
and never downloaded/extracted. A document only "falls through" to the next
one for the same CIN when it comes back as a genuine failure -- "Content not
available" or "Photo pdf" -- not merely because clause 3a's markers didn't
match (that still counts as a usable "Full Extract" result and stops the
cascade). This holds even if a CIN's documents happen to land in different
INPUT_BATCH_SIZE chunks, and also across separate runs of the script (a
CIN with an existing successful/skipped row is recognized on resume).

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
from collections import OrderedDict
from concurrent.futures import ThreadPoolExecutor, as_completed
from playwright.sync_api import sync_playwright
from google.oauth2.service_account import Credentials

# ---------------- CONFIG ----------------
SERVICE_ACCOUNT_FILE = json.loads(os.environ["GOOGLE_SERVICE_ACCOUNT_JSON"])

INPUT_SPREADSHEET_ID = os.environ["SHEET_ID"]
INPUT_SHEET_NAME = "Altered MOA"

SESSION_FILE = json.loads(os.environ["TRACXN_SESSION_JSON"])  # created by login_and_save_session.py

# extract_main_objects() below finds the Objects clause by locating the
# "REGISTERED OFFICE OF THE COMPANY" heading and then reading the numbered
# list that immediately follows it, rather than matching a start/end phrase
# pair -- MOA documents phrase those boundary phrases too inconsistently
# across filing eras/formats to match reliably.

# A line that starts a top-level numbered list entry, e.g. "1. To establish"
# or "1.To establish" (no space after the dot is common in these PDFs) --
# but NOT a sub-item like "1.1" (dot immediately followed by another digit).
NUMERIC_ITEM_PATTERN = re.compile(r"^\s*(\d{1,3})\.(?!\d)")

# A line that's nothing but a "Page X of Y" footer -- PDF extraction
# sometimes inserts these mid-clause; they're dropped rather than kept as
# part of the Objects text.
PAGE_FOOTER_PATTERN = re.compile(r"^\s*PAGE\s+\d+\s+OF\s+\d+\s*$")

# If no numbered item shows up within this many lines of the start marker,
# the sighting isn't trusted as "item 1" of the Objects list (it's more
# likely an unrelated numbered section further down a garbled/short
# extraction) -- extraction is treated as failed rather than scanning to
# the end of the document.
NO_NUMERIC_3A_GAP_THRESHOLD = 50

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
STATUS_SKIPPED = "Skipped"  # this row's CIN already had a successful extraction elsewhere

# Written to the EXTRACTION_HEADER cell for rows skipped because another
# document for the same CIN already produced a usable result.
SKIPPED_EXTRACTION_TEXT = "Skipped - CIN already extracted"

# extraction_value outcomes that count as "this document was not readable"
# for the purposes of the per-CIN cascade below -- these (and only these)
# are what trigger trying the *next* document for the same CIN. A
# "Full Extract" (markers just didn't match) still counts as a usable
# result and stops the cascade for that CIN.
FAILURE_EXTRACTION_VALUES = {"Content not available", "Photo pdf"}

PREFETCH_WORKERS = 8  # parallel threads used for the fast direct-download pass
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


# def load_cookies_from_session(session_file: str) -> dict:
#     """Load cookies out of a Playwright storage_state JSON file, in a form
#     usable directly by the `requests` library."""
#     with open(session_file, "r") as f:
#         state = json.load(f)
#     return {c["name"]: c["value"] for c in state.get("cookies", [])}


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
        print("Status:", response.status_code)
        print("Content-Type:", response.headers.get("Content-Type"))
        print("Length:", len(response.content))

        if response.status_code == 200:
            print(response.content[:100])
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

OCR_DPI = 300  # higher = more accurate OCR but slower/more memory
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
    """Extract the Objects clause by finding the "REGISTERED OFFICE OF THE
    COMPANY" heading and then reading the numbered list (1., 2., 3., ...)
    that follows it. Stops when either:
      - the numbering restarts at "1." after having reached at least "2."
        (a different numbered section, e.g. "objects incidental or
        ancillary", has begun), or
      - the first numbered item found is implausibly far (more than
        NO_NUMERIC_3A_GAP_THRESHOLD lines) from the start marker.
    If neither happens, the list is taken to run to the end of the text.

    Returns "" if the start marker isn't found, or if no numbered item is
    ever found after it (nothing usable to extract)."""

    lines = text.splitlines()

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
            if num == 1 and last_item_num >= 2:
                stop_idx = i
                break
            if last_item_num == 0 and (i - start_idx) > NO_NUMERIC_3A_GAP_THRESHOLD:
                stop_idx = i
                break
            last_item_num = num

    if start_idx is None or last_item_num == 0:
        # Never found the heading, or found it but never saw a single
        # numbered item after it -- nothing usable to extract.
        return ""

    if stop_idx is None:
        # Numbering never restarted and no gap-timeout fired -- the list
        # runs to the end of the extracted text.
        stop_idx = len(lines)

    if stop_idx <= start_idx:
        return ""

    out = []
    for line in lines[start_idx:stop_idx]:
        u = line.upper()
        if PAGE_FOOTER_PATTERN.match(u):
            continue
        if "OBJECTS TO BE PURSUED BY THE COMPANY" in u:
            continue
        out.append(line)

    return re.sub(r"\s+", " ", " ".join(out)).strip()


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
    print("PDF size:", len(pdf_bytes))
    print("First 20 bytes:", pdf_bytes[:20])
    print("Last 20 bytes:", pdf_bytes[-20:])
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


def extract_links_parallel(context, cookies: dict, browser_lock: threading.Lock,
                           links, extracted_result_cache: dict):
    """Prefetches + extracts every link in `links` that isn't already present
    in `extracted_result_cache`, mutating the cache in place with
    {link: (extraction_value, status_value)}. Used both for a plain batch
    pass and for each round of the per-CIN cascade in process_sheet()."""
    links_to_fetch = sorted(set(links) - set(extracted_result_cache.keys()))
    if not links_to_fetch:
        return

    prefetch_cache = prefetch_direct_downloads(links_to_fetch, cookies)

    print(f"  Extracting {len(links_to_fetch)} not-yet-cached document(s) "
          f"with up to {EXTRACTION_WORKERS} parallel workers...")

    with ThreadPoolExecutor(max_workers=EXTRACTION_WORKERS) as executor:
        future_to_link = {
            executor.submit(
                extract_doc_text_parallel,
                context, cookies, browser_lock, link,
                prefetch_cache.get(link),
            ): link
            for link in links_to_fetch
        }
        done = 0
        for future in as_completed(future_to_link):
            link = future_to_link[future]
            try:
                extract_text, status_value = future.result()
            except Exception:
                import traceback
                traceback.print_exc()
                extract_text, status_value = "Content not available", STATUS_NONE
            extracted_result_cache[link] = (extract_text, status_value)
            done += 1
            if done % 10 == 0 or done == len(links_to_fetch):
                print(f"    Extraction progress: {done}/{len(links_to_fetch)}")


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

    cookies = load_cookies_from_session()
    # cookies = load_cookies_from_session(SESSION_FILE)

    last_col_needed = max(indices.values())
    last_col_letter = col_num_to_letter(last_col_needed + 1)
    total_rows_in_sheet = input_sheet.row_count  # includes header + possible trailing blank rows

    # doc_link -> (extraction_value, status_value), shared across all batches
    extracted_result_cache = {}

    # CIN -> True once ANY document for that CIN has yielded a real,
    # non-failure extraction (in this run or a previous one, per the sheet).
    # Once True, remaining documents for that CIN are skipped rather than
    # extracted. Shared across all batches so a CIN whose docs happen to
    # straddle a batch boundary is still handled correctly.
    cin_satisfied = {}

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

            # ---- Pass 1 (in-memory): group this chunk's rows by CIN, in
            # sheet order, and figure out which ones are actual candidates
            # to extract. A row is excluded if it has no CIN (trailing blank
            # row), no Link (nothing to extract), or already has a
            # non-empty `extraction` value (resume support). While scanning
            # already-processed rows we also update `cin_satisfied` so a
            # CIN whose first document already succeeded (this run or a
            # previous one) has its other documents skipped.
            cin_docs = OrderedDict()  # cin -> [{"row": n, "link": url}, ...] in sheet order
            any_data_in_chunk = False
            skipped_already_processed = 0
            skipped_no_link = 0

            for offset, row in enumerate(chunk_rows):
                sheet_row_number = batch_start + offset
                cin = safe_get(row, cin_idx)
                if not cin:
                    continue
                any_data_in_chunk = True

                existing_extraction = safe_get(row, extraction_idx)
                if existing_extraction:
                    skipped_already_processed += 1
                    existing_status = safe_get(row, status_idx)
                    if existing_status in (STATUS_CLAUSE_MATCHED, STATUS_FULL_EXTRACT, STATUS_SKIPPED):
                        cin_satisfied[cin] = True
                    continue

                link = safe_get(row, link_idx)
                if not link:
                    skipped_no_link += 1
                    continue

                cin_docs.setdefault(cin, []).append({"row": sheet_row_number, "link": link})

            if not any_data_in_chunk:
                # Reached trailing blank rows -- nothing left to process.
                print("  No CIN values found in this batch -- stopping.")
                break

            if skipped_already_processed:
                print(f"  Skipping {skipped_already_processed} row(s) that already "
                      f"have an '{EXTRACTION_HEADER}' value (already processed).")
            if skipped_no_link:
                print(f"  Skipping {skipped_no_link} row(s) with no Link value.")

            if not cin_docs:
                # Everything in this chunk was already processed or had no
                # link -- move on without touching prefetch/extraction/output.
                print("  Nothing new to process in this batch.")
                current_row = batch_end + 1
                continue

            total_candidate_rows = sum(len(v) for v in cin_docs.values())
            print(f"  {total_candidate_rows} document row(s) across {len(cin_docs)} CIN(s) "
                  f"to consider (cascading per CIN: stop at the first usable document).")

            # ---- Pass 2/3 (cascading): for each CIN, try its documents in
            # sheet order, one at a time. Every CIN's "current" candidate is
            # extracted together in parallel each round. A CIN drops out of
            # the cascade once a document succeeds (its remaining docs are
            # marked Skipped) or it runs out of documents (the last failure
            # is recorded as the result).
            new_extraction_values = {}
            new_status_values = {}

            pending = {cin: list(docs) for cin, docs in cin_docs.items()}

            # CINs already known-satisfied (from an earlier batch or a
            # previous run) skip straight to "Skipped" with no extraction.
            for cin in list(pending.keys()):
                if cin_satisfied.get(cin):
                    for d in pending[cin]:
                        new_extraction_values[d["row"]] = SKIPPED_EXTRACTION_TEXT
                        new_status_values[d["row"]] = STATUS_SKIPPED
                    del pending[cin]

            round_num = 0
            while pending:
                round_num += 1
                round_targets = {}  # sheet_row_number -> link
                row_to_cin = {}
                for cin, docs in pending.items():
                    head = docs[0]
                    round_targets[head["row"]] = head["link"]
                    row_to_cin[head["row"]] = cin

                print(f"  Cascade round {round_num}: attempting {len(round_targets)} "
                      f"document(s) (one per still-open CIN)...")

                extract_links_parallel(
                    context, cookies, browser_lock,
                    round_targets.values(), extracted_result_cache,
                )

                for row, link in round_targets.items():
                    cin = row_to_cin[row]
                    extract_text, status_value = extracted_result_cache.get(
                        link, ("Content not available", STATUS_NONE)
                    )
                    pending[cin].pop(0)  # this document has now been attempted

                    if extract_text in FAILURE_EXTRACTION_VALUES and pending[cin]:
                        # Not readable, but there's another document for
                        # this CIN left to try next round.
                        new_extraction_values[row] = extract_text
                        new_status_values[row] = status_value
                        continue

                    # Either it succeeded, or it failed with nothing left to
                    # try -- either way this CIN's cascade is finished.
                    new_extraction_values[row] = truncate_for_sheet_cell(extract_text)
                    new_status_values[row] = status_value
                    if extract_text not in FAILURE_EXTRACTION_VALUES:
                        cin_satisfied[cin] = True
                        for d in pending[cin]:
                            new_extraction_values[d["row"]] = SKIPPED_EXTRACTION_TEXT
                            new_status_values[d["row"]] = STATUS_SKIPPED
                    del pending[cin]

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