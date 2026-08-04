import io
import json
import os
import re
import threading
import time
import xml.etree.ElementTree as ET
from concurrent.futures import ThreadPoolExecutor, as_completed

import fitz  # PyMuPDF
import gspread
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaIoBaseDownload

# ------------------------- CONFIG -------------------------

def load_service_account_infos() -> list[dict]:
    """Loads one or more service-account JSON blobs from env vars.
    Primary account: GOOGLE_SERVICE_ACCOUNT_JSON
    Additional accounts (optional, any number): GOOGLE_SERVICE_ACCOUNT_JSON_2,
    GOOGLE_SERVICE_ACCOUNT_JSON_3, ...
    Order matters -- accounts are tried in this order for Drive downloads,
    falling back to the next account when one lacks access to a file.
    """
    infos = []
    primary = os.environ.get("GOOGLE_SERVICE_ACCOUNT_JSON")
    if primary:
        infos.append(json.loads(primary))
    idx = 2
    while True:
        val = os.environ.get(f"GOOGLE_SERVICE_ACCOUNT_JSON_{idx}")
        if not val:
            break
        infos.append(json.loads(val))
        idx += 1
    if not infos:
        raise ValueError(
            "No service account JSON found. Set GOOGLE_SERVICE_ACCOUNT_JSON "
            "(and optionally GOOGLE_SERVICE_ACCOUNT_JSON_2, _3, ...)."
        )
    return infos


GOOGLE_SERVICE_ACCOUNT_INFOS = load_service_account_infos()
# Primary account is used for Sheets I/O (writing results, master sheet).
GOOGLE_SERVICE_ACCOUNT_INFO = GOOGLE_SERVICE_ACCOUNT_INFOS[0]

# --------- Master-workflow config ---------
# The master spreadsheet lists one child spreadsheet per row and tracks
# per-user claiming/progress so multiple runs/users can't double-process
# the same child sheet. See process_master_workflow() / main().
MASTER_SPREADSHEET_ID = "1oNr3g2Pjpyu9u09w0lCFVT9vJwbBGn8O2rbx4kvjd88"
MASTER_SHEET_NAME = "Sheet List"                # master tab name
CURRENT_USER = os.environ.get("USER_NAME")      # identifies this run's claims; set per user/machine

# Master sheet headers (in this order):
#   Doc Type | Sheet Link | User | Drive Link Status | extraction User |
#   Extarction status | XFA User | XFA Status
MASTER_SHEET_LINK_COL_LETTER = "B"
MASTER_XFA_USER_COL_LETTER = "G"
MASTER_XFA_STATUS_COL_LETTER = "H"
MASTER_START_ROW = 6                            # 1 if the master sheet has no header row

MASTER_STATUS_PROCESSING = "Processing"
MASTER_STATUS_COMPLETED = "Done"
MASTER_STATUS_FAILED = "Failed"

# Candidate child-sheet tab names, tried in this order.
CHILD_SHEET_NAME_CANDIDATES = ["Actual Sheet", "Sheet1"]

# Google Sheets URL / ID patterns, e.g.
#   https://docs.google.com/spreadsheets/d/SPREADSHEET_ID/edit#gid=0
SPREADSHEET_ID_PATTERNS = [
    re.compile(r"/spreadsheets/d/([a-zA-Z0-9_-]{20,})"),
    re.compile(r"[?&]id=([a-zA-Z0-9_-]{20,})"),
]
# --------- End master-workflow config ---------

CIN_COL_LETTER = "A"                            # CIN used to dedup rows
LINK_COL_LETTER = "F"                           # Google Drive link to the PDF
OUTPUT_COL_LETTER = "G"                         # extracted objects clause goes here
STATUS_COL_LETTER = "H"                         # marked "Clause 3a" once (re)processed
RETRY_TRIGGER_VALUE = "Content not available"   # only rows with G == this get reprocessed
STATUS_VALUE = "Clause 3a"                      # written to H after a successful (re)write to G
SKIP_VALUE = "Skipped"                          # written to G for duplicate-CIN rows
SKIP_STATUS_VALUE = "Skipped - Same CIN"        # written to H for duplicate-CIN rows

# Background color (RGB, 0-1 scale, as the Sheets API expects) used to
# highlight G:H on any row currently sitting at RETRY_TRIGGER_VALUE, so
# it's visually obvious which rows are still "Content not available".
CREAM_BACKGROUND_COLOR = {"red": 1.0, "green": 0.973, "blue": 0.863}  # ~ #FFF8DC
HIGHLIGHT_REQUEST_CHUNK_SIZE = 500              # requests per formatting batch_update call
START_ROW = 2                                   # 1 if no header row
BATCH_SIZE = 100                                # rows per outer processing window
WRITE_BATCH_SIZE = 25                            # rows per batch_update call (sub-chunk of BATCH_SIZE)
SKIP_WRITE_BATCH_SIZE = 200                      # rows per batch_update call for cheap duplicate-CIN skip writes
MAX_WORKERS = 8                                  # concurrent PDF downloads
# Small per-request delay, applied per worker thread rather than globally,
# so we're still polite to the Drive API without serializing everything.
REQUEST_DELAY_SECONDS = 0.3

# Retry settings for Drive downloads. 403/429/5xx are near-always transient
# (rate limiting or a momentary backend hiccup) and worth a few retries with
# backoff before giving up. A 404 ("File not found") is treated separately
# (see process_one_row) -- it's not retried, it's written straight to the
# sheet as "File Not Found".
MAX_DOWNLOAD_RETRIES = 4                # total attempts = 1 initial + this many retries
RETRY_BACKOFF_BASE_SECONDS = 2          # backoff: 2s, 4s, 8s, 16s (doubling each retry)
RETRYABLE_HTTP_STATUSES = {403, 429, 500, 502, 503, 504}

# Retry settings for Sheets API calls (read AND write -- batch_update,
# update_acell, col_values, open_by_key, worksheet, ...). Sheets enforces a
# per-user "write requests per minute" quota (60/min by default); a burst of
# many small batch_update calls back-to-back (e.g. writing thousands of
# duplicate-CIN skip rows in 25-row chunks) can blow through that quota
# well within a minute, and without retry the resulting 429 propagates
# straight up and fails the whole child sheet. Backoff starts at a full
# minute since the quota window is per-minute -- shorter waits just spend
# the retry budget hitting the same still-active limit again.
SHEETS_MAX_RETRIES = 6                          # total attempts = 1 initial + this many retries
SHEETS_RETRY_BACKOFF_BASE_SECONDS = 60          # backoff: 60s, 120s, 240s, ... doubling each retry
# Small fixed pacing delay between consecutive Sheets *write* calls issued
# back-to-back in a loop (e.g. each 25-row skip-marker chunk), to spread
# requests out and avoid tripping the per-minute quota in the first place
# rather than only reacting to it after the fact.
SHEETS_WRITE_PACING_SECONDS = 1.0

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive.readonly",
]
# ------------------------------------------------------------


def build_service_account_creds_list() -> list[Credentials]:
    """Turn each loaded service-account JSON blob (GOOGLE_SERVICE_ACCOUNT_INFOS)
    into a Credentials object, preserving order. creds_list[0] is the
    primary account (used for Sheets I/O); the full list is used for
    Drive-download fallback (see fetch_pdf_bytes_multi_account)."""
    return [
        Credentials.from_service_account_info(info, scopes=SCOPES)
        for info in GOOGLE_SERVICE_ACCOUNT_INFOS
    ]

_thread_local = threading.local()


def is_retryable_sheets_error(exc: Exception) -> bool:
    """True for rate-limit (429) and transient server (5xx) errors from the
    Sheets API. gspread raises gspread.exceptions.APIError, whose message
    embeds the underlying Google API error JSON -- status code parsing is
    best-effort (the response object isn't always present), so this also
    falls back to matching on the well-known quota-error text."""
    if isinstance(exc, gspread.exceptions.APIError):
        try:
            status = exc.response.status_code
            if status in RETRYABLE_HTTP_STATUSES:
                return True
        except Exception:
            pass
    msg = str(exc)
    return "429" in msg or "Quota exceeded" in msg or "RESOURCE_EXHAUSTED" in msg


def with_sheets_retry(func, *args, **kwargs):
    """Call any Sheets API function (gc.open_by_key, sh.worksheet,
    ws.col_values, ws.batch_update, ws.update_acell, ...) with exponential
    backoff on rate-limit/5xx errors. Non-retryable errors, and the last
    attempt's error once SHEETS_MAX_RETRIES is exhausted, are re-raised
    so callers' existing error handling still applies unchanged."""
    last_exc = None
    for attempt in range(SHEETS_MAX_RETRIES + 1):  # attempt 0 = first try
        try:
            return func(*args, **kwargs)
        except Exception as exc:
            last_exc = exc
            if not is_retryable_sheets_error(exc) or attempt == SHEETS_MAX_RETRIES:
                raise
            wait = SHEETS_RETRY_BACKOFF_BASE_SECONDS * (2 ** attempt)
            print(f"    [sheets retry] {type(exc).__name__}: {exc} -- "
                  f"attempt {attempt + 1}/{SHEETS_MAX_RETRIES + 1}, waiting {wait}s...")
            time.sleep(wait)

    raise last_exc if last_exc else RuntimeError("Unknown Sheets API failure")


def col_letter_to_index(letter: str) -> int:
    """'A' -> 1, 'D' -> 4, etc."""
    return ord(letter.upper()) - ord("A") + 1


# ===================== MASTER-WORKFLOW HELPERS =====================

def extract_spreadsheet_id_from_link(link: str) -> str:
    """Pull the spreadsheet ID out of a Google Sheets URL (or return the
    string as-is if it already looks like a bare ID). Raises ValueError if
    nothing usable is found, so a malformed master-sheet link surfaces as a
    clear per-row failure instead of a confusing API error downstream."""
    link = link.strip()
    for pattern in SPREADSHEET_ID_PATTERNS:
        m = pattern.search(link)
        if m:
            return m.group(1)
    # Bare ID already, most likely -- Sheets IDs are alphanumeric/_/- and
    # generally 30-44 chars, but be lenient and just require "looks like an
    # ID, not a URL".
    if link and "/" not in link and " " not in link:
        return link
    raise ValueError(f"Could not find a spreadsheet ID in Sheet Link: {link}")


def open_child_worksheet(gc, spreadsheet_id: str):
    """Open the child spreadsheet and pick its worksheet by name, trying
    each entry in CHILD_SHEET_NAME_CANDIDATES in order ('Actual Sheet'
    first, then 'Sheet1'). Raises gspread.WorksheetNotFound if none of the
    candidates exist."""
    sh = with_sheets_retry(gc.open_by_key, spreadsheet_id)
    last_exc = None
    for name in CHILD_SHEET_NAME_CANDIDATES:
        try:
            return sh, with_sheets_retry(sh.worksheet, name)
        except gspread.exceptions.WorksheetNotFound as exc:
            last_exc = exc
            continue
    raise last_exc if last_exc else gspread.exceptions.WorksheetNotFound(
        f"None of {CHILD_SHEET_NAME_CANDIDATES} found in spreadsheet {spreadsheet_id}"
    )


# ===================== GOOGLE DRIVE LINK / DOWNLOAD HELPERS =====================

# Covers the common shapes Drive links show up in:
#   https://drive.google.com/file/d/FILE_ID/view?usp=sharing
#   https://drive.google.com/open?id=FILE_ID
#   https://drive.google.com/uc?id=FILE_ID&export=download
#   https://docs.google.com/document/d/FILE_ID/edit  (just in case)
DRIVE_ID_PATTERNS = [
    re.compile(r"/file/d/([a-zA-Z0-9_-]{10,})"),
    re.compile(r"[?&]id=([a-zA-Z0-9_-]{10,})"),
    re.compile(r"/d/([a-zA-Z0-9_-]{10,})"),
]


def extract_drive_file_id(link: str) -> str:
    """Pull the Drive file ID out of any of the common link shapes above.
    Raises ValueError if none of the patterns match, so a malformed link
    surfaces as a clear per-row error instead of a confusing API failure."""
    for pattern in DRIVE_ID_PATTERNS:
        m = pattern.search(link)
        if m:
            return m.group(1)
    raise ValueError(f"Could not find a Drive file ID in link: {link}")


def build_drive_service(creds: Credentials):
    """Build a Drive API client. googleapiclient's Resource objects are not
    documented as thread-safe, so (like requests.Session before it) each
    worker thread gets its own rather than sharing one."""
    return build("drive", "v3", credentials=creds, cache_discovery=False)


def get_thread_drive_services(creds_list: list[Credentials]):
    """One Drive service per (worker thread, service account), built lazily
    and reused across that thread's subsequent tasks (avoids rebuilding
    them per PDF). Returns a list in the same order as creds_list, i.e.
    primary account first."""
    if not hasattr(_thread_local, "drive_services"):
        _thread_local.drive_services = [build_drive_service(c) for c in creds_list]
    return _thread_local.drive_services


def fetch_pdf_bytes_from_drive(drive_service, file_id: str) -> bytes:
    """Download a Drive file's raw bytes straight into memory (never
    touches disk). Single attempt, no retry -- see
    fetch_pdf_bytes_from_drive_with_retry for the retrying wrapper used by
    process_one_row."""
    request = drive_service.files().get_media(fileId=file_id)
    buf = io.BytesIO()
    downloader = MediaIoBaseDownload(buf, request)
    done = False
    while not done:
        _, done = downloader.next_chunk()
    pdf_bytes = buf.getvalue()

    if not pdf_bytes[:1024].lstrip().startswith(b"%PDF") and b"%PDF" not in pdf_bytes[:2048]:
        raise RuntimeError(
            "Downloaded content doesn't look like a PDF -- check that the "
            "file is actually a PDF and is shared with the service account."
        )
    return pdf_bytes


def fetch_pdf_bytes_from_drive_with_retry(drive_service, file_id: str) -> bytes:
    """Same as fetch_pdf_bytes_from_drive, but retries with exponential
    backoff on the error codes in RETRYABLE_HTTP_STATUSES (404/403/429/5xx).
    A fresh get_media() request is issued on every attempt.

    Re-raises the last error once MAX_DOWNLOAD_RETRIES is exhausted, so the
    caller's existing "ERROR: <exc>" handling still applies -- this only
    changes how many times we try before giving up.
    """
    last_exc = None
    for attempt in range(MAX_DOWNLOAD_RETRIES + 1):  # attempt 0 = first try
        try:
            return fetch_pdf_bytes_from_drive(drive_service, file_id)
        except HttpError as exc:
            status = exc.resp.status if getattr(exc, "resp", None) is not None else None
            last_exc = exc
            if status not in RETRYABLE_HTTP_STATUSES or attempt == MAX_DOWNLOAD_RETRIES:
                raise
            wait = RETRY_BACKOFF_BASE_SECONDS * (2 ** attempt)
            print(f"    [retry] file_id={file_id} got HTTP {status}, "
                  f"attempt {attempt + 1}/{MAX_DOWNLOAD_RETRIES + 1}, "
                  f"waiting {wait}s before retrying...")
            time.sleep(wait)
        except (ConnectionError, TimeoutError, OSError) as exc:
            # Network-level hiccups (not an HttpError at all) -- also worth
            # retrying rather than failing the row outright.
            last_exc = exc
            if attempt == MAX_DOWNLOAD_RETRIES:
                raise
            wait = RETRY_BACKOFF_BASE_SECONDS * (2 ** attempt)
            print(f"    [retry] file_id={file_id} got {type(exc).__name__}: {exc}, "
                  f"attempt {attempt + 1}/{MAX_DOWNLOAD_RETRIES + 1}, "
                  f"waiting {wait}s before retrying...")
            time.sleep(wait)

    # Should be unreachable (loop always returns or raises), but keep a
    # safety net so a logic slip can't silently return None.
    raise last_exc if last_exc else RuntimeError("Unknown download failure")


def fetch_pdf_bytes_multi_account(drive_services: list, file_id: str) -> bytes:
    """Try each configured service account's Drive service in order (see
    GOOGLE_SERVICE_ACCOUNT_JSON / _2 / _3 ... env vars) until one can
    successfully download the file. Within each account, transient errors
    (429/5xx, plus 403 which can also be a rate-limit) still get their full
    backoff-retry treatment via fetch_pdf_bytes_from_drive_with_retry. Only
    once an account is fully exhausted -- with a 403 ("no access") or 404
    ("not shared with this account, so Drive hides it entirely") -- do we
    fall through to the next account, since that's the scenario this
    fallback exists for: the same file/folder shared with some service
    accounts but not others.

    Raises the last account's exception once every account has been tried,
    so the caller's existing 404 -> "File Not Found" / other -> "ERROR: ..."
    handling in process_one_row still applies unchanged.
    """
    last_exc = None
    for idx, drive_service in enumerate(drive_services):
        try:
            return fetch_pdf_bytes_from_drive_with_retry(drive_service, file_id)
        except HttpError as exc:
            status = exc.resp.status if getattr(exc, "resp", None) is not None else None
            last_exc = exc
            if status in (403, 404) and idx < len(drive_services) - 1:
                print(f"    [account fallback] file_id={file_id} got HTTP {status} on "
                      f"account #{idx + 1}/{len(drive_services)}, trying account "
                      f"#{idx + 2}...")
                continue
            raise

    raise last_exc if last_exc else RuntimeError("No service accounts configured")


# ===================== XFA / "Please wait..." PDF HANDLING =====================
#
# Some INC-33 eMOA PDFs are dynamic XFA (Adobe LiveCycle) forms: the page's
# static content stream is nothing but a "Please wait..." placeholder, and
# the real content only ever gets rendered live by Adobe's XFA engine from
# XML packets embedded elsewhere in the file. PyMuPDF's get_text() (and any
# non-Adobe renderer) only ever sees that placeholder -- never the real
# text -- no matter how good the page-text heuristics below are. For these
# files we read the field values directly out of the XFA "datasets" XML
# packet instead, which is the same data Adobe renders from.


def is_xfa_dynamic_pdf(doc: "fitz.Document") -> bool:
    """True if this PDF is a dynamic XFA form (content lives in XFA XML
    packets, not on the page itself -- these are the ones that show
    'Please wait...' in non-Adobe viewers/extractors)."""
    try:
        flag = doc.xref_get_key(doc.pdf_catalog(), "NeedsRendering")
        return bool(flag) and flag[1] == "true"
    except Exception:
        return False


def get_xfa_datasets_xml(doc: "fitz.Document") -> bytes | None:
    """Pull the raw 'datasets' XFA packet (the actual filled-in field
    values) out of the PDF's /AcroForm /XFA array. Returns None if this
    isn't an XFA form or has no datasets packet."""
    try:
        acroform_xref = int(doc.xref_get_key(doc.pdf_catalog(), "AcroForm")[1].split()[0])
    except Exception:
        return None

    xfa_val = doc.xref_get_key(acroform_xref, "XFA")
    if not xfa_val or xfa_val[0] != "array":
        return None

    # The array is a flat "(name)xref 0 R (name)xref 0 R ..." list -- no
    # guaranteed whitespace after the closing paren, so don't require it.
    pairs = re.findall(r"\(([^)]+)\)\s*(\d+)\s+0\s+R", xfa_val[1])
    for name, xref_str in pairs:
        if name == "datasets":
            return doc.xref_stream(int(xref_str))
    return None


def extract_objects_clause_from_xfa(pdf_bytes: bytes) -> tuple[str, str | None]:
    """
    XFA-native alternative to text-scraping: reads field values directly out
    of the XFA 'datasets' XML packet instead of page text.

    This INC-33 eMOA template stores clause 3(a) content in one of five
    parallel field sets (TABLEA3A .. TABLEE3A, depending which lettered
    variant this filing used), with a selector field TABLE_M naming which
    letter is active. Falls back to scanning all five if TABLE_M is
    missing/unrecognized.

    Returns (text, warning). warning is:
      - None                                   -> clean hit via TABLE_M
      - "NOT_XFA"                               -> not a dynamic XFA form at
                                                    all; caller should fall
                                                    through to page-text logic
      - "NO_XFA_DATASETS" / "XFA_XML_PARSE_ERROR: ..." -> XFA form but
                                                    couldn't read/parse the
                                                    datasets packet
      - "TABLE_M_MISSING_FELL_BACK_TO_SCAN"    -> succeeded, but via the
                                                    less-certain fallback scan
      - "WARNING_MULTIPLE_3A_FIELDS_POPULATED(n)" -> ambiguous, needs review
      - "NOT_FOUND_IN_XFA_DATASETS"             -> XFA form, but no 3(a)
                                                    field had content
    """
    doc = fitz.open(stream=pdf_bytes, filetype="pdf")
    try:
        if not is_xfa_dynamic_pdf(doc):
            return "", "NOT_XFA"

        datasets_bytes = get_xfa_datasets_xml(doc)
        if not datasets_bytes:
            return "", "NO_XFA_DATASETS"

        try:
            root = ET.fromstring(datasets_bytes)
        except ET.ParseError as e:
            return "", f"XFA_XML_PARSE_ERROR: {e}"
    finally:
        doc.close()

    def find_field(tag_name: str):
        for el in root.iter():
            if el.tag.split("}")[-1] == tag_name:  # strip {namespace} if present
                return el
        return None

    active_letter = None
    m_el = find_field("TABLE_M")
    if m_el is not None and m_el.text and m_el.text.strip():
        active_letter = m_el.text.strip().upper()[0]

    def get_3a_text(letter: str) -> str:
        el = find_field(f"TABLE{letter}3A")
        return el.text.strip() if el is not None and el.text and el.text.strip() else ""

    if active_letter:
        text = get_3a_text(active_letter)
        if text:
            return text, None

    # Fallback: TABLE_M missing/wrong -- scan all five letters and use
    # whichever one is actually populated. More than one populated means
    # something's off with our assumption, so flag it for manual review
    # rather than silently picking one.
    populated = []
    for letter in "ABCDE":
        text = get_3a_text(letter)
        if text:
            populated.append((letter, text))

    if len(populated) == 1:
        return populated[0][1], "TABLE_M_MISSING_FELL_BACK_TO_SCAN"
    if len(populated) > 1:
        combined = "\n---\n".join(f"[{letter}] {text}" for letter, text in populated)
        return combined, f"WARNING_MULTIPLE_3A_FIELDS_POPULATED({len(populated)})"

    return "", "NOT_FOUND_IN_XFA_DATASETS"


# ===================== NON-XFA ("printed") PDF HANDLING =====================

# Fallback stop markers -- if the "(b)" marker is missing/misdetected, stop
# at the next section of the MoA instead of running all the way to the
# subscriber/signature pages (which contain PAN/DIN and other personal data
# we don't want leaking into the sheet).
FALLBACK_STOP_PATTERNS = [
    re.compile(r"^4\b.*LIABILITY OF THE MEMBER"),   # "4 The liability of the member(s)..."
    re.compile(r"^SUBSCRIBER DETAILS"),
    re.compile(r"^SIGNED BEFORE ME"),
]

# Hard cap as an absolute last resort, in case none of the fallback markers
# are found either (so a bad/unexpected PDF format can't blow up the sheet
# with the entire rest of the document). Set generously high -- legitimate
# objects clauses can run several hundred lines across many pages, so this
# should only ever bite on a genuinely unrecognized format, not a long list.
MAX_CAPTURED_LINES_IF_NO_STOP_MARKER = 1000


PAGE_FOOTER_PATTERN = re.compile(r"^PAGE\s+\d+\s+OF\s+\d+$")

# A genuine "(b) matters necessary for furtherance..." boundary is followed
# by its own answer restarting enumeration at "1." -- e.g. "1. To do all or
# any of the acts...". A false-positive "(b)" (the question label getting
# extracted mid-stream because the (a) answer box overflows across a page
# break, with the label sitting at the top of the next page's left column)
# is instead followed by content that continues an existing list item or
# sentence, and does NOT restart at "1.".
RESTART_AT_ONE_PATTERN = re.compile(r"^1[\.\)]")

# Lines that are just continuation of the "(b) ... in clause 3(a) are"
# label sentence, not real content -- skip over these while looking ahead.
LABEL_CONTINUATION_PATTERN = re.compile(r"CLAUSE\s*3\s*\(\s*A\s*\)", re.IGNORECASE)


# Real content-bearing item start (only ever true inside clause (b) in this
# form's layout): a number, a period, then actual text on the SAME line --
# e.g. "1. To enter into any". Clause 3(a)'s own items render as a BARE
# marker with nothing else on that line ("1." / "2." / "3."), with the text
# wrapping onto the next line -- so this pattern never matches inside 3(a),
# only at the true start of clause (b).
# Matches a numbered list item marker whether it's:
#   - bare, alone on its own line ("1.", "2.")            -- one layout style
#   - inline, with text starting on the same line ("1. To acquire...") -- another
# This lets us track the running item number regardless of which layout a
# given filing uses.
NUMERIC_ITEM_PATTERN = re.compile(r"^(\d{1,2})[\.\)](?:\s|$)")
# How many lines of unenumerated content we tolerate after start_idx before
# concluding 3(a) itself never used numeric enumeration (it was lettered
# like (a)(b)(c)... or a plain paragraph) -- meaning the first numeric "1."
# we then see must already be clause (b)'s start, not 3(a)'s own.
# Observed: real numeric 3(a) lists start IMMEDIATELY (gap=0 lines);
# lettered/prose 3(a) sections ran 40+ lines before their first "1.".
# 10 gives generous margin on both sides.
NO_NUMERIC_3A_GAP_THRESHOLD = 10


def extract_objects_from_page_text(pdf_bytes: bytes) -> tuple[str, str | None]:
    """
    Page-text scraping path for non-XFA ("printed") PDFs.

    Supports INC-33 eMOA 3(a) in any of these layouts:
      - numeric items, bare  ("1." alone, text on next line)
      - numeric items, inline ("1. To do X" on one line)
      - lettered items        ("(a) To do X", "(b) To do Y", ...)
      - plain prose, no enumeration at all
    Stop = wherever clause (b)'s own numbering genuinely begins:
      - if 3(a) is itself numeric: enumeration restarting at 1 after 2+.
      - otherwise (lettered / prose): the first numeric "1." encountered,
        since 3(a) never produced one of its own within the tolerance gap.
    We deliberately do NOT rely on the literal "(b) *Matters..." caption
    text as the stop marker -- PyMuPDF emits that caption well after (b)'s
    real content has already started (a form-rendering quirk), so using it
    directly would swallow several of (b)'s own items into the result.
    Also handles the old (A)->(B) MoA format as a fallback.
    """
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

    # -------- New INC-33 format --------
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

    # -------- Old format (also acts as fallback) --------
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


def extract_main_objects_from_pdf_bytes(pdf_bytes: bytes) -> tuple[str, str | None]:
    """
    Top-level extraction dispatcher: tries the XFA datasets path first
    (cheap check -- returns immediately with "NOT_XFA" for ordinary PDFs),
    and falls through to page-text scraping otherwise.
    """
    xfa_text, xfa_warning = extract_objects_clause_from_xfa(pdf_bytes)
    if xfa_warning != "NOT_XFA":
        # This IS an XFA form -- whether it succeeded cleanly, succeeded via
        # fallback scan, or failed, this is the final answer for this PDF.
        # Page-text scraping would just re-find the "Please wait..." page.
        return xfa_text, xfa_warning

    return extract_objects_from_page_text(pdf_bytes)


def process_one_row(row: int, link: str, creds_list: list[Credentials]) -> tuple[int, str]:
    """Download + extract a single row's PDF. Runs inside a worker thread."""
    try:
        file_id = extract_drive_file_id(link)
        drive_services = get_thread_drive_services(creds_list)
        pdf_bytes = fetch_pdf_bytes_multi_account(drive_services, file_id)
        extracted, warning = extract_main_objects_from_pdf_bytes(pdf_bytes)

        if warning:
            cell_value = f"[{warning}]\n{extracted}" if extracted else f"[{warning}]"
        else:
            cell_value = extracted or "NOT FOUND"

    except HttpError as exc:
        status = exc.resp.status if getattr(exc, "resp", None) is not None else None
        if status == 404:
            # Genuinely missing/unshared file -- no point retrying, and no
            # point writing a full ERROR traceback either. Just flag it.
            cell_value = "File Not Found"
        else:
            cell_value = f"ERROR: {exc}"
    except Exception as exc:
        cell_value = f"ERROR: {exc}"

    time.sleep(REQUEST_DELAY_SECONDS)
    return row, cell_value


def highlight_content_not_available_rows(
    sh, ws, output_col_idx: int, status_col_idx: int, col_g_values: list[str]
) -> None:
    """Paint the G:H cells cream on every row where G currently equals
    RETRY_TRIGGER_VALUE ("Content not available"). Purely cosmetic -- does
    not touch cell values. Uses the raw spreadsheet batch_update (repeatCell
    requests) since gspread's worksheet.format() only handles one range per
    call; here we may need to flag many non-contiguous rows at once.

    Adjacent flagged rows are merged into a single contiguous GridRange so a
    long run of consecutive "Content not available" rows doesn't cost one
    request each -- keeps the request count (and therefore batch_update
    calls) manageable on large sheets.
    """
    flagged_rows = [
        row
        for row in range(START_ROW, len(col_g_values) + 1)
        if (col_g_values[row - 1].strip() if row - 1 < len(col_g_values) else "") == RETRY_TRIGGER_VALUE
    ]
    if not flagged_rows:
        print("No 'Content not available' rows found to highlight.")
        return

    # Collapse consecutive row numbers into (start, end) runs.
    runs = []
    run_start = flagged_rows[0]
    prev = flagged_rows[0]
    for row in flagged_rows[1:]:
        if row == prev + 1:
            prev = row
            continue
        runs.append((run_start, prev))
        run_start = row
        prev = row
    runs.append((run_start, prev))

    sheet_id = ws.id
    requests = []
    for start_row, end_row in runs:
        requests.append({
            "repeatCell": {
                "range": {
                    "sheetId": sheet_id,
                    "startRowIndex": start_row - 1,     # 0-indexed, inclusive
                    "endRowIndex": end_row,              # 0-indexed, exclusive -> covers end_row
                    "startColumnIndex": output_col_idx - 1,
                    "endColumnIndex": status_col_idx,    # covers G and H (contiguous columns)
                },
                "cell": {"userEnteredFormat": {"backgroundColor": CREAM_BACKGROUND_COLOR}},
                "fields": "userEnteredFormat.backgroundColor",
            }
        })

    print(f"\n=== Highlighting {len(flagged_rows)} '{RETRY_TRIGGER_VALUE}' row(s) "
          f"({len(runs)} contiguous run(s)) in cream ===")
    for i in range(0, len(requests), HIGHLIGHT_REQUEST_CHUNK_SIZE):
        with_sheets_retry(sh.batch_update, {"requests": requests[i : i + HIGHLIGHT_REQUEST_CHUNK_SIZE]})
        time.sleep(SHEETS_WRITE_PACING_SECONDS)
    print("  -> highlighting applied.")


def write_skip_rows(ws, skip_rows: list[int]) -> None:
    """Write the CIN-dedup 'Skipped' marker directly to G/H for rows that
    are never downloaded/processed at all, in SKIP_WRITE_BATCH_SIZE chunks
    so a huge dedup list can't blow past batch_update payload limits. Each
    chunk is a plain value write (no formatting), so a much bigger chunk
    size than WRITE_BATCH_SIZE is safe here and keeps the request COUNT
    down -- the Sheets API's per-minute quota is on requests, not cells, so
    fewer/bigger batch_update calls matter more than payload size."""
    if not skip_rows:
        return

    print(f"\n=== Writing {len(skip_rows)} duplicate-CIN skip row(s) "
          f"(G='{SKIP_VALUE}', H='{SKIP_STATUS_VALUE}') ===")
    for i in range(0, len(skip_rows), SKIP_WRITE_BATCH_SIZE):
        sub = skip_rows[i : i + SKIP_WRITE_BATCH_SIZE]
        batch_updates = []
        for row in sub:
            batch_updates.append({"range": f"{OUTPUT_COL_LETTER}{row}", "values": [[SKIP_VALUE]]})
            batch_updates.append({"range": f"{STATUS_COL_LETTER}{row}", "values": [[SKIP_STATUS_VALUE]]})
        with_sheets_retry(ws.batch_update, batch_updates, value_input_option="RAW")
        print(f"  -> wrote skip marker for rows {sub[0]}-{sub[-1]} ({len(sub)} row(s))")
        time.sleep(SHEETS_WRITE_PACING_SECONDS)


def process_child_sheet(gc, creds_list: list[Credentials], spreadsheet_id: str) -> None:
    """Run the existing extraction logic (unchanged) end-to-end against a
    single child spreadsheet: open it (picking 'Actual Sheet' or 'Sheet1'),
    highlight 'Content not available' rows, dedup by CIN, and (re)process
    every eligible row. Raises on unrecoverable failure so the master-
    workflow caller can mark that row Failed and move on to the next sheet.
    """
    sh, ws = open_child_worksheet(gc, spreadsheet_id)
    print(f"\n########## Processing child sheet: '{ws.title}' "
          f"(spreadsheet {spreadsheet_id}) ##########")

    cin_col_idx = col_letter_to_index(CIN_COL_LETTER)
    link_col_idx = col_letter_to_index(LINK_COL_LETTER)
    output_col_idx = col_letter_to_index(OUTPUT_COL_LETTER)

    col_a_values = with_sheets_retry(ws.col_values, cin_col_idx)
    col_f_values = with_sheets_retry(ws.col_values, link_col_idx)
    col_g_values = with_sheets_retry(ws.col_values, output_col_idx)

    # Highlight every row currently sitting at "Content not available"
    # (cream, G:H) before anything else touches the sheet, so it reflects
    # the state as of the start of this run.
    status_col_idx = col_letter_to_index(STATUS_COL_LETTER)
    highlight_content_not_available_rows(
        sh=sh,
        ws=ws,
        output_col_idx=output_col_idx,
        status_col_idx=status_col_idx,
        col_g_values=col_g_values,
    )

    # Build the list of rows that need (re)processing:
    #   - must have a link in F
    #   - col G must be EXACTLY the retry-trigger value ("Content not
    #     available"). Rows where G is empty are left completely alone
    #     (never processed by this run), and rows where G already holds a
    #     real extracted result (or some other value) are also skipped.
    #
    # Among candidate rows, dedup by CIN (col A): only the first row for a
    # given (non-blank) CIN is actually processed; every later row sharing
    # that CIN is diverted into skip_rows and written as "Skipped" /
    # "Skipped - Same CIN" instead, without ever downloading its PDF.
    rows_to_process = []
    skip_rows = []
    seen_cins = set()
    for row in range(START_ROW, len(col_f_values) + 1):
        link = col_f_values[row - 1].strip() if row - 1 < len(col_f_values) else ""
        current_g = col_g_values[row - 1].strip() if row - 1 < len(col_g_values) else ""
        if not (link and current_g == RETRY_TRIGGER_VALUE):
            continue

        cin = col_a_values[row - 1].strip() if row - 1 < len(col_a_values) else ""
        if cin and cin in seen_cins:
            skip_rows.append(row)
            continue

        if cin:
            seen_cins.add(cin)
        rows_to_process.append((row, link))

    print(f"{len(rows_to_process)} row(s) marked '{RETRY_TRIGGER_VALUE}' to actually reprocess, "
          f"{len(skip_rows)} row(s) marked '{RETRY_TRIGGER_VALUE}' skipped as duplicate CIN, "
          f"out of {len(col_f_values)} total.")
    print(f"Using up to {MAX_WORKERS} concurrent downloads per batch of {BATCH_SIZE}.")

    # Write the duplicate-CIN skip markers up front. These never touch the
    # Drive API at all -- cheap, so no need to interleave with the
    # processing windows below.
    write_skip_rows(ws, skip_rows)

    # Process in outer windows of BATCH_SIZE (100). Within each window, work
    # is further split into WRITE_BATCH_SIZE (25) sub-chunks: each sub-chunk
    # is downloaded/extracted concurrently (up to MAX_WORKERS threads), then
    # immediately written with its own batch_update call before moving to
    # the next sub-chunk. This keeps each write small and easy to verify
    # (25 rows at a time) while still batching instead of one API call per
    # row. Each written row updates BOTH column G (the extracted text) and
    # column H (set to "Clause 3a" to mark it as reprocessed).
    for chunk_start in range(0, len(rows_to_process), BATCH_SIZE):
        chunk = rows_to_process[chunk_start : chunk_start + BATCH_SIZE]
        print(f"\n=== Window: rows {chunk[0][0]}-{chunk[-1][0]} ({len(chunk)} row(s)) ===")

        for sub_start in range(0, len(chunk), WRITE_BATCH_SIZE):
            sub_chunk = chunk[sub_start : sub_start + WRITE_BATCH_SIZE]
            print(f"\n--- Sub-batch: rows {sub_chunk[0][0]}-{sub_chunk[-1][0]} "
                  f"({len(sub_chunk)} row(s)) ---")

            results = {}  # row -> cell_value
            with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
                futures = {
                    executor.submit(process_one_row, row, link, creds_list): row
                    for row, link in sub_chunk
                }
                for future in as_completed(futures):
                    row = futures[future]
                    try:
                        _, cell_value = future.result()
                    except Exception as exc:  # safety net; process_one_row already catches its own errors
                        cell_value = f"ERROR: {exc}"
                    results[row] = cell_value

            # Preserve row order when building AND printing the batch_update
            # payload, so it's easy to eyeball that row N really got row N's
            # content (concurrent completion order above doesn't matter here).
            batch_updates = []
            for row, link in sub_chunk:
                cell_value = results[row]
                preview = cell_value[:60].replace("\n", " ")
                print(f"  Row {row}: {preview}...")
                batch_updates.append({"range": f"{OUTPUT_COL_LETTER}{row}", "values": [[cell_value]]})
                batch_updates.append({"range": f"{STATUS_COL_LETTER}{row}", "values": [[STATUS_VALUE]]})

            with_sheets_retry(ws.batch_update, batch_updates, value_input_option="RAW")
            print(f"  -> wrote {len(sub_chunk)} row(s) "
                  f"({OUTPUT_COL_LETTER}{sub_chunk[0][0]}:{OUTPUT_COL_LETTER}{sub_chunk[-1][0]}, "
                  f"{STATUS_COL_LETTER} set to '{STATUS_VALUE}')")
            time.sleep(SHEETS_WRITE_PACING_SECONDS)

    print(f"\n########## Done with child sheet: '{ws.title}' "
          f"(spreadsheet {spreadsheet_id}) ##########")


# ===================== MASTER-WORKFLOW ORCHESTRATION =====================

def claim_next_master_row(master_ws) -> dict | None:
    """Re-read the master sheet fresh and claim exactly ONE eligible row:
      - XFA User == CURRENT_USER and XFA Status == 'Processing' -> resume
        it (already claimed by an earlier, interrupted run by this user).
        Takes priority so an interrupted child sheet is finished before any
        new one is started.
      - Otherwise, the first row with XFA User blank -> claim it now by
        writing CURRENT_USER / 'Processing'.
    Returns {"row": int, "spreadsheet_id": str} or None if nothing is left
    to do. Re-reading on every call (rather than once up front) means rows
    claimed/completed by other concurrent runs since the last check are
    respected, and a malformed Sheet Link is skipped with a warning rather
    than aborting the whole run.
    """
    link_col_idx = col_letter_to_index(MASTER_SHEET_LINK_COL_LETTER)
    user_col_idx = col_letter_to_index(MASTER_XFA_USER_COL_LETTER)
    status_col_idx = col_letter_to_index(MASTER_XFA_STATUS_COL_LETTER)

    link_values = with_sheets_retry(master_ws.col_values, link_col_idx)
    user_values = with_sheets_retry(master_ws.col_values, user_col_idx)
    status_values = with_sheets_retry(master_ws.col_values, status_col_idx)

    def cell(values: list[str], row: int) -> str:
        return values[row - 1].strip() if row - 1 < len(values) else ""

    num_rows = max(len(link_values), len(user_values), len(status_values))

    def build_entry(row: int) -> dict | None:
        link = cell(link_values, row)
        try:
            spreadsheet_id = extract_spreadsheet_id_from_link(link)
        except ValueError as exc:
            print(f"  [master row {row}] SKIPPED -- {exc}")
            return None
        return {"row": row, "spreadsheet_id": spreadsheet_id}

    # Priority 1: resume a row this user already has "in flight".
    for row in range(MASTER_START_ROW, num_rows + 1):
        if cell(user_values, row) == CURRENT_USER and cell(status_values, row) == MASTER_STATUS_PROCESSING:
            entry = build_entry(row)
            if entry:
                print(f"[master row {row}] resuming (already claimed, status='{MASTER_STATUS_PROCESSING}').")
                return entry

    # Priority 2: claim the first row with a blank XFA User.
    for row in range(MASTER_START_ROW, num_rows + 1):
        link = cell(link_values, row)
        if not link:
            continue
        if cell(status_values, row) == MASTER_STATUS_COMPLETED:
            continue
        if cell(user_values, row):
            continue  # already claimed by someone (this user's in-flight rows were handled above)

        entry = build_entry(row)
        if entry is None:
            continue

        with_sheets_retry(master_ws.update_acell, f"{MASTER_XFA_USER_COL_LETTER}{row}", CURRENT_USER)
        with_sheets_retry(master_ws.update_acell, f"{MASTER_XFA_STATUS_COL_LETTER}{row}", MASTER_STATUS_PROCESSING)
        print(f"[master row {row}] claimed for user '{CURRENT_USER}'.")
        return entry

    return None


def update_master_status(master_ws, row: int, status: str) -> None:
    """Write XFA Status for a single master row (Done / Failed)."""
    with_sheets_retry(master_ws.update_acell, f"{MASTER_XFA_STATUS_COL_LETTER}{row}", status)


def process_master_workflow() -> None:
    """Master-sheet-driven orchestration layer. Repeatedly: re-reads the
    master spreadsheet, claims exactly ONE eligible row (blank XFA User, or
    a row this user already has in-flight), fully processes that single
    child spreadsheet (which internally still runs in BATCH_SIZE /
    WRITE_BATCH_SIZE windows via process_child_sheet -- unchanged), writes
    XFA Status = 'Done' on success or 'Failed' on failure, then moves on to
    the next eligible row. Stops when no eligible row remains. A failure on
    one child sheet never aborts the run -- it's marked Failed and the loop
    continues.
    """
    creds_list = build_service_account_creds_list()
    gc = gspread.authorize(creds_list[0])  # primary account handles Sheets I/O

    master_sh = with_sheets_retry(gc.open_by_key, MASTER_SPREADSHEET_ID)
    master_ws = with_sheets_retry(master_sh.worksheet, MASTER_SHEET_NAME)

    while True:
        entry = claim_next_master_row(master_ws)
        if entry is None:
            print("\nNo more eligible master rows -- stopping.")
            break

        row = entry["row"]
        spreadsheet_id = entry["spreadsheet_id"]
        try:
            process_child_sheet(gc, creds_list, spreadsheet_id)
        except Exception as exc:
            print(f"\n!!! [master row {row}] FAILED processing spreadsheet "
                  f"{spreadsheet_id}: {exc} -- marking Failed and continuing. !!!")
            try:
                update_master_status(master_ws, row, MASTER_STATUS_FAILED)
            except Exception as update_exc:
                print(f"    (also failed to write Failed status back to master: {update_exc})")
            continue

        try:
            update_master_status(master_ws, row, MASTER_STATUS_COMPLETED)
            print(f"[master row {row}] marked '{MASTER_STATUS_COMPLETED}'.")
        except Exception as update_exc:
            print(f"    (child sheet processed OK, but failed to write "
                  f"'{MASTER_STATUS_COMPLETED}' status back to master row {row}: {update_exc})")

    print("\nMaster workflow done.")


def main():
    process_master_workflow()


if __name__ == "__main__":
    main()