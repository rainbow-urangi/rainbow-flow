from __future__ import annotations
from urllib.parse import urlparse
import re

ACTIVITY_RULE_VERSION = "v0.2"

def _safe_trim(s: str | None, max_len: int) -> str | None:
    if not s:
        return None
    s = str(s).strip()
    if not s:
        return None
    return s[:max_len]

def _url_path(page_url: str) -> str:
    try:
        p = urlparse(page_url)
        return (p.path or "/")[:256]
    except Exception:
        return "/"

def _bucket_status(code: int | None) -> str | None:
    if code is None:
        return None
    if 200 <= code < 300: return "2xx"
    if 300 <= code < 400: return "3xx"
    if 400 <= code < 500: return "4xx"
    if 500 <= code < 600: return "5xx"
    return "other"

_digitish = re.compile(r"\b\d{4,}\b")

def _mask_ids(text: str) -> str:
    return _digitish.sub("<num>", text)

def build_activity(row: dict) -> tuple[str, str | None, str | None, str]:
    event_type = (row.get("event_type") or "").upper()
    itype = (row.get("interaction_type") or "").lower()

    page_url = row.get("page_url") or ""
    path = _url_path(page_url)

    api_path = _safe_trim(row.get("api_path"), 512)
    api_method = _safe_trim(row.get("api_method"), 16)

    status_code = row.get("api_status_code")
    try:
        status_code = int(status_code) if status_code is not None else None
    except Exception:
        status_code = None

    label = _safe_trim(row.get("associated_label"), 128)
    etext = _safe_trim(row.get("element_text"), 128)
    etag = _safe_trim(row.get("element_tag"), 32)
    ptitle = _safe_trim(row.get("page_title"), 128)

    if event_type == "PAGE_VIEW":
        act = f"NAV:{path}"
        return act, "NAV", path.split("/")[1] if path != "/" else None, ACTIVITY_RULE_VERSION

    if api_path and api_method:
        bucket = _bucket_status(status_code)
        act = f"API:{api_method}_{api_path}"
        if bucket:
            act += f"_{bucket}"
        return _mask_ids(act), "API", api_path.split("/")[1] if api_path.startswith("/") else None, ACTIVITY_RULE_VERSION

    if itype == "submit":
        act = f"SUBMIT:{ptitle or path}"
        return _mask_ids(act), "SUBMIT", None, ACTIVITY_RULE_VERSION

    if itype == "click":
        target = label or etext or etag or "unknown"
        act = f"CLICK:{target}"
        return _mask_ids(act), "CLICK", None, ACTIVITY_RULE_VERSION

    if itype in ("change", "input", "keyup", "keydown", "textinput"):
        target = label or etag or "unknown"
        act = f"INPUT:{target}"
        return _mask_ids(act), "INPUT", None, ACTIVITY_RULE_VERSION

    act = f"EVT:{itype or 'unknown'}"
    return _mask_ids(act), "EVT", None, ACTIVITY_RULE_VERSION