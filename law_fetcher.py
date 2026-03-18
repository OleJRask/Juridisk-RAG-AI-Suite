
#!/usr/bin/env python3
from __future__ import annotations
import argparse
import csv
import hashlib
import importlib
import io
import json
import re
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from html import unescape
from io import BytesIO
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple
from urllib.error import HTTPError, URLError
from urllib.parse import urlparse
from urllib.request import Request, urlopen
import asyncio
from typing import Optional

try:
    from playwright.sync_api import sync_playwright
except ImportError:
    sync_playwright = None

@dataclass
class FetchResult:
    status_code: int
    content_type: str
    body: bytes
    elapsed_ms: int

RETRYABLE_DEFAULT_HTTP_CODES = {408, 429, 500, 502, 503, 504}

def slugify(value: str, fallback: str = "unknown") -> str:
    value = (value or "").strip().lower()
    value = re.sub(r"[^\w\s-]", "", value, flags=re.UNICODE)
    value = re.sub(r"[\s_-]+", "-", value)
    value = value.strip("-")
    return value or fallback

def safe_filename(value: str, fallback: str = "untitled", max_len: int = 120) -> str:
    # Replace forbidden characters and also remove parentheses and other punctuation that can cause issues
    value = re.sub(r'[<>:"/\\|?*()\[\]{};,.!\'`~@#$%^&=+\x00-\x1f]', "_", (value or "").strip())
    value = re.sub(r"\s+", " ", value).strip(" ._-")
    # Remove consecutive underscores
    value = re.sub(r'_+', '_', value)
    return (value or fallback)[:max_len]

def normalize_header(name: str) -> str:
    return re.sub(r"\W+", "", (name or "").strip().lower())

def read_laws(csv_path: Path) -> List[Dict[str, str]]:
    last_error: Optional[Exception] = None
    for encoding in ("utf-16", "utf-16-le", "utf-8-sig", "cp1252", "latin-1"):
        try:
            text = csv_path.read_text(encoding=encoding)
            reader = csv.DictReader(io.StringIO(text), delimiter=";", quotechar='"')
            if not reader.fieldnames:
                raise ValueError("CSV appears empty or has no header")

            header_map = {normalize_header(name): name for name in reader.fieldnames}
            url_col = header_map.get("eliurl")
            if not url_col:
                raise ValueError("Missing required column: EliUrl")

            rows: List[Dict[str, str]] = []
            debug_count = 0
            for row in reader:
                cleaned: Dict[str, str] = {}
                for key, value in row.items():
                    if key is None:
                        continue
                    if isinstance(value, list):
                        cleaned[key] = " ".join(str(part).strip() for part in value if part is not None).strip()
                    else:
                        cleaned[key] = (value or "").strip()
                ressort = cleaned.get("Ressort", "")
                year_str = cleaned.get("År", "")
                if debug_count < 5:
                    print(f"[DEBUG] Ressort: '{ressort}', År: '{year_str}'")
                    debug_count += 1
                try:
                    year = int(year_str)
                except Exception:
                    year = 0
                # Match ressort regardless of case and whitespace
                if cleaned.get(url_col) and ressort.strip().lower() == "social- og boligministeriet" and year >= 2000:
                    cleaned["EliUrl"] = cleaned[url_col]
                    rows.append(cleaned)
            print(f"[INFO] Antal love efter filtrering: {len(rows)}")
            return rows
        except (UnicodeDecodeError, ValueError) as exc:
            last_error = exc

    raise ValueError(f"Failed to parse CSV at {csv_path}: {last_error}")

def parse_retry_codes(raw: str) -> set[int]:
    values = set()
    for token in (raw or "").split(","):
        token = token.strip()
        if not token:
            continue
        try:
            values.add(int(token))
        except ValueError:
            continue
    return values or set(RETRYABLE_DEFAULT_HTTP_CODES)

def _unique_preserve_order(values: List[str]) -> List[str]:
    seen = set()
    ordered: List[str] = []
    for value in values:
        key = value.strip()
        if not key or key in seen:
            continue
        seen.add(key)
        ordered.append(key)
    return ordered

def build_candidate_base_urls(row: Dict[str, str], url_fallback_mode: bool, fallback_max_variants: int) -> List[str]:
    primary = (row.get("EliUrl") or "").strip()
    if not primary:
        return []

    if not url_fallback_mode:
        return [primary]

    candidates: List[str] = [primary]
    parsed = urlparse(primary)
    netloc = parsed.netloc

    if netloc.startswith("www."):
        candidates.append(primary.replace("www.", "", 1))
    elif netloc:
        candidates.append(primary.replace(netloc, f"www.{netloc}", 1))

    accn = (row.get("ACCN") or "").strip()
    if accn:
        candidates.append(f"https://retsinformation.dk/eli/accn/{accn}")
        candidates.append(f"https://www.retsinformation.dk/eli/accn/{accn}")
        candidates.append(f"https://retsinformation.dk/eli/ft/{accn}")
        candidates.append(f"https://www.retsinformation.dk/eli/ft/{accn}")

    year = (row.get("År") or "").strip()
    number = (row.get("Nummer") or "").strip()
    if year and number:
        doc_type_map = {
            "lovtidende a": "lta",
            "lovtidende b": "ltb",
            "lovtidende c": "ltc",
            "ministerialtidende": "mt",
        }
        pub_key = (row.get("Publiceringsmedie") or "").strip().lower()
        short_pub = doc_type_map.get(pub_key, "lta")
        candidates.append(f"https://retsinformation.dk/eli/{short_pub}/{year}/{number}")
        candidates.append(f"https://www.retsinformation.dk/eli/{short_pub}/{year}/{number}")

    ordered = _unique_preserve_order(candidates)
    limit = max(1, fallback_max_variants)
    return ordered[:limit]

def _retry_sleep(
    attempt: int,
    base_seconds: float,
    multiplier: float,
) -> None:
    delay = max(0.0, base_seconds) * (max(1.0, multiplier) ** max(0, attempt - 1))
    if delay > 0:
        time.sleep(delay)

def fetch_url(
    url: str,
    timeout: int,
    user_agent: str,
    *,
    retry_mode: bool,
    max_retries: int,
    retry_backoff_seconds: float,
    retry_backoff_multiplier: float,
    retry_http_codes: set[int],
) -> FetchResult:
    request = Request(url, headers={"User-Agent": user_agent})
    retries_allowed = max(0, max_retries) if retry_mode else 0

    for attempt in range(retries_allowed + 1):
        started = time.perf_counter()
        try:
            with urlopen(request, timeout=timeout) as response:
                body = response.read()
                elapsed_ms = int((time.perf_counter() - started) * 1000)
                return FetchResult(
                    status_code=getattr(response, "status", 200),
                    content_type=response.headers.get("Content-Type", ""),
                    body=body,
                    elapsed_ms=elapsed_ms,
                )
        except HTTPError as exc:
            should_retry = attempt < retries_allowed and exc.code in retry_http_codes
            if not should_retry:
                raise
            print(
                f"  retry {attempt + 1}/{retries_allowed} for HTTP {exc.code}: {url}",
                file=sys.stderr,
            )
            _retry_sleep(attempt + 1, retry_backoff_seconds, retry_backoff_multiplier)
        except (URLError, TimeoutError, ConnectionResetError, OSError) as exc:
            should_retry = attempt < retries_allowed
            if not should_retry:
                raise
            print(
                f"  retry {attempt + 1}/{retries_allowed} after {type(exc).__name__}: {url}",
                file=sys.stderr,
            )
            _retry_sleep(attempt + 1, retry_backoff_seconds, retry_backoff_multiplier)

    raise RuntimeError(f"Unreachable retry loop for URL: {url}")

def extract_page_title(html_text: str) -> str:
    match = re.search(r"<title[^>]*>(.*?)</title>", html_text, flags=re.IGNORECASE | re.DOTALL)
    if not match:
        return ""
    return re.sub(r"\s+", " ", unescape(match.group(1))).strip()

def extract_text_from_html(html: str) -> str:
    text = re.sub(r"(?is)<(script|style|noscript).*?>.*?</\1>", " ", html)
    text = re.sub(r"(?i)<br\s*/?>", "\n", text)
    text = re.sub(r"(?i)</(p|div|li|h1|h2|h3|h4|h5|h6|tr|section|article)>", "\n", text)
    text = re.sub(r"<[^>]+>", " ", text)
    text = unescape(text).replace("\r", "")
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n[ \t]+", "\n", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()

def extract_text_from_pdf(pdf_bytes: bytes) -> str:
    pypdf = importlib.import_module("pypdf")
    reader = pypdf.PdfReader(BytesIO(pdf_bytes))
    page_texts: List[str] = []
    for page in reader.pages:
        text = (page.extract_text() or "").strip()
        if text:
            page_texts.append(text)
    return "\n\n".join(page_texts).strip()

def chunk_text(text: str, max_chars: int, overlap_chars: int, min_chunk_chars: int) -> List[str]:
    if not text:
        return []

    chunks: List[str] = []
    cursor = 0
    n = len(text)

    while cursor < n:
        upper = min(cursor + max_chars, n)
        if upper < n:
            split_at = text.rfind(" ", cursor + int(max_chars * 0.6), upper)
            if split_at > cursor:
                upper = split_at

        segment = text[cursor:upper].strip()
        if segment and (len(segment) >= min_chunk_chars or upper == n or not chunks):
            chunks.append(segment)

        if upper >= n:
            break

        cursor = max(upper - overlap_chars, cursor + 1)

    return chunks

def estimate_tokens(text: str) -> int:
    return max(1, len(text) // 4)

def build_target_dir(output_dir: Path, row: Dict[str, str], eli_url: str) -> Path:
    ressort = slugify(row.get("Ressort") or "ukendt-ressort", fallback="ukendt-ressort")
    year = slugify(row.get("År") or "unknown-year", fallback="unknown-year")
    document_type = slugify(row.get("DokumentType") or "unknown-type", fallback="unknown-type")
    number_raw = row.get("Nummer", "no-number")
    if not number_raw:
        number_raw = "no-number"
    number = safe_filename(number_raw, fallback="no-number", max_len=30)
    document_id_raw = row.get("DokumentId", "no-docid")
    if not document_id_raw:
        document_id_raw = "no-docid"
    document_id = safe_filename(document_id_raw, fallback="no-docid", max_len=40)
    tail = f"{number}_{document_id}"
    if tail in {"no-number_no-docid", "_", "", None}:
        tail = f"url_{hashlib.sha1(eli_url.encode('utf-8')).hexdigest()[:12]}"
    return output_dir / "laws" / ressort / year / document_type / safe_filename(tail, fallback="item")

def build_document_id(row: Dict[str, str], eli_url: str) -> str:
    year = (row.get("År") or "unknown-year").strip()
    number = (row.get("Nummer") or "no-number").strip()
    document_id = (row.get("DokumentId") or "no-docid").strip()
    url_hash = hashlib.sha1(eli_url.encode("utf-8")).hexdigest()[:10]
    return safe_filename(f"{year}-{number}-{document_id}-{url_hash}", fallback=f"doc-{url_hash}", max_len=100)

def write_json(path: Path, payload: Dict[str, Any]) -> None:
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

def write_jsonl(path: Path, rows: List[Dict[str, Any]]) -> None:
    with path.open("w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row, ensure_ascii=False) + "\n")

def collect_facets(documents: List[Dict[str, Any]]) -> Dict[str, List[str]]:
    def values(key: str) -> List[str]:
        unique = {doc.get("metadata", {}).get(key, "") for doc in documents}
        return sorted(v for v in unique if v)

    return {
        "language": ["da"],
        "year": values("year"),
        "document_type": values("document_type"),
        "ressort": values("ressort"),
        "authority": values("authority"),
        "historical": values("historical"),
        "geo": values("geo"),
    }

def build_metadata_index(documents: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    index_rows: List[Dict[str, Any]] = []
    for doc in documents:
        metadata = doc.get("metadata", {})
        index_rows.append(
            {
                "document_id": doc.get("document_id", ""),
                "title": doc.get("title", ""),
                "url": doc.get("url", ""),
                "full_text_path": doc.get("full_text_path", ""),
                "pdf_path": doc.get("pdf_path", ""),
                "html_path": doc.get("html_path", ""),
                "chunk_count": doc.get("chunk_count", 0),
                "text_length": doc.get("text_length", 0),
                "token_estimate": doc.get("token_estimate", 0),
                "metadata": metadata,
            }
        )
    return index_rows

def build_retrieval_text(metadata: Dict[str, Any], chunk: str) -> str:
    header = [
        f"Titel: {metadata.get('title', '')}",
        f"Populær titel: {metadata.get('popular_title', '')}",
        f"Dokumenttype: {metadata.get('document_type', '')}",
        f"Ressort: {metadata.get('ressort', '')}",
        f"År: {metadata.get('year', '')}",
        f"Nummer: {metadata.get('number', '')}",
    ]
    return "\n".join([line for line in header if line.strip().endswith(":") is False] + ["", chunk]).strip()

def clean_playwright_text(text: str) -> str:
    """
    Fjerner typiske header/footer-artefakter fra Playwright-hentede love.
    """
    import re
    # Fjern alt før første forekomst af 'VI MARGRETHE DEN ANDEN' eller 'Bekendtgørelse af' eller 'Lov om'
    start_patterns = [
        r'VI MARGRETHE DEN ANDEN',
        r'Bekendtgørelse af',
        r'Lov om',
        r'LOV nr',
        r'Konsolideret ved',
    ]
    start_idx = 0
    for pat in start_patterns:
        m = re.search(pat, text, re.IGNORECASE)
        if m:
            start_idx = m.start()
            break
    text = text[start_idx:]

    # Fjern alt efter typiske footers
    end_patterns = [
        r'Gå til top',
        r'Om Retsinformation',
        r'Officielle noter',
        r'Besøg også',
        r'KontaktFAQ',
        r'Vejledning',
        r'Vend tilbage til',
        r'\bOm\b',
    ]
    end_idx = len(text)
    for pat in end_patterns:
        m = re.search(pat, text, re.IGNORECASE)
        if m:
            end_idx = m.start()
            break
    text = text[:end_idx]

    # Fjern overflødige blanklinjer og whitespace
    text = re.sub(r'\n{3,}', '\n\n', text)
    text = re.sub(r'[ \t]+', ' ', text)
    text = text.strip()
    return text



@dataclass
class FetchResult:
    status_code: int
    content_type: str
    body: bytes
    elapsed_ms: int


RETRYABLE_DEFAULT_HTTP_CODES = {408, 429, 500, 502, 503, 504}


def slugify(value: str, fallback: str = "unknown") -> str:
    value = (value or "").strip().lower()
    value = re.sub(r"[^\w\s-]", "", value, flags=re.UNICODE)
    value = re.sub(r"[\s_-]+", "-", value)
    value = value.strip("-")
    return value or fallback


def safe_filename(value: str, fallback: str = "untitled", max_len: int = 120) -> str:
    value = re.sub(r'[<>:"/|?*\x00-\x1f]', "_", (value or "").strip())
    value = re.sub(r"\s+", " ", value).strip(" .")
    return (value or fallback)[:max_len]


def normalize_header(name: str) -> str:
    return re.sub(r"\W+", "", (name or "").strip().lower())


def read_laws(csv_path: Path) -> List[Dict[str, str]]:
    last_error: Optional[Exception] = None
    for encoding in ("utf-16", "utf-16-le", "utf-8-sig", "cp1252", "latin-1"):
        try:
            text = csv_path.read_text(encoding=encoding)
            reader = csv.DictReader(io.StringIO(text), delimiter=";", quotechar='"')
            if not reader.fieldnames:
                raise ValueError("CSV appears empty or has no header")

            header_map = {normalize_header(name): name for name in reader.fieldnames}
            url_col = header_map.get("eliurl")
            if not url_col:
                raise ValueError("Missing required column: EliUrl")

            rows: List[Dict[str, str]] = []
            for row in reader:
                cleaned: Dict[str, str] = {}
                for key, value in row.items():
                    if key is None:
                        continue
                    if isinstance(value, list):
                        cleaned[key] = " ".join(str(part).strip() for part in value if part is not None).strip()
                    else:
                        cleaned[key] = (value or "").strip()
                if cleaned.get(url_col):
                    cleaned["EliUrl"] = cleaned[url_col]
                    rows.append(cleaned)
            return rows
        except (UnicodeDecodeError, ValueError) as exc:
            last_error = exc

    raise ValueError(f"Failed to parse CSV at {csv_path}: {last_error}")


def parse_retry_codes(raw: str) -> set[int]:
    values = set()
    for token in (raw or "").split(","):
        token = token.strip()
        if not token:
            continue
        try:
            values.add(int(token))
        except ValueError:
            continue
    return values or set(RETRYABLE_DEFAULT_HTTP_CODES)


def _unique_preserve_order(values: List[str]) -> List[str]:
    seen = set()
    ordered: List[str] = []
    for value in values:
        key = value.strip()
        if not key or key in seen:
            continue
        seen.add(key)
        ordered.append(key)
    return ordered


def build_candidate_base_urls(row: Dict[str, str], url_fallback_mode: bool, fallback_max_variants: int) -> List[str]:
    primary = (row.get("EliUrl") or "").strip()
    if not primary:
        return []

    if not url_fallback_mode:
        return [primary]

    candidates: List[str] = [primary]
    parsed = urlparse(primary)
    netloc = parsed.netloc

    if netloc.startswith("www."):
        candidates.append(primary.replace("www.", "", 1))
    elif netloc:
        candidates.append(primary.replace(netloc, f"www.{netloc}", 1))

    accn = (row.get("ACCN") or "").strip()
    if accn:
        candidates.append(f"https://retsinformation.dk/eli/accn/{accn}")
        candidates.append(f"https://www.retsinformation.dk/eli/accn/{accn}")
        candidates.append(f"https://retsinformation.dk/eli/ft/{accn}")
        candidates.append(f"https://www.retsinformation.dk/eli/ft/{accn}")

    year = (row.get("År") or "").strip()
    number = (row.get("Nummer") or "").strip()
    if year and number:
        doc_type_map = {
            "lovtidende a": "lta",
            "lovtidende b": "ltb",
            "lovtidende c": "ltc",
            "ministerialtidende": "mt",
        }
        pub_key = (row.get("Publiceringsmedie") or "").strip().lower()
        short_pub = doc_type_map.get(pub_key, "lta")
        candidates.append(f"https://retsinformation.dk/eli/{short_pub}/{year}/{number}")
        candidates.append(f"https://www.retsinformation.dk/eli/{short_pub}/{year}/{number}")

    ordered = _unique_preserve_order(candidates)
    limit = max(1, fallback_max_variants)
    return ordered[:limit]


def _retry_sleep(
    attempt: int,
    base_seconds: float,
    multiplier: float,
) -> None:
    delay = max(0.0, base_seconds) * (max(1.0, multiplier) ** max(0, attempt - 1))
    if delay > 0:
        time.sleep(delay)


def fetch_url(
    url: str,
    timeout: int,
    user_agent: str,
    *,
    retry_mode: bool,
    max_retries: int,
    retry_backoff_seconds: float,
    retry_backoff_multiplier: float,
    retry_http_codes: set[int],
) -> FetchResult:
    request = Request(url, headers={"User-Agent": user_agent})
    retries_allowed = max(0, max_retries) if retry_mode else 0

    for attempt in range(retries_allowed + 1):
        started = time.perf_counter()
        try:
            with urlopen(request, timeout=timeout) as response:
                body = response.read()
                elapsed_ms = int((time.perf_counter() - started) * 1000)
                return FetchResult(
                    status_code=getattr(response, "status", 200),
                    content_type=response.headers.get("Content-Type", ""),
                    body=body,
                    elapsed_ms=elapsed_ms,
                )
        except HTTPError as exc:
            should_retry = attempt < retries_allowed and exc.code in retry_http_codes
            if not should_retry:
                raise
            print(
                f"  retry {attempt + 1}/{retries_allowed} for HTTP {exc.code}: {url}",
                file=sys.stderr,
            )
            _retry_sleep(attempt + 1, retry_backoff_seconds, retry_backoff_multiplier)
        except (URLError, TimeoutError, ConnectionResetError, OSError) as exc:
            should_retry = attempt < retries_allowed
            if not should_retry:
                raise
            print(
                f"  retry {attempt + 1}/{retries_allowed} after {type(exc).__name__}: {url}",
                file=sys.stderr,
            )
            _retry_sleep(attempt + 1, retry_backoff_seconds, retry_backoff_multiplier)

    raise RuntimeError(f"Unreachable retry loop for URL: {url}")


def extract_page_title(html_text: str) -> str:
    match = re.search(r"<title[^>]*>(.*?)</title>", html_text, flags=re.IGNORECASE | re.DOTALL)
    if not match:
        return ""
    return re.sub(r"\s+", " ", unescape(match.group(1))).strip()


def extract_text_from_html(html: str) -> str:
        # Debug: Gem HTML hvis tekstudtræk fejler
        # (bruges senere i main-flowet)
    # Forsøg at udtrække tekst fra <div class='ri-tekst'> hvis den findes
    match = re.search(r"<div[^>]+class=[\"']ri-tekst[\"'][^>]*>(.*?)</div>", html, flags=re.DOTALL|re.IGNORECASE)
    if match:
        raw = match.group(1)
        text = re.sub(r"(?is)<(script|style|noscript).*?>.*?</\1>", " ", raw)
        text = re.sub(r"(?i)<br\s*/?>", "\n", text)
        text = re.sub(r"(?i)</(p|div|li|h1|h2|h3|h4|h5|h6|tr|section|article)>", "\n", text)
        text = re.sub(r"<[^>]+>", " ", text)
        text = unescape(text).replace("\r", "")
        text = re.sub(r"[ \t]+", " ", text)
        text = re.sub(r"\n[ \t]+", "\n", text)
        text = re.sub(r"\n{3,}", "\n\n", text)
        return text.strip()
    # Fald tilbage til eksisterende logik
    text = re.sub(r"(?is)<(script|style|noscript).*?>.*?</\1>", " ", html)
    text = re.sub(r"(?i)<br\s*/?>", "\n", text)
    text = re.sub(r"(?i)</(p|div|li|h1|h2|h3|h4|h5|h6|tr|section|article)>", "\n", text)
    text = re.sub(r"<[^>]+>", " ", text)
    text = unescape(text).replace("\r", "")
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n[ \t]+", "\n", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def extract_text_from_pdf(pdf_bytes: bytes) -> str:
    pypdf = importlib.import_module("pypdf")
    reader = pypdf.PdfReader(BytesIO(pdf_bytes))
    page_texts: List[str] = []
    for page in reader.pages:
        text = (page.extract_text() or "").strip()
        if text:
            page_texts.append(text)
    return "\n\n".join(page_texts).strip()


def chunk_text(text: str, max_chars: int, overlap_chars: int, min_chunk_chars: int) -> List[str]:
    if not text:
        return []

    chunks: List[str] = []
    cursor = 0
    n = len(text)

    while cursor < n:
        upper = min(cursor + max_chars, n)
        if upper < n:
            split_at = text.rfind(" ", cursor + int(max_chars * 0.6), upper)
            if split_at > cursor:
                upper = split_at

        segment = text[cursor:upper].strip()
        if segment and (len(segment) >= min_chunk_chars or upper == n or not chunks):
            chunks.append(segment)

        if upper >= n:
            break

        cursor = max(upper - overlap_chars, cursor + 1)

    return chunks


def estimate_tokens(text: str) -> int:
    return max(1, len(text) // 4)


def build_target_dir(output_dir: Path, row: Dict[str, str], eli_url: str) -> Path:
    ressort = slugify(row.get("Ressort") or "ukendt-ressort", fallback="ukendt-ressort")
    year = slugify(row.get("År") or "unknown-year", fallback="unknown-year")
    document_type = slugify(row.get("DokumentType") or "unknown-type", fallback="unknown-type")
    number_raw = row.get("Nummer", "no-number")
    if not number_raw:
        number_raw = "no-number"
    number = safe_filename(number_raw, fallback="no-number", max_len=30)
    document_id_raw = row.get("DokumentId", "no-docid")
    if not document_id_raw:
        document_id_raw = "no-docid"
    document_id = safe_filename(document_id_raw, fallback="no-docid", max_len=40)
    tail = f"{number}_{document_id}"
    if tail in {"no-number_no-docid", "_", "", None}:
        tail = f"url_{hashlib.sha1(eli_url.encode('utf-8')).hexdigest()[:12]}"
    return output_dir / "laws" / ressort / year / document_type / safe_filename(tail, fallback="item")


def build_document_id(row: Dict[str, str], eli_url: str) -> str:
    year = (row.get("År") or "unknown-year").strip()
    number = (row.get("Nummer") or "no-number").strip()
    document_id = (row.get("DokumentId") or "no-docid").strip()
    url_hash = hashlib.sha1(eli_url.encode("utf-8")).hexdigest()[:10]
    return safe_filename(f"{year}-{number}-{document_id}-{url_hash}", fallback=f"doc-{url_hash}", max_len=100)


def write_json(path: Path, payload: Dict[str, Any]) -> None:
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def write_jsonl(path: Path, rows: List[Dict[str, Any]]) -> None:
    with path.open("w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row, ensure_ascii=False) + "\n")


def collect_facets(documents: List[Dict[str, Any]]) -> Dict[str, List[str]]:
    def values(key: str) -> List[str]:
        unique = {doc.get("metadata", {}).get(key, "") for doc in documents}
        return sorted(v for v in unique if v)

    return {
        "language": ["da"],
        "year": values("year"),
        "document_type": values("document_type"),
        "ressort": values("ressort"),
        "authority": values("authority"),
        "historical": values("historical"),
        "geo": values("geo"),
    }


def build_metadata_index(documents: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    index_rows: List[Dict[str, Any]] = []
    for doc in documents:
        metadata = doc.get("metadata", {})
        index_rows.append(
            {
                "document_id": doc.get("document_id", ""),
                "title": doc.get("title", ""),
                "url": doc.get("url", ""),
                "full_text_path": doc.get("full_text_path", ""),
                "pdf_path": doc.get("pdf_path", ""),
                "html_path": doc.get("html_path", ""),
                "chunk_count": doc.get("chunk_count", 0),
                "text_length": doc.get("text_length", 0),
                "token_estimate": doc.get("token_estimate", 0),
                "metadata": metadata,
            }
        )
    return index_rows


def build_retrieval_text(metadata: Dict[str, Any], chunk: str) -> str:
    header = [
        f"Titel: {metadata.get('title', '')}",
        f"Populær titel: {metadata.get('popular_title', '')}",
        f"Dokumenttype: {metadata.get('document_type', '')}",
        f"Ressort: {metadata.get('ressort', '')}",
        f"År: {metadata.get('year', '')}",
        f"Nummer: {metadata.get('number', '')}",
    ]
    return "\n".join([line for line in header if line.strip().endswith(":") is False] + ["", chunk]).strip()


def run(args: argparse.Namespace) -> int:
    input_csv = Path(args.input).resolve()
    output_dir = Path(args.output).resolve()
    output_dir.mkdir(parents=True, exist_ok=True)

    rows = read_laws(input_csv)
    selected_rows = rows if args.limit is None else rows[: max(args.limit, 0)]
    print(f"[INFO] selected_rows: {len(selected_rows)}")
    retry_http_codes = parse_retry_codes(args.retry_http_codes)

    rag_dir = output_dir / "rag"
    rag_dir.mkdir(parents=True, exist_ok=True)

    documents: List[Dict[str, Any]] = []
    chunks: List[Dict[str, Any]] = []
    failures: List[Dict[str, Any]] = []
    items: List[Dict[str, Any]] = []
    recoveries: List[Dict[str, Any]] = []
    html_only_successes = 0
    seen_urls = set()

    run_started_utc = datetime.now(timezone.utc).isoformat()

    for idx, row in enumerate(selected_rows, start=1):
        print(f"[{idx}/{len(selected_rows)}] Henter lov: {row.get('Titel', '')} ({row.get('EliUrl', '')})")
        original_eli_url = row.get("EliUrl", "")
        if not original_eli_url:
            continue
        # Early skip if already fetched
        law_dir = build_target_dir(output_dir, row, original_eli_url)
        metadata_path = law_dir / "metadata.json"
        text_path = law_dir / "full_text_da.txt"
        if metadata_path.exists() and text_path.exists():
            try:
                # Check if files are non-empty and valid JSON
                if metadata_path.stat().st_size > 0 and text_path.stat().st_size > 0:
                    with open(metadata_path, encoding="utf-8") as f:
                        json.load(f)
                    print(f"[SKIP] Allerede hentet: {row.get('Titel', '')}")
                    skipped_existing = True
                    items.append({
                        "url": original_eli_url,
                        "resolved_url": original_eli_url,
                        "status": "skipped_existing",
                        "path": str(law_dir),
                        "title": row.get('Titel', ''),
                        "text_source": "existing",
                        "text_chars": text_path.stat().st_size,
                        "chunks": 0,
                    })
                    continue
            except Exception:
                pass
        try:
            title = row.get("Titel", "")
            law_dir = build_target_dir(output_dir, row, original_eli_url)
            law_dir.mkdir(parents=True, exist_ok=True)
            metadata_path = law_dir / "metadata.json"
            text_path = law_dir / "full_text_da.txt"
            html_path = law_dir / "source.html"
            pdf_path = law_dir / "source.pdf"
            rdfa_path = law_dir / "source.rdfa"
            candidate_base_urls = build_candidate_base_urls(row, args.url_fallback_mode, args.fallback_max_variants)
            skipped_existing = False
            html_result = None
            resolved_base_url = ""
            html_errors = []
            for candidate_url in candidate_base_urls:
                try:
                    html_result = fetch_url(
                        candidate_url,
                        timeout=args.timeout,
                        user_agent=args.user_agent,
                        retry_mode=args.retry_mode,
                        max_retries=args.max_retries,
                        retry_backoff_seconds=args.retry_backoff_seconds,
                        retry_backoff_multiplier=args.retry_backoff_multiplier,
                        retry_http_codes=retry_http_codes,
                    )
                    resolved_base_url = candidate_url
                    break
                except (HTTPError, URLError, Exception) as exc:
                    html_errors.append(f"{candidate_url} -> {type(exc).__name__}: {exc}")
            if html_result is None:
                raise RuntimeError("All base URL attempts failed: " + " | ".join(html_errors))
            html_bytes = html_result.body
            html_text = html_bytes.decode("utf-8", errors="replace")
            html_path.write_text(html_text, encoding="utf-8")
            pdf_result = None
            pdf_url = ""
            pdf_errors = []
            for base_for_pdf in _unique_preserve_order([resolved_base_url] + candidate_base_urls):
                candidate_pdf_url = f"{base_for_pdf.rstrip('/')}/pdf"
                try:
                    pdf_result = fetch_url(
                        candidate_pdf_url,
                        timeout=args.timeout,
                        user_agent=args.user_agent,
                        retry_mode=args.retry_mode,
                        max_retries=args.max_retries,
                        retry_backoff_seconds=args.retry_backoff_seconds,
                        retry_backoff_multiplier=args.retry_backoff_multiplier,
                        retry_http_codes=retry_http_codes,
                    )
                    pdf_url = candidate_pdf_url
                    break
                except (HTTPError, URLError, Exception) as exc:
                    pdf_errors.append(f"{candidate_pdf_url} -> {type(exc).__name__}: {exc}")
            pdf_available = pdf_result is not None
            pdf_bytes = b""
            if pdf_available:
                pdf_bytes = pdf_result.body
                pdf_path.write_bytes(pdf_bytes)
            elif not args.allow_html_only:
                raise RuntimeError("All PDF URL attempts failed: " + " | ".join(pdf_errors))
            else:
                html_only_successes += 1
            rdfa_url = f"{resolved_base_url.rstrip('/')}.rdfa"
            rdfa_text = ""
            rdfa_status = None
            rdfa_content_type = ""
            for base_for_rdfa in _unique_preserve_order([resolved_base_url] + candidate_base_urls):
                candidate_rdfa_url = f"{base_for_rdfa.rstrip('/')}.rdfa"
                try:
                    rdfa_result = fetch_url(
                        candidate_rdfa_url,
                        timeout=args.timeout,
                        user_agent=args.user_agent,
                        retry_mode=args.retry_mode,
                        max_retries=args.max_retries,
                        retry_backoff_seconds=args.retry_backoff_seconds,
                        retry_backoff_multiplier=args.retry_backoff_multiplier,
                        retry_http_codes=retry_http_codes,
                    )
                    rdfa_text = rdfa_result.body.decode("utf-8", errors="replace")
                    rdfa_path.write_text(rdfa_text, encoding="utf-8")
                    rdfa_status = rdfa_result.status_code
                    rdfa_content_type = rdfa_result.content_type
                    rdfa_url = candidate_rdfa_url
                    break
                except Exception:
                    continue
            full_text = ""
            text_source = "html" if not pdf_available else "pdf"
            if pdf_available:
                full_text = extract_text_from_pdf(pdf_bytes)
            html_full_text = extract_text_from_html(html_text)
            if len(html_full_text) > len(full_text):
                full_text = html_full_text
                text_source = "html"
            if len(full_text) < args.min_full_text_chars and sync_playwright is not None:
                print(f"[DEBUG] Prøver at hente lovtekst med Playwright for {original_eli_url}")
                fetched = None
                try:
                    with sync_playwright() as p:
                        browser = p.chromium.launch(headless=True)
                        page = browser.new_page()
                        page.goto(original_eli_url, timeout=60000)
                        page.wait_for_timeout(3000)
                        for selector in [".ri-tekst", "main", "body"]:
                            try:
                                content = page.query_selector(selector)
                                if content:
                                    text = content.inner_text().strip()
                                    if len(text) > 200:
                                        browser.close()
                                        text = clean_playwright_text(text)
                                        fetched = text
                                        break
                            except Exception:
                                continue
                        browser.close()
                except Exception as e:
                    print(f"[DEBUG] Playwright-fejl: {e}")
                if fetched and len(fetched) > len(full_text):
                    print(f"[DEBUG] Playwright fandt {len(fetched)} tegn for {original_eli_url}")
                    full_text = fetched
                    text_source = "playwright"
            if len(full_text) < args.min_full_text_chars:
                full_text = "\n".join(
                    part
                    for part in [
                        row.get("Titel", ""),
                        row.get("PopulærTitel", ""),
                        row.get("DokumentType", ""),
                        row.get("Ressort", ""),
                        original_eli_url,
                    ]
                    if part
                ).strip()
                text_source = "fallback"
            text_path.write_text(full_text, encoding="utf-8")
            if len(full_text) < 200:
                debug_html_path = law_dir / "debug_source.html"
                try:
                    if html_path.exists():
                        html_content = html_path.read_text(encoding="utf-8", errors="replace")
                        debug_html_path.write_text(html_content, encoding="utf-8", errors="replace")
                        print(f"[DEBUG] Gemte debug_source.html for {law_dir}")
                except Exception as e:
                    print(f"[DEBUG] Kunne ikke gemme debug_source.html for {law_dir}: {e}")
            page_title = extract_page_title(html_text)
            url_bits = urlparse(resolved_base_url)
            fetched_metadata = {
                "fetched_at_utc": datetime.now(timezone.utc).isoformat(),
                "source_url": resolved_base_url,
                "original_source_url": original_eli_url,
                "candidate_base_urls": candidate_base_urls,
                "pdf_url": pdf_url,
                "rdfa_url": rdfa_url,
                "source_domain": url_bits.netloc,
                "source_path": url_bits.path,
                "page_title": page_title,
                "html": {
                    "status_code": html_result.status_code,
                    "content_type": html_result.content_type,
                    "elapsed_ms": html_result.elapsed_ms,
                    "bytes": len(html_bytes),
                    "sha1": hashlib.sha1(html_bytes).hexdigest(),
                    "file": "source.html",
                },
                "pdf": {
                    "available": pdf_available,
                    "status_code": pdf_result.status_code if pdf_result else None,
                    "content_type": pdf_result.content_type if pdf_result else "",
                    "elapsed_ms": pdf_result.elapsed_ms if pdf_result else None,
                    "bytes": len(pdf_bytes) if pdf_available else 0,
                    "sha1": hashlib.sha1(pdf_bytes).hexdigest() if pdf_available else "",
                    "file": "source.pdf" if pdf_available else None,
                },
                "rdfa": {
                    "status_code": rdfa_status,
                    "content_type": rdfa_content_type,
                    "file": "source.rdfa" if rdfa_text else None,
                },
                "full_text": {
                    "file": "full_text_da.txt",
                    "chars": len(full_text),
                    "source": text_source,
                },
            }
            metadata = {
                "csv_row": row,
                "fetched": fetched_metadata,
                "language": "da",
                "source_type": "law",
            }
            write_json(metadata_path, metadata)
            if resolved_base_url != original_eli_url:
                recoveries.append(
                    {
                        "title": title,
                        "original_url": original_eli_url,
                        "resolved_url": resolved_base_url,
                        "pdf_url": pdf_url,
                    }
                )
            stored_meta = json.loads(metadata_path.read_text(encoding="utf-8"))
            fetched_meta = stored_meta.get("fetched", {})
            full_text = text_path.read_text(encoding="utf-8", errors="replace") if text_path.exists() else ""
            if not full_text and pdf_path.exists():
                full_text = extract_text_from_pdf(pdf_path.read_bytes())
            if not full_text and html_path.exists():
                full_text = extract_text_from_html(html_path.read_text(encoding="utf-8", errors="replace"))
            if not full_text:
                full_text = "\n".join(
                    part
                    for part in [
                        row.get("Titel", ""),
                        row.get("PopulærTitel", ""),
                        row.get("DokumentType", ""),
                        row.get("Ressort", ""),
                        original_eli_url,
                    ]
                    if part
                ).strip()
            text_source = fetched_meta.get("full_text", {}).get("source", "existing")
            page_title = fetched_meta.get("page_title", "")
            resolved_or_original_url = fetched_meta.get("source_url", original_eli_url)
            doc_id = build_document_id(row, original_eli_url)
            normalized_meta = {
                "title": row.get("Titel", "") or page_title,
                "popular_title": row.get("PopulærTitel", ""),
                "document_type": row.get("DokumentType", ""),
                "ressort": row.get("Ressort", ""),
                "authority": row.get("AdministrerendeMyndighed", ""),
                "year": row.get("År", ""),
                "number": row.get("Nummer", ""),
                "document_id": row.get("DokumentId", ""),
                "published_medium": row.get("Publiceringsmedie", ""),
                "published_date": row.get("BekendtgørelsesDato", ""),
                "publiceret_tidspunkt": row.get("PubliceretTidspunkt", ""),
                "signed_date": row.get("UnderskriftDato", ""),
                "historical": row.get("Historisk", ""),
                "geo": row.get("GeografiskDækning", ""),
                "url": resolved_or_original_url,
                "original_url": original_eli_url,
                "source_domain": fetched_meta.get("source_domain", ""),
                "source_path": fetched_meta.get("source_path", ""),
                "page_title": page_title,
                "language": "da",
                "text_source": text_source,
                "sha1_html": fetched_meta.get("html", {}).get("sha1", ""),
                "sha1_pdf": fetched_meta.get("pdf", {}).get("sha1", ""),
            }
            doc_record = {
                "document_id": doc_id,
                "url": resolved_or_original_url,
                "title": normalized_meta["title"],
                "full_text_path": str(text_path.relative_to(output_dir)).replace("\\", "/"),
                "html_path": str(html_path.relative_to(output_dir)).replace("\\", "/"),
                "pdf_path": str(pdf_path.relative_to(output_dir)).replace("\\", "/") if pdf_path.exists() else None,
                "rdfa_path": str(rdfa_path.relative_to(output_dir)).replace("\\", "/") if rdfa_path.exists() else None,
                "text_length": len(full_text),
                "token_estimate": estimate_tokens(full_text),
                "metadata": normalized_meta,
            }
            document_chunks = chunk_text(
                text=full_text,
                max_chars=args.chunk_size,
                overlap_chars=args.chunk_overlap,
                min_chunk_chars=args.min_chunk_size,
            )
            for chunk_index, chunk in enumerate(document_chunks):
                chunk_id = f"{doc_id}::chunk-{chunk_index:04d}"
                chunks.append(
                    {
                        "chunk_id": chunk_id,
                        "document_id": doc_id,
                        "chunk_index": chunk_index,
                        "text": chunk,
                        "token_estimate": estimate_tokens(chunk),
                        "metadata": normalized_meta,
                        "retrieval_text_da": build_retrieval_text(normalized_meta, chunk),
                    }
                )
            doc_record["chunk_count"] = len(document_chunks)
            documents.append(doc_record)
            items.append(
                {
                    "url": original_eli_url,
                    "resolved_url": resolved_or_original_url,
                    "status": "skipped_existing" if skipped_existing else "ok",
                    "path": str(law_dir),
                    "title": title,
                    "text_source": text_source,
                    "text_chars": len(full_text),
                    "chunks": len(document_chunks),
                }
            )
        except (HTTPError, URLError, Exception) as exc:
            failures.append(
                {
                    "url": original_eli_url,
                    "candidate_urls": candidate_base_urls,
                    "title": title,
                    "error_type": type(exc).__name__,
                    "reason": str(exc),
                }
            )
        if args.sleep_seconds > 0:
            time.sleep(args.sleep_seconds)

            document_chunks = chunk_text(
                text=full_text,
                max_chars=args.chunk_size,
                overlap_chars=args.chunk_overlap,
                min_chunk_chars=args.min_chunk_size,
            )

            for chunk_index, chunk in enumerate(document_chunks):
                chunk_id = f"{doc_id}::chunk-{chunk_index:04d}"
                chunks.append(
                    {
                        "chunk_id": chunk_id,
                        "document_id": doc_id,
                        "chunk_index": chunk_index,
                        "text": chunk,
                        "token_estimate": estimate_tokens(chunk),
                        "metadata": normalized_meta,
                        "retrieval_text_da": build_retrieval_text(normalized_meta, chunk),
                    }
                )

            doc_record["chunk_count"] = len(document_chunks)
            documents.append(doc_record)

            items.append(
                {
                    "url": original_eli_url,
                    "resolved_url": resolved_or_original_url,
                    "status": "skipped_existing" if skipped_existing else "ok",
                    "path": str(law_dir),
                    "title": title,
                    "text_source": text_source,
                    "text_chars": len(full_text),
                    "chunks": len(document_chunks),
                }
            )


        if args.sleep_seconds > 0:
            time.sleep(args.sleep_seconds)

    write_jsonl(rag_dir / "documents_da.jsonl", documents)
    write_jsonl(rag_dir / "chunks_da.jsonl", chunks)
    metadata_index = build_metadata_index(documents)
    (rag_dir / "metadata_index_da.json").write_text(
        json.dumps(metadata_index, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    rag_manifest = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "language": "da",
        "documents": len(documents),
        "chunks": len(chunks),
        "metadata_index_file": "metadata_index_da.json",
        "avg_chunks_per_document": round((len(chunks) / len(documents)), 2) if documents else 0,
        "chunk_size": args.chunk_size,
        "chunk_overlap": args.chunk_overlap,
        "min_chunk_size": args.min_chunk_size,
        "facets": collect_facets(documents),
    }
    write_json(rag_dir / "rag_manifest_da.json", rag_manifest)

    run_index = {
        "generated_at_utc": run_started_utc,
        "input_csv": str(input_csv),
        "output_dir": str(output_dir),
        "rows_total_with_eliurl": len(rows),
        "rows_considered": len(selected_rows),
        "items": len(items),
        "failed": len(failures),
        "recovered_via_fallback": len(recoveries),
        "html_only_successes": html_only_successes,
        "documents": len(documents),
        "chunks": len(chunks),
    }
    write_json(output_dir / "run_index.json", run_index)

    write_json(
        output_dir / "recovery_report.json",
        {
            "generated_at_utc": datetime.now(timezone.utc).isoformat(),
            "url_fallback_mode": args.url_fallback_mode,
            "recovered_count": len(recoveries),
            "recoveries": recoveries,
        },
    )

    with (output_dir / "run_failures.csv").open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=["url", "candidate_urls", "title", "error_type", "reason"])
        writer.writeheader()
        for failure in failures:
            writer.writerow(
                {
                    "url": failure.get("url", ""),
                    "candidate_urls": " | ".join(failure.get("candidate_urls", [])),
                    "title": failure.get("title", ""),
                    "error_type": failure.get("error_type", ""),
                    "reason": failure.get("reason", ""),
                }
            )

    print("\nRun complete")
    print(f"- Output folder: {output_dir}")
    print(f"- Processed items: {len(items)}")
    print(f"- Failures: {len(failures)}")
    print(f"- Recovered via URL fallback: {len(recoveries)}")
    print(f"- HTML-only successes: {html_only_successes}")
    print(f"- RAG documents (da): {len(documents)}")
    print(f"- RAG chunks (da): {len(chunks)}")

    return 0 if not failures else 2


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Fetch full text, PDF, and metadata for laws in CurrentLaws.csv using EliUrl, "
            "then build a Danish RAG corpus."
        )
    )
    parser.add_argument("--input", default="CurrentLaws.csv", help="Path to source CSV file")
    parser.add_argument("--output", default="laws_rag", help="Output directory")
    parser.add_argument("--limit", type=int, default=None, help="Only process first N rows")
    parser.add_argument("--timeout", type=int, default=40, help="HTTP timeout in seconds")
    parser.add_argument("--sleep-seconds", type=float, default=0.0, help="Pause between requests")
    parser.add_argument("--overwrite", action="store_true", help="Overwrite existing files")
    parser.add_argument(
        "--no-dedupe-urls",
        dest="dedupe_urls",
        action="store_false",
        help="Process duplicate EliUrl values too",
    )
    parser.set_defaults(dedupe_urls=True)

    parser.add_argument("--chunk-size", type=int, default=1600, help="Max chars per chunk")
    parser.add_argument("--chunk-overlap", type=int, default=260, help="Chars overlap between chunks")
    parser.add_argument("--min-chunk-size", type=int, default=220, help="Minimum chunk size")
    parser.add_argument(
        "--min-full-text-chars",
        type=int,
        default=300,
        help="Minimum full text length before fallback text synthesis",
    )
    parser.add_argument(
        "--user-agent",
        default="RAG-LA-Danish-LawFetcher/3.0 (+local-script)",
        help="HTTP User-Agent",
    )
    parser.add_argument(
        "--retry-mode",
        action="store_true",
        help="Enable automatic retry mode for HTTP/network failures",
    )
    parser.add_argument(
        "--max-retries",
        type=int,
        default=2,
        help="Maximum retries per request when --retry-mode is enabled",
    )
    parser.add_argument(
        "--retry-backoff-seconds",
        type=float,
        default=1.0,
        help="Base backoff seconds between retries",
    )
    parser.add_argument(
        "--retry-backoff-multiplier",
        type=float,
        default=2.0,
        help="Backoff multiplier per retry (exponential)",
    )
    parser.add_argument(
        "--retry-http-codes",
        default="408,429,500,502,503,504",
        help="Comma-separated HTTP status codes considered retryable",
    )
    parser.add_argument(
        "--url-fallback-mode",
        action="store_true",
        help="Try URL variants (including ACCN-based ELI URLs) when primary EliUrl fails",
    )
    parser.add_argument(
        "--fallback-max-variants",
        type=int,
        default=8,
        help="Maximum number of URL variants to try per law when --url-fallback-mode is enabled",
    )
    parser.add_argument(
        "--allow-html-only",
        action="store_true",
        help="Treat a law as successful even if PDF fetch fails, using HTML text only",
    )
    return parser


if __name__ == "__main__":
    parser = build_parser()
    args = parser.parse_args()
    try:
        raise SystemExit(run(args))
    except KeyboardInterrupt:
        print("Interrupted by user", file=sys.stderr)
        raise SystemExit(130)
