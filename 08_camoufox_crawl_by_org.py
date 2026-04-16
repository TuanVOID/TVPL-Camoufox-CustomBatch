"""
08_camoufox_crawl_by_org.py
===========================
TVPL crawler using Camoufox + Playwright (no Chrome CDP dependency).

Key goals:
- Persistent per-worker profile via Camoufox persistent context
- Proxy support per worker at launch time
- TVPL captcha handling with OCR + anti-loop recovery
- Cloudflare challenge handling (auto + manual fallback)
- JSONL output schema compatible with the chatbot pipeline
"""

from __future__ import annotations

import argparse
import asyncio
import io
import json
import logging
import os
import random
import re
import shutil
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, quote, unquote, urljoin, urlparse


try:
    from camoufox.async_api import AsyncCamoufox
except Exception:
    AsyncCamoufox = None  # type: ignore[assignment]


try:
    from playwright.async_api import TimeoutError as PlaywrightTimeoutError
except Exception:
    PlaywrightTimeoutError = TimeoutError  # type: ignore[assignment]


try:
    import pytesseract
    from PIL import Image, ImageFilter, ImageOps

    _default_tesseract = r"C:\Program Files\Tesseract-OCR\tesseract.exe"
    _env_tesseract = str(os.getenv("TESSERACT_CMD", "")).strip()
    if _env_tesseract:
        pytesseract.pytesseract.tesseract_cmd = _env_tesseract
    elif Path(_default_tesseract).exists():
        pytesseract.pytesseract.tesseract_cmd = _default_tesseract
    else:
        _bin = shutil.which("tesseract")
        if _bin:
            pytesseract.pytesseract.tesseract_cmd = _bin
    HAS_OCR = True
except Exception:
    HAS_OCR = False

try:
    from pypdf import PdfReader

    HAS_PDF_TEXT = True
except Exception:
    HAS_PDF_TEXT = False


BASE_URL = "https://thuvienphapluat.vn"
TODAY = datetime.now().strftime("%d/%m/%Y")
SEARCH_TPL = (
    BASE_URL
    + "/page/tim-van-ban.aspx?keyword=&area=0&type=0&status=0&lan=1&org={org}"
    + f"&signer=0&match=True&sort=1&bdate=12/04/1946&edate={TODAY}&page={{page}}"
)

ORG_MAP = {
    1: {"slug": "co-quan-tw", "name": "Co quan TW"},
    3: {"slug": "bhxh-viet-nam", "name": "BHXH Viet Nam"},
    6: {"slug": "bo-giao-duc-dao-tao", "name": "Bo Giao duc va Dao tao"},
    12: {"slug": "bo-noi-vu", "name": "Bo Noi vu"},
    19: {"slug": "bo-van-hoa-tt-dl", "name": "Bo Van hoa, TT va DL"},
    22: {"slug": "chinh-phu", "name": "Chinh phu"},
    23: {"slug": "chu-tich-nuoc", "name": "Chu tich nuoc"},
    26: {"slug": "quoc-hoi", "name": "Quoc hoi"},
    33: {"slug": "thu-tuong-chinh-phu", "name": "Thu tuong Chinh phu"},
    95: {"slug": "tong-ldld-viet-nam", "name": "Tong LDLD Viet Nam"},
    97: {"slug": "uy-ban-tvqh", "name": "Uy ban TVQH"},
    98: {"slug": "van-phong-chinh-phu", "name": "Van phong Chinh phu"},
    104: {"slug": "bo-dan-toc-ton-giao", "name": "Bo Dan toc va Ton giao"},
}

LISTING_LINK_SELECTORS = [
    "p.vblist > a",
    "p.vblist a",
    ".nqTitle a",
    "a[href*='/van-ban/']",
]

CONTENT_SELECTORS = [
    "div.content1 div.content1",
    "div.content1",
    ".box-content-vb",
    ".toan-van-container",
    "#toanvancontent",
    "article",
]

PDF_TAB_SELECTORS = [
    "a:has-text('Văn bản gốc/PDF')",
    "a:has-text('Van ban goc/PDF')",
    "a:has-text('PDF')",
]

METADATA_ROW_SELECTORS = [".boxTTVB .item", ".right-col p", ".ttvb .item"]

CAPTCHA_IMAGE_SELECTORS = [
    "img[src*='RegistImage']",
    "img[src*='registimage']",
    "img[id*='imgSecCode']",
    "img[id$='imgSecCode']",
    "img[alt*='captcha' i]",
]

CAPTCHA_INPUT_SELECTORS = [
    "#ctl00_Content_txtSecCode",
    "input[id$='txtSecCode']",
    "input[name$='txtSecCode']",
    "input[id*='SecCode']",
]

CAPTCHA_BUTTON_SELECTORS = [
    "#ctl00_Content_CheckButton",
    "input[id$='CheckButton']",
    "button[id$='CheckButton']",
    "input[type='submit'][value*='Xac' i]",
    "input[type='submit'][value*='Nhan' i]",
    "button:has-text('Xác nhận')",
    "button:has-text('Xac nhan')",
]

CAPTCHA_PAGE_HINT_TEXTS = [
    "mã bảo vệ",
    "ma bao ve",
    "xác nhận mã",
    "xac nhan ma",
    "robot",
    "mã captcha",
    "captcha",
]

TRIM_MARKERS = [
    "Luu tru\nGhi chu\nY kien",
    "Bai lien quan:",
    "Hoi dap phap luat",
    "Ban an lien quan",
    "Facebook\nEmail\nIn",
]

METADATA_LABELS = [
    ("Loai van ban", "loai_van_ban"),
    ("Loại văn bản", "loai_van_ban"),
    ("So hieu", "so_hieu"),
    ("Số hiệu", "so_hieu"),
    ("Co quan ban hanh", "co_quan"),
    ("Cơ quan ban hành", "co_quan"),
    ("Nguoi ky", "nguoi_ky"),
    ("Người ký", "nguoi_ky"),
    ("Ngay ban hanh", "ngay_ban_hanh"),
    ("Ngày ban hành", "ngay_ban_hanh"),
    ("Ngay hieu luc", "ngay_hieu_luc"),
    ("Ngày hiệu lực", "ngay_hieu_luc"),
    ("Tinh trang", "tinh_trang"),
    ("Tình trạng", "tinh_trang"),
    ("Linh vuc", "linh_vuc"),
    ("Lĩnh vực", "linh_vuc"),
]


def parse_orgs(orgs_text: str) -> list[int]:
    orgs: list[int] = []
    for part in orgs_text.split(","):
        part = part.strip()
        if not part:
            continue
        org = int(part)
        if org not in ORG_MAP:
            raise ValueError(f"Unknown org id: {org}")
        orgs.append(org)
    if not orgs:
        raise ValueError("Empty --orgs")
    return orgs


def parse_ranges(ranges_text: str) -> list[tuple[int, int]]:
    ranges: list[tuple[int, int]] = []
    for part in ranges_text.split(","):
        part = part.strip()
        if not part:
            continue
        if "-" not in part:
            raise ValueError(f"Invalid range: {part}")
        start_s, end_s = part.split("-", 1)
        start = int(start_s.strip())
        end = int(end_s.strip())
        if start < 1 or end < start:
            raise ValueError(f"Invalid range: {part}")
        ranges.append((start, end))
    if not ranges:
        raise ValueError("Empty --ranges")
    return ranges


def get_file_suffix(start_page: int, end_page: int) -> str:
    if start_page > 1:
        return f"_p{start_page:03d}"
    if end_page < 999:
        return "_p001"
    return ""


def get_output_file(output_dir: Path, org_id: int, start_page: int, end_page: int) -> Path:
    suffix = get_file_suffix(start_page, end_page)
    slug = ORG_MAP[org_id]["slug"]
    output_dir.mkdir(parents=True, exist_ok=True)
    return output_dir / f"org_{org_id:03d}_{slug}{suffix}.jsonl"


def normalize_url(url: str) -> str:
    return url.split("#")[0].split("?")[0].rstrip("/")


def is_same_listing_target(current_url: str, target_url: str) -> bool:
    """Compare listing URLs by path + org + page query params."""
    try:
        cur = urlparse(current_url)
        tar = urlparse(target_url)
    except Exception:
        return False

    cur_path = (cur.path or "").rstrip("/").lower()
    tar_path = (tar.path or "").rstrip("/").lower()
    equivalent_listing_paths = {
        "/page/tim-van-ban.aspx",
        "/page/searchlegal.aspx",
    }
    same_or_equivalent_path = (
        cur_path == tar_path
        or (cur_path in equivalent_listing_paths and tar_path in equivalent_listing_paths)
    )
    if not same_or_equivalent_path:
        return False

    cur_qs = parse_qs(cur.query or "", keep_blank_values=True)
    tar_qs = parse_qs(tar.query or "", keep_blank_values=True)

    for key in ("org", "page"):
        cur_val = (cur_qs.get(key) or [""])[0].strip()
        tar_val = (tar_qs.get(key) or [""])[0].strip()
        if cur_val != tar_val:
            return False
    return True


def load_crawled_urls(output_file: Path) -> set[str]:
    urls: set[str] = set()
    if not output_file.exists():
        return urls

    with output_file.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                row = json.loads(line)
            except json.JSONDecodeError:
                continue
            url = row.get("url")
            if isinstance(url, str) and url:
                urls.add(normalize_url(url))
    return urls


def repair_mojibake(text: str) -> str:
    if not text:
        return text

    bad_markers = ("Ã", "Â", "â", "Ð")
    if not any(m in text for m in bad_markers):
        return text

    def score(value: str) -> int:
        good_chars = "ăâđêôơưĂÂĐÊÔƠƯáàảãạấầẩẫậắằẳẵặéèẻẽẹếềểễệíìỉĩịóòỏõọốồổỗộớờởỡợúùủũụứừửữựýỳỷỹỵ"
        good = sum(1 for ch in value if ch in good_chars)
        bad = sum(value.count(m) for m in bad_markers)
        return good * 3 - bad

    candidates = [text]
    for src in ("latin-1", "cp1252"):
        try:
            candidates.append(text.encode(src, errors="ignore").decode("utf-8", errors="ignore"))
        except Exception:
            continue
    return max(candidates, key=score)


def parse_proxy_value(proxy_text: str) -> str | None:
    """
    Accept:
    - ip:port:user:password
    - ip:port
    - user:pass@ip:port
    - full URL: http://user:pass@ip:port
    """
    raw = proxy_text.strip()
    if not raw:
        return None

    if raw.startswith(("http://", "https://", "socks5://", "socks5h://")):
        return raw

    if "@" in raw and "://" not in raw:
        creds, hostport = raw.rsplit("@", 1)
        if ":" not in creds or ":" not in hostport:
            raise ValueError("Invalid proxy format: user:pass@ip:port")
        user, password = creds.split(":", 1)
        host, port = hostport.rsplit(":", 1)
        return f"http://{quote(user, safe='')}:{quote(password, safe='')}@{host}:{port}"

    parts = raw.split(":")
    if len(parts) == 2:
        host, port = parts
        return f"http://{host}:{port}"
    if len(parts) == 4:
        host, port, user, password = parts
        return f"http://{quote(user, safe='')}:{quote(password, safe='')}@{host}:{port}"

    raise ValueError(
        "Invalid proxy format. Use ip:port:user:password, user:pass@ip:port, ip:port, or URL."
    )


def proxy_url_to_launch_proxy(proxy_url: str | None) -> dict[str, str] | None:
    if not proxy_url:
        return None

    p = urlparse(proxy_url)
    if not p.scheme or not p.hostname or not p.port:
        raise ValueError(f"Invalid proxy URL: {proxy_url}")

    proxy: dict[str, str] = {"server": f"{p.scheme}://{p.hostname}:{p.port}"}
    if p.username is not None:
        proxy["username"] = unquote(p.username)
    if p.password is not None:
        proxy["password"] = unquote(p.password)
    return proxy


def infer_document_number(title: str, content: str, url: str) -> str:
    patterns = [
        r"\bS[ốo][:\s]+([0-9][0-9A-Za-zÀ-ỹĐđ/\-\.]{2,60})",
        r"\bSo[:\s]+([0-9][0-9A-Za-z/\-\.]{2,60})",
    ]
    for pat in patterns:
        m = re.search(pat, content, flags=re.IGNORECASE)
        if m:
            cand = m.group(1).strip(" .;:,")
            if re.search(r"\d", cand):
                return cand

    m = re.search(r"\b(\d{1,5}/[^\s]{1,40})", title)
    if m:
        cand = m.group(1).strip(" .;:,")
        if re.search(r"\d", cand) and "/" in cand:
            return cand

    m = re.search(r"/([A-Za-z0-9\-]+)-\d+\.aspx$", url)
    if m:
        token = m.group(1)
        m2 = re.search(r"(\d{1,5}-[A-Za-z0-9\-]{2,60})", token)
        if m2:
            return m2.group(1)
    return ""


def build_search_url(org_id: int, page: int) -> str:
    return SEARCH_TPL.format(org=org_id, page=page)


def build_ocr_variants(raw_img: Image.Image) -> list[Image.Image]:
    variants: list[Image.Image] = []
    gray = ImageOps.autocontrast(raw_img.convert("L"))
    for scale in (2, 3):
        up = gray.resize((gray.width * scale, gray.height * scale), Image.Resampling.LANCZOS)
        denoise = up.filter(ImageFilter.MedianFilter(size=3))
        for threshold in (90, 110, 130, 150, 170):
            bw = denoise.point(lambda x, t=threshold: 255 if x > t else 0)
            variants.append(bw)
            variants.append(ImageOps.invert(bw))
    return variants


def ocr_captcha_code(raw_img: Image.Image) -> str | None:
    if not HAS_OCR:
        return None

    configs = (
        "--oem 1 --psm 7 -c tessedit_char_whitelist=0123456789",
        "--oem 1 --psm 8 -c tessedit_char_whitelist=0123456789",
        "--oem 1 --psm 13 -c tessedit_char_whitelist=0123456789",
    )
    score_map: dict[str, int] = {}

    for img in build_ocr_variants(raw_img):
        for cfg in configs:
            text = pytesseract.image_to_string(img, config=cfg).strip()
            digits = re.sub(r"\D", "", text)
            if not digits:
                continue
            if not (5 <= len(digits) <= 8):
                continue
            score = 2 if len(digits) == 6 else 1
            score_map[digits] = score_map.get(digits, 0) + score

    if not score_map:
        return None
    return max(score_map.items(), key=lambda kv: kv[1])[0]


def extract_text_from_pdf_bytes(data: bytes) -> str:
    if not HAS_PDF_TEXT or not data:
        return ""

    try:
        reader = PdfReader(io.BytesIO(data))
    except Exception:
        return ""

    chunks: list[str] = []
    max_pages = min(len(reader.pages), 300)
    for idx in range(max_pages):
        try:
            txt = (reader.pages[idx].extract_text() or "").strip()
        except Exception:
            txt = ""
        if txt:
            chunks.append(txt)

    if not chunks:
        return ""

    text = "\n\n".join(chunks).strip()
    return repair_mojibake(text)


@dataclass
class CrawlStats:
    items: int = 0
    links: int = 0
    requests: int = 0
    listing_pages: int = 0
    blocked_pages: int = 0


class TVPLCamoufoxCrawler:
    def __init__(
        self,
        *,
        worker: str,
        org_ids: list[int],
        ranges: list[tuple[int, int]],
        delay: float,
        proxy_url: str | None,
        profile_dir: Path,
        output_dir: Path,
        headless: bool,
        viewport_width: int,
        viewport_height: int,
        single_page: int | None,
        cf_auto_attempts: int,
        cf_manual_wait: int,
        captcha_retries: int,
        captcha_manual_wait: int,
        navigation_retries: int,
    ) -> None:
        self.worker = worker
        self.org_ids = org_ids
        self.ranges = ranges
        self.base_delay = max(1.0, float(delay))
        self.proxy_url = proxy_url
        self.profile_dir = profile_dir
        self.output_dir = output_dir
        self.headless = headless
        self.viewport_width = max(800, int(viewport_width))
        self.viewport_height = max(600, int(viewport_height))
        self.single_page = single_page
        self.cf_auto_attempts = max(1, int(cf_auto_attempts))
        self.cf_manual_wait = max(0, int(cf_manual_wait))
        self.captcha_retries = max(1, int(captcha_retries))
        self.captcha_manual_wait = max(0, int(captcha_manual_wait))
        self.navigation_retries = max(1, int(navigation_retries))

        self.stats = CrawlStats()
        self.logger = logging.getLogger(__name__)

        self._outputs: dict[tuple[int, int, int], Path] = {}
        self._seen_urls: dict[str, set[str]] = {}

        for org_id in self.org_ids:
            for start_page, end_page in self.ranges:
                output_file = get_output_file(self.output_dir, org_id, start_page, end_page)
                key = str(output_file)
                self._outputs[(org_id, start_page, end_page)] = output_file
                self._seen_urls[key] = load_crawled_urls(output_file)

        self._cm: Any | None = None
        self._browser_obj: Any | None = None
        self.page: Any | None = None

    def _wait_seconds(self) -> float:
        low = max(1.0, self.base_delay - 2.0)
        high = max(low, self.base_delay + 2.0)
        return random.uniform(low, high)

    async def _sleep_request_delay(self) -> None:
        await asyncio.sleep(self._wait_seconds())

    @staticmethod
    def _is_redundant_blank_tab(url: str | None) -> bool:
        u = (url or "").strip().lower()
        if not u:
            return True
        if u in {"about:blank", "about:newtab", "chrome://newtab/"}:
            return True
        return u.startswith("about:blank") or u.startswith("about:newtab") or u.startswith("data:")

    @staticmethod
    def _is_non_content_url(url: str | None) -> bool:
        u = (url or "").strip().lower()
        if not u:
            return True
        if u.startswith("about:") or u.startswith("data:"):
            return True
        if u.startswith("chrome://") or u.startswith("moz-extension://"):
            return True
        return False

    def _page_score(self, url: str | None) -> int:
        u = (url or "").strip().lower()
        if not u:
            return -100
        if "/check.aspx" in u:
            return 120
        if "challenges.cloudflare.com" in u or "cf-challenge" in u:
            return 110
        if "thuvienphapluat.vn" in u:
            return 100
        if self._is_non_content_url(u):
            return -50
        return 10

    def _pick_best_page(self, pages: list[Any]) -> Any | None:
        if not pages:
            return None

        best_page: Any | None = None
        best_score = -10_000
        for p in pages:
            try:
                if p.is_closed():
                    continue
            except Exception:
                continue

            try:
                url = p.url
            except Exception:
                url = ""
            score = self._page_score(url)
            if score > best_score:
                best_score = score
                best_page = p
        return best_page

    async def _ensure_active_page(self) -> Any | None:
        if self.page is not None:
            try:
                if not self.page.is_closed():
                    return self.page
            except Exception:
                pass

        if self._browser_obj is None:
            return self.page

        try:
            pages = list(self._browser_obj.pages)
        except Exception:
            pages = []

        chosen: Any | None = None
        for p in pages:
            try:
                if p.is_closed():
                    continue
                chosen = p
                break
            except Exception:
                continue

        if chosen is None:
            try:
                chosen = await self._browser_obj.new_page()
            except Exception:
                chosen = None

        self.page = chosen
        if self.page is None:
            return None

        try:
            await self.page.bring_to_front()
        except Exception:
            pass
        try:
            await self.page.set_viewport_size({"width": self.viewport_width, "height": self.viewport_height})
        except Exception:
            pass
        return self.page

    async def _normalize_page_view(self) -> None:
        """Keep browser/page zoom and horizontal scroll stable across profiles."""
        await self._ensure_active_page()
        if not self.page:
            return

        try:
            await self.page.keyboard.press("Control+0")
        except Exception:
            pass

        try:
            await self.page.evaluate(
                """
                () => {
                    try { window.scrollTo(0, 0); } catch (e) {}
                    try {
                        if (document && document.documentElement) {
                            document.documentElement.scrollLeft = 0;
                            document.documentElement.scrollTop = 0;
                        }
                        if (document && document.body) {
                            document.body.scrollLeft = 0;
                            document.body.scrollTop = 0;
                        }
                    } catch (e) {}
                }
                """
            )
        except Exception:
            pass

    async def _close_extra_blank_tabs(self, keep_page: Any | None) -> int:
        if self._browser_obj is None:
            return 0
        try:
            pages = list(self._browser_obj.pages)
        except Exception:
            return 0

        closed = 0
        for p in pages:
            if keep_page is not None and p == keep_page:
                continue
            try:
                if not self._is_redundant_blank_tab(getattr(p, "url", "")):
                    continue
                await p.close()
                closed += 1
            except Exception:
                continue
        return closed

    async def start(self) -> None:
        if AsyncCamoufox is None:
            raise RuntimeError(
                "Cannot import Camoufox. Install first:\n"
                "  python -m pip install -U \"camoufox[geoip]\"\n"
                "  python -m camoufox fetch"
            )

        proxy_dict = proxy_url_to_launch_proxy(self.proxy_url)
        launch_kwargs: dict[str, Any] = {
            "persistent_context": True,
            "user_data_dir": str(self.profile_dir),
            "headless": self.headless,
            "humanize": True,
            "block_webrtc": True,
            "disable_coop": True,
        }
        if proxy_dict:
            launch_kwargs["proxy"] = proxy_dict
            launch_kwargs["geoip"] = True

        self.profile_dir.mkdir(parents=True, exist_ok=True)
        self._cm = AsyncCamoufox(**launch_kwargs)
        self._browser_obj = await self._cm.__aenter__()
        self.page = await self._browser_obj.new_page()
        reused_first_tab = False
        closed_tabs = 0
        await self._ensure_active_page()
        await self._normalize_page_view()

        self.logger.info(
            "[W%s] Camoufox started: profile=%s proxy=%s headless=%s viewport=%dx%d reused_first_tab=%s closed_blank_tabs=%d",
            self.worker,
            self.profile_dir,
            "on" if proxy_dict else "off",
            self.headless,
            self.viewport_width,
            self.viewport_height,
            reused_first_tab,
            closed_tabs,
        )
        self.logger.info(
            "[W%s] OCR status: %s (tesseract=%s)",
            self.worker,
            "on" if HAS_OCR else "off",
            getattr(getattr(pytesseract, "pytesseract", None), "tesseract_cmd", "n/a") if HAS_OCR else "n/a",
        )
        self.logger.info("[W%s] PDF text extractor: %s", self.worker, "on" if HAS_PDF_TEXT else "off")

    async def close(self) -> None:
        if self.page is not None:
            try:
                await self.page.close()
            except Exception:
                pass
            self.page = None

        if self._cm is not None:
            try:
                await self._cm.__aexit__(None, None, None)
            finally:
                self._cm = None
                self._browser_obj = None

    def _is_check_page(self) -> bool:
        if not self.page:
            return False
        return "/check.aspx" in ((self.page.url or "").lower())

    async def _is_tvpl_captcha_page(self) -> bool:
        if not self.page:
            return False

        if self._is_check_page():
            return True

        try:
            inp_count = await self.page.locator(
                "#ctl00_Content_txtSecCode, input[id$='txtSecCode'], input[id*='SecCode']"
            ).count()
            img_count = await self.page.locator(
                "img[src*='RegistImage'], img[src*='registimage'], img[id*='imgSecCode']"
            ).count()
            if inp_count > 0 and img_count > 0:
                return True
        except Exception:
            pass

        try:
            body = (await self.page.locator("body").inner_text(timeout=1200)).lower()
            if any(hint in body for hint in CAPTCHA_PAGE_HINT_TEXTS):
                return True
        except Exception:
            pass

        return False

    async def _is_cloudflare_challenge(self) -> bool:
        await self._ensure_active_page()
        if not self.page:
            return False
        try:
            title = ((await self.page.title()) or "").strip().lower()
        except Exception:
            title = ""

        if "just a moment" in title:
            return True

        try:
            html = ((await self.page.content()) or "").lower()
        except Exception:
            html = ""

        return (
            "verifying you are human" in html
            or "verify you are human" in html
            or "performing security verification" in html
            or "security verification" in html
            or "this website uses a security service" in html
            or "challenges.cloudflare.com" in html
            or "cf-turnstile" in html
            or "cf_turnstile" in html
            or "cf-browser-verification" in html
        )

    async def _click_cloudflare_widget(self) -> bool:
        await self._ensure_active_page()
        if not self.page:
            return False

        try:
            await self.page.bring_to_front()
        except Exception:
            pass

        selectors = [
            "iframe[src*='challenges.cloudflare.com']",
            "iframe[src*='turnstile']",
            "iframe[title*='Cloudflare']",
            "#cf-turnstile",
            "#cf_turnstile",
            ".cf-turnstile",
            "div[data-sitekey]",
            ".main-content p+div>div>div",
        ]

        for sel in selectors:
            try:
                loc = self.page.locator(sel).first
                if await loc.count() == 0:
                    continue

                try:
                    await loc.scroll_into_view_if_needed(timeout=1200)
                except Exception:
                    pass

                try:
                    await loc.click(timeout=1200, force=True)
                    self.logger.info("[W%s] Clicked Cloudflare selector: %s", self.worker, sel)
                    return True
                except Exception:
                    pass

                box = await loc.bounding_box()
                if box and box.get("width", 0) > 0 and box.get("height", 0) > 0:
                    width = float(box["width"])
                    height = float(box["height"])
                    x_left = box["x"] + min(max(14.0, width * 0.14), max(14.0, width - 14.0))
                    y_mid = box["y"] + min(max(10.0, height * 0.52), max(10.0, height - 10.0))
                    try:
                        await self.page.mouse.click(x_left, y_mid, delay=random.randint(90, 200))
                        self.logger.info(
                            "[W%s] Clicked Cloudflare bbox-left selector: %s at (%.1f, %.1f)",
                            self.worker,
                            sel,
                            x_left,
                            y_mid,
                        )
                        return True
                    except Exception:
                        x_center = box["x"] + max(8.0, width * 0.5)
                        y_center = box["y"] + max(8.0, height * 0.5)
                        try:
                            await self.page.mouse.click(x_center, y_center, delay=random.randint(90, 200))
                            self.logger.info(
                                "[W%s] Clicked Cloudflare bbox-center selector: %s at (%.1f, %.1f)",
                                self.worker,
                                sel,
                                x_center,
                                y_center,
                            )
                            return True
                        except Exception:
                            pass
            except Exception:
                continue

        # Fallback: click inside challenge iframe bounds.
        try:
            for frame in self.page.frames:
                f_url = (frame.url or "").lower()
                if "challenges.cloudflare.com" not in f_url:
                    continue
                frame_el = await frame.frame_element()
                if frame_el is None:
                    continue
                box = await frame_el.bounding_box()
                if not box or box.get("width", 0) <= 0 or box.get("height", 0) <= 0:
                    continue
                width = float(box["width"])
                height = float(box["height"])
                x = box["x"] + min(max(14.0, width * 0.14), max(14.0, width - 14.0))
                y = box["y"] + min(max(10.0, height * 0.52), max(10.0, height - 10.0))
                await self.page.mouse.click(x, y, delay=random.randint(90, 200))
                self.logger.info(
                    "[W%s] Clicked Cloudflare iframe fallback at (%.1f, %.1f).",
                    self.worker,
                    x,
                    y,
                )
                return True
        except Exception:
            pass

        return False

    async def _handle_cloudflare(self) -> bool:
        if not await self._is_cloudflare_challenge():
            return True

        self.stats.blocked_pages += 1

        for attempt in range(1, self.cf_auto_attempts + 1):
            clicked = await self._click_cloudflare_widget()
            if not self.page:
                return False
            await self.page.wait_for_timeout(3500 if clicked else 1800)
            if not await self._is_cloudflare_challenge():
                self.logger.info("[W%s] Cloudflare solved automatically.", self.worker)
                return True
            self.logger.warning(
                "[W%s] Cloudflare still active (auto attempt %d/%d).",
                self.worker,
                attempt,
                self.cf_auto_attempts,
            )

        if self.cf_manual_wait <= 0:
            return False

        self.logger.warning(
            "[W%s] Waiting %ds for manual Cloudflare solve...",
            self.worker,
            self.cf_manual_wait,
        )
        for sec in range(self.cf_manual_wait):
            if not await self._is_cloudflare_challenge():
                self.logger.info("[W%s] Cloudflare solved manually.", self.worker)
                return True
            if sec > 0 and sec % 10 == 0:
                self.logger.info("[W%s] Manual CF wait: %ds", self.worker, sec)
            if self.page is not None:
                await self.page.wait_for_timeout(1000)
            else:
                await asyncio.sleep(1)
        return not await self._is_cloudflare_challenge()

    async def _wait_manual_captcha(self) -> bool:
        if self.captcha_manual_wait <= 0:
            return False
        self.logger.warning(
            "[W%s] Waiting %ds for manual TVPL captcha solve...",
            self.worker,
            self.captcha_manual_wait,
        )
        for sec in range(self.captcha_manual_wait):
            await self._ensure_active_page()
            if not await self._is_tvpl_captcha_page():
                return True
            if sec > 0 and sec % 10 == 0:
                self.logger.info("[W%s] Manual captcha wait: %ds", self.worker, sec)
            if self.page is not None:
                await self.page.wait_for_timeout(1000)
            else:
                await asyncio.sleep(1)
        return not await self._is_tvpl_captcha_page()

    async def _find_first_visible_locator(self, selectors: list[str]) -> Any | None:
        await self._ensure_active_page()
        if not self.page:
            return None

        fallback: Any | None = None
        for sel in selectors:
            try:
                loc = self.page.locator(sel).first
                if await loc.count() == 0:
                    continue
                if fallback is None:
                    fallback = loc
                try:
                    await loc.scroll_into_view_if_needed(timeout=700)
                except Exception:
                    pass
                try:
                    if await loc.is_visible():
                        return loc
                except Exception:
                    return loc
            except Exception:
                continue
        return fallback

    async def _find_captcha_elements(self) -> tuple[Any | None, Any | None, Any | None]:
        img = await self._find_first_visible_locator(CAPTCHA_IMAGE_SELECTORS)
        inp = await self._find_first_visible_locator(CAPTCHA_INPUT_SELECTORS)
        btn = await self._find_first_visible_locator(CAPTCHA_BUTTON_SELECTORS)
        return img, inp, btn

    async def _solve_tvpl_captcha_and_return(self, target_url: str) -> bool:
        await self._ensure_active_page()
        if not self.page:
            return False

        target_norm = normalize_url(target_url)
        self.logger.info("[W%s] TVPL captcha flow start: current_url=%s", self.worker, (self.page.url or "").strip())

        for attempt in range(1, self.captcha_retries + 1):
            await self._ensure_active_page()
            if not self.page:
                return False

            try:
                if not await self._is_tvpl_captcha_page():
                    current_url = self.page.url or ""
                    current_norm = normalize_url(current_url)

                    if "/van-ban/" in target_url and current_norm != target_norm:
                        self.logger.info(
                            "[W%s] Post-captcha redirect to target doc: current=%s target=%s",
                            self.worker,
                            current_url,
                            target_url,
                        )
                        self.stats.requests += 1
                        await self.page.goto(target_url, wait_until="domcontentloaded", timeout=65000)
                        await self.page.wait_for_timeout(1000)
                        if await self._is_tvpl_captcha_page():
                            continue
                    elif "/page/" in target_url and not is_same_listing_target(current_url, target_url):
                        self.logger.info(
                            "[W%s] Post-captcha redirect to target listing: current=%s target=%s",
                            self.worker,
                            current_url,
                            target_url,
                        )
                        self.stats.requests += 1
                        await self.page.goto(target_url, wait_until="domcontentloaded", timeout=65000)
                        await self.page.wait_for_timeout(1000)
                        if await self._is_tvpl_captcha_page():
                            continue
                    await self._normalize_page_view()
                    return True

                if not HAS_OCR:
                    self.logger.warning(
                        "[W%s] OCR unavailable (Tesseract missing). Waiting manual captcha solve...",
                        self.worker,
                    )
                    return await self._wait_manual_captcha()

                img, inp, btn = await self._find_captcha_elements()
                if img is None:
                    self.logger.warning(
                        "[W%s] Captcha image not found on check page (attempt %d/%d).",
                        self.worker,
                        attempt,
                        self.captcha_retries,
                    )
                    return await self._wait_manual_captcha()

                try:
                    png = await img.screenshot()
                    code = ocr_captcha_code(Image.open(io.BytesIO(png)))
                except Exception:
                    code = None

                if not code:
                    self.logger.info(
                        "[W%s] OCR returned empty (attempt %d/%d), refreshing captcha.",
                        self.worker,
                        attempt,
                        self.captcha_retries,
                    )
                    await self.page.wait_for_timeout(900)
                    if attempt >= self.captcha_retries:
                        return await self._wait_manual_captcha()
                    try:
                        await img.click(timeout=600)
                    except Exception:
                        pass
                    continue

                if inp is None or btn is None:
                    self.logger.warning(
                        "[W%s] Captcha input/button not found (attempt %d/%d).",
                        self.worker,
                        attempt,
                        self.captcha_retries,
                    )
                    return await self._wait_manual_captcha()

                self.logger.info("[W%s] Captcha OCR attempt %d/%d -> %s", self.worker, attempt, self.captcha_retries, code)
                try:
                    await inp.click(timeout=1200)
                except Exception:
                    pass
                try:
                    await inp.fill("")
                except Exception:
                    pass
                typed_ok = False
                try:
                    await inp.type(code, delay=random.randint(55, 120))
                    val = re.sub(r"\D", "", await inp.input_value())
                    typed_ok = (val == code) or val.endswith(code)
                except Exception:
                    typed_ok = False

                if not typed_ok:
                    try:
                        await inp.click(timeout=800)
                    except Exception:
                        pass
                    try:
                        await inp.fill(code)
                    except Exception as exc:
                        self.logger.warning(
                            "[W%s] Captcha fill fallback failed (attempt %d/%d): %s",
                            self.worker,
                            attempt,
                            self.captcha_retries,
                            exc,
                        )
                        await self.page.wait_for_timeout(900)
                        continue

                try:
                    await btn.click(timeout=1200, force=True)
                except Exception:
                    try:
                        await btn.click(timeout=1200)
                    except Exception:
                        await self.page.keyboard.press("Enter")

                try:
                    await self.page.wait_for_load_state("domcontentloaded", timeout=5000)
                except Exception:
                    pass
                await self.page.wait_for_timeout(2200)

                if not await self._is_tvpl_captcha_page():
                    current_url = self.page.url or ""
                    current_norm = normalize_url(current_url)
                    if "/van-ban/" in target_url and current_norm != target_norm:
                        self.logger.info(
                            "[W%s] Captcha passed but wrong doc URL, redirecting: current=%s target=%s",
                            self.worker,
                            current_url,
                            target_url,
                        )
                        self.stats.requests += 1
                        await self.page.goto(target_url, wait_until="domcontentloaded", timeout=65000)
                        await self.page.wait_for_timeout(900)
                    elif "/page/" in target_url and not is_same_listing_target(current_url, target_url):
                        self.logger.info(
                            "[W%s] Captcha passed but wrong listing URL, redirecting: current=%s target=%s",
                            self.worker,
                            current_url,
                            target_url,
                        )
                        self.stats.requests += 1
                        await self.page.goto(target_url, wait_until="domcontentloaded", timeout=65000)
                        await self.page.wait_for_timeout(900)
                    await self._normalize_page_view()
                    return not await self._is_tvpl_captcha_page()

                self.logger.info(
                    "[W%s] Captcha still not passed after OCR attempt %d/%d.",
                    self.worker,
                    attempt,
                    self.captcha_retries,
                )
                try:
                    await img.click(timeout=600)
                except Exception:
                    pass
                await self.page.wait_for_timeout(900)
            except Exception as exc:
                self.logger.warning(
                    "[W%s] Captcha solve exception (attempt %d/%d): %s",
                    self.worker,
                    attempt,
                    self.captcha_retries,
                    exc,
                )
                try:
                    await self.page.wait_for_timeout(900)
                except Exception:
                    pass
                continue

        return await self._wait_manual_captcha()

    async def _resolve_interstitials(self, target_url: str) -> bool:
        for _ in range(self.navigation_retries + self.captcha_retries + 3):
            await self._ensure_active_page()
            if not self.page:
                return False

            if await self._is_cloudflare_challenge():
                if not await self._handle_cloudflare():
                    return False
                continue

            if await self._is_tvpl_captcha_page():
                self.logger.info("[W%s] TVPL captcha detected at: %s", self.worker, (self.page.url or "").strip())
                if not await self._solve_tvpl_captcha_and_return(target_url):
                    return False
                continue
            return True
        return False

    async def _goto_with_recovery(self, target_url: str, purpose: str) -> bool:
        await self._ensure_active_page()
        if not self.page:
            return False

        for attempt in range(1, self.navigation_retries + 1):
            await self._ensure_active_page()
            if not self.page:
                return False
            self.stats.requests += 1
            try:
                await self.page.goto(target_url, wait_until="domcontentloaded", timeout=65000)
            except PlaywrightTimeoutError:
                self.logger.warning(
                    "[W%s] Timeout while opening %s (attempt %d/%d): %s",
                    self.worker,
                    purpose,
                    attempt,
                    self.navigation_retries,
                    target_url,
                )
            except Exception as exc:
                self.logger.warning(
                    "[W%s] Error while opening %s (attempt %d/%d): %s (%s)",
                    self.worker,
                    purpose,
                    attempt,
                    self.navigation_retries,
                    target_url,
                    exc,
                )

            await self.page.wait_for_timeout(700)
            await self._normalize_page_view()

            if await self._resolve_interstitials(target_url):
                return True

            backoff = min(20.0, self.base_delay + attempt * 2.0)
            self.logger.warning(
                "[W%s] Recovery failed for %s (attempt %d/%d), backoff %.1fs.",
                self.worker,
                purpose,
                attempt,
                self.navigation_retries,
                backoff,
            )
            await asyncio.sleep(backoff)

        return False

    async def _extract_first_text(self, selectors: list[str]) -> str:
        if not self.page:
            return ""
        for sel in selectors:
            try:
                node = self.page.locator(sel).first
                if await node.count() == 0:
                    continue
                txt = (await node.inner_text()).strip()
            except Exception:
                txt = ""
            if txt:
                return repair_mojibake(" ".join(txt.split()))
        return ""

    async def _extract_content(self) -> str:
        if not self.page:
            return ""
        for sel in CONTENT_SELECTORS:
            try:
                nodes = self.page.locator(sel)
                cnt = await nodes.count()
                if cnt == 0:
                    continue
                for i in range(cnt):
                    node = nodes.nth(i)
                    txt = (await node.inner_text()).strip()
                    txt = repair_mojibake(txt)
                    if len(txt) > 200:
                        for marker in TRIM_MARKERS:
                            idx = txt.find(marker)
                            if idx > 0:
                                txt = txt[:idx].rstrip()
                        return txt
            except Exception:
                continue
        return ""

    async def _try_open_pdf_tab(self) -> bool:
        if not self.page:
            return False
        for sel in PDF_TAB_SELECTORS:
            try:
                tab = self.page.locator(sel).first
                if await tab.count() == 0:
                    continue
                await tab.click(timeout=1800)
                await self.page.wait_for_timeout(900)
                return True
            except Exception:
                continue
        return False

    def _expand_pdf_candidate_url(self, raw_url: str, base_url: str) -> list[str]:
        if not raw_url:
            return []
        raw = raw_url.strip().replace("&amp;", "&")
        if not raw or raw.lower().startswith("javascript:"):
            return []

        candidates: list[str] = []
        abs_url = urljoin(base_url, raw)
        if ".pdf" in abs_url.lower():
            candidates.append(abs_url)

        try:
            parsed = urlparse(abs_url)
            qs = parse_qs(parsed.query or "", keep_blank_values=True)
        except Exception:
            qs = {}
        for key in ("file", "pdf", "url", "src", "doc", "document"):
            vals = qs.get(key) or []
            for v in vals:
                vv = unquote(v).strip()
                if not vv:
                    continue
                cand = urljoin(base_url, vv)
                if ".pdf" in cand.lower():
                    candidates.append(cand)

        return candidates

    async def _collect_pdf_candidate_urls(self) -> list[str]:
        if not self.page:
            return []
        base_url = self.page.url or BASE_URL
        found: list[str] = []
        seen: set[str] = set()

        raw_values: list[str] = []
        try:
            vals = await self.page.evaluate(
                """
                () => {
                    const out = [];
                    const nodes = document.querySelectorAll("a[href], iframe[src], embed[src], object[data], source[src]");
                    for (const n of nodes) {
                        const href = n.getAttribute("href");
                        const src = n.getAttribute("src");
                        const data = n.getAttribute("data");
                        if (href) out.push(href);
                        if (src) out.push(src);
                        if (data) out.push(data);
                    }
                    return out;
                }
                """
            )
            if isinstance(vals, list):
                raw_values.extend(str(x) for x in vals)
        except Exception:
            pass

        try:
            html = await self.page.content()
            raw_values.extend(
                re.findall(r"""(?:src|href|data)\s*=\s*["']([^"']+)["']""", html, flags=re.IGNORECASE)
            )
        except Exception:
            pass

        for raw in raw_values:
            for cand in self._expand_pdf_candidate_url(raw, base_url):
                if cand in seen:
                    continue
                seen.add(cand)
                found.append(cand)

        return found

    async def _download_pdf_bytes(self, pdf_url: str) -> bytes | None:
        await self._ensure_active_page()
        if not self.page:
            return None
        try:
            resp = await self.page.request.get(pdf_url, timeout=65000, fail_on_status_code=False)
        except Exception:
            return None

        try:
            body = await resp.body()
        except Exception:
            return None
        if not body:
            return None

        ctype = (resp.headers.get("content-type", "") or "").lower()
        if body.startswith(b"%PDF") or "pdf" in ctype:
            return body
        return None

    async def _extract_pdf_content_from_page(self) -> tuple[str, str]:
        if not HAS_PDF_TEXT:
            return "", ""

        candidates = await self._collect_pdf_candidate_urls()
        if not candidates:
            opened = await self._try_open_pdf_tab()
            if opened:
                candidates = await self._collect_pdf_candidate_urls()

        for pdf_url in candidates[:12]:
            data = await self._download_pdf_bytes(pdf_url)
            if not data:
                continue
            content = extract_text_from_pdf_bytes(data)
            if len(content) >= 200:
                return content, pdf_url
        return "", ""

    async def _extract_content_with_pdf_fallback(self) -> tuple[str, str, str]:
        html_content = await self._extract_content()
        best_content = html_content
        source = "html"
        pdf_url = ""

        if len(best_content) < 200:
            pdf_content, found_pdf_url = await self._extract_pdf_content_from_page()
            if len(pdf_content) > len(best_content):
                best_content = pdf_content
                source = "pdf"
                pdf_url = found_pdf_url

        return best_content, source, pdf_url

    async def _extract_meta(self) -> dict[str, str]:
        meta: dict[str, str] = {}
        if not self.page:
            return meta

        for sel in METADATA_ROW_SELECTORS:
            try:
                rows = self.page.locator(sel)
                cnt = await rows.count()
            except Exception:
                cnt = 0
            if cnt == 0:
                continue

            for i in range(cnt):
                try:
                    txt = (await rows.nth(i).inner_text()).strip()
                except Exception:
                    txt = ""
                if not txt:
                    continue
                txt = repair_mojibake(" ".join(txt.split()))
                lower = txt.lower()
                for label, key in METADATA_LABELS:
                    if label.lower() not in lower:
                        continue
                    val = re.sub(re.escape(label), "", txt, flags=re.IGNORECASE).strip().lstrip(":").strip()
                    if val:
                        meta[key] = val
            if meta:
                return meta
        return meta

    async def _collect_listing_links(self) -> list[str]:
        if not self.page:
            return []

        found: list[str] = []
        current_url = self.page.url or BASE_URL

        for sel in LISTING_LINK_SELECTORS:
            try:
                links = await self.page.query_selector_all(sel)
            except Exception:
                links = []

            for link in links:
                try:
                    href = await link.get_attribute("href")
                except Exception:
                    href = None
                if not href:
                    continue
                abs_url = normalize_url(urljoin(current_url, href))
                if "/van-ban/" not in abs_url or ".aspx" not in abs_url:
                    continue
                if abs_url not in found:
                    found.append(abs_url)

            if found:
                break

        return found

    async def _crawl_document(
        self,
        *,
        doc_url: str,
        org_id: int,
        page_num: int,
        output_file: Path,
        seen: set[str],
    ) -> bool:
        if not self.page:
            return False

        ok = await self._goto_with_recovery(doc_url, "document")
        if not ok:
            self.logger.warning("[W%s] Skip document after retries: %s", self.worker, doc_url)
            self.logger.info(
                "[W%s] DOC_STATUS %s",
                self.worker,
                json.dumps(
                    {
                        "org": org_id,
                        "page": page_num,
                        "url": doc_url,
                        "status": "failed",
                        "reason": "navigation",
                    },
                    ensure_ascii=False,
                ),
            )
            return False

        final_url = normalize_url(self.page.url or doc_url)
        if final_url in seen:
            self.logger.info(
                "[W%s] DOC_STATUS %s",
                self.worker,
                json.dumps(
                    {
                        "org": org_id,
                        "page": page_num,
                        "url": doc_url,
                        "final_url": final_url,
                        "status": "seen",
                    },
                    ensure_ascii=False,
                ),
            )
            return False

        title = await self._extract_first_text(["h1", ".doc-title", ".title", "title"])
        title = title.replace(" - THU VIEN PHAP LUAT", "").replace(" - THƯ VIỆN PHÁP LUẬT", "")
        content, content_source, pdf_url = await self._extract_content_with_pdf_fallback()
        if content_source == "pdf":
            self.logger.info(
                "[W%s] PDF fallback content extracted: chars=%d url=%s",
                self.worker,
                len(content),
                pdf_url or "n/a",
            )
        if not content:
            self.logger.info(
                "[W%s] DOC_STATUS %s",
                self.worker,
                json.dumps(
                    {
                        "org": org_id,
                        "page": page_num,
                        "url": doc_url,
                        "final_url": final_url,
                        "status": "failed",
                        "reason": "empty_content",
                    },
                    ensure_ascii=False,
                ),
            )
            return False

        meta = await self._extract_meta()
        if content_source != "html":
            meta["content_source"] = content_source
        if pdf_url:
            meta["pdf_url"] = pdf_url
        doc_number = (meta.get("so_hieu") or "").strip()
        if not doc_number:
            doc_number = infer_document_number(title, content, final_url)
            if doc_number:
                meta["so_hieu"] = doc_number

        row = {
            "url": final_url,
            "title": title,
            "content": content,
            "meta": meta,
            "document_number": doc_number,
            "source": "thuvienphapluat",
            "crawl_time": datetime.now().isoformat(timespec="seconds"),
        }

        output_file.parent.mkdir(parents=True, exist_ok=True)
        with output_file.open("a", encoding="utf-8") as f:
            f.write(json.dumps(row, ensure_ascii=False) + "\n")

        seen.add(final_url)
        self.stats.items += 1
        self.logger.info("[W%s] wrote #%d org=%d -> %s", self.worker, self.stats.items, org_id, output_file.name)
        self.logger.info(
            "[W%s] DOC_STATUS %s",
            self.worker,
            json.dumps(
                {
                    "org": org_id,
                    "page": page_num,
                    "url": doc_url,
                    "final_url": final_url,
                    "status": "ok",
                },
                ensure_ascii=False,
            ),
        )
        return True

    async def crawl_listing_page(self, *, org_id: int, start_page: int, end_page: int, page_num: int) -> tuple[int, int]:
        if not self.page:
            return 0, 0

        output_file = self._outputs[(org_id, start_page, end_page)]
        seen = self._seen_urls.setdefault(str(output_file), set())
        url = build_search_url(org_id, page_num)

        ok = await self._goto_with_recovery(url, "listing")
        if not ok:
            self.logger.warning("[W%s] listing failed org=%d page=%d", self.worker, org_id, page_num)
            self.logger.info(
                "[W%s] PAGE_LISTING_FAIL %s",
                self.worker,
                json.dumps({"org": org_id, "page": page_num}, ensure_ascii=False),
            )
            return 0, 0

        links = await self._collect_listing_links()
        self.stats.listing_pages += 1
        self.stats.links += len(links)
        self.logger.info(
            "[W%s] PAGE_MANIFEST %s",
            self.worker,
            json.dumps(
                {
                    "org": org_id,
                    "page": page_num,
                    "links": links,
                },
                ensure_ascii=False,
            ),
        )

        if links:
            self.logger.info("[W%s] org=%d page=%d -> +%d links", self.worker, org_id, page_num, len(links))
        else:
            self.logger.info("[W%s] org=%d page=%d -> no links", self.worker, org_id, page_num)
            return 0, 0

        written = 0
        for doc_url in links:
            if doc_url in seen:
                self.logger.info(
                    "[W%s] DOC_STATUS %s",
                    self.worker,
                    json.dumps(
                        {
                            "org": org_id,
                            "page": page_num,
                            "url": doc_url,
                            "final_url": doc_url,
                            "status": "seen",
                        },
                        ensure_ascii=False,
                    ),
                )
                continue
            if await self._crawl_document(
                doc_url=doc_url,
                org_id=org_id,
                page_num=page_num,
                output_file=output_file,
                seen=seen,
            ):
                written += 1
            await self._sleep_request_delay()

        return written, len(links)

    async def run(self) -> CrawlStats:
        await self.start()
        try:
            for org_id in self.org_ids:
                for start_page, end_page in self.ranges:
                    if self.single_page is not None:
                        if not (start_page <= self.single_page <= end_page):
                            continue
                        pages = [self.single_page]
                    else:
                        pages = list(range(start_page, end_page + 1))

                    for page_num in pages:
                        await self.crawl_listing_page(
                            org_id=org_id,
                            start_page=start_page,
                            end_page=end_page,
                            page_num=page_num,
                        )
                        await self._sleep_request_delay()
        finally:
            await self.close()
        return self.stats


def parse_args() -> argparse.Namespace:
    script_dir = Path(__file__).resolve().parent

    parser = argparse.ArgumentParser(description="TVPL crawler using Camoufox")
    parser.add_argument("--worker", type=str, default="w1", help="Worker id label, e.g. w1")
    parser.add_argument("--orgs", type=str, default="", help="Org ids, comma-separated")
    parser.add_argument("--ranges", type=str, default="1-999", help="Page ranges, e.g. 1-50,101-150")
    parser.add_argument("--single-page", type=int, default=None, help="Only crawl this listing page")
    parser.add_argument("--delay", type=float, default=18.0, help="Base delay in seconds")
    parser.add_argument("--proxy", type=str, default="", help="Proxy format supported by parser")
    parser.add_argument("--profile-dir", type=str, default="", help="Persistent profile directory")
    parser.add_argument("--headless", action="store_true", help="Run browser in headless mode")
    parser.add_argument("--viewport-width", type=int, default=1600, help="Viewport width in pixels")
    parser.add_argument("--viewport-height", type=int, default=900, help="Viewport height in pixels")
    parser.add_argument("--cf-auto-attempts", type=int, default=3, help="Cloudflare auto attempts")
    parser.add_argument("--cf-manual-wait", type=int, default=30, help="Cloudflare manual wait in seconds")
    parser.add_argument("--captcha-retries", type=int, default=8, help="Captcha OCR retries")
    parser.add_argument("--captcha-manual-wait", type=int, default=30, help="Manual captcha wait in seconds")
    parser.add_argument("--navigation-retries", type=int, default=4, help="Navigation attempts per URL")
    parser.add_argument("--output-dir", type=str, default=str(script_dir / "output"), help="Output directory")
    parser.add_argument("--state-dir", type=str, default=str(script_dir / "state"), help="State directory")
    parser.add_argument("--list", action="store_true", help="List org ids")
    return parser.parse_args()


async def async_main(args: argparse.Namespace) -> int:
    if args.list:
        print("\nOrg list:")
        for oid in sorted(ORG_MAP):
            print(f"  {oid:>3} | {ORG_MAP[oid]['name']}")
        return 0

    if not args.orgs.strip():
        print("Argument error: --orgs is required unless --list is used.")
        return 2

    try:
        org_ids = parse_orgs(args.orgs)
        ranges = parse_ranges(args.ranges)
    except Exception as exc:
        print(f"Argument error: {exc}")
        return 2

    if args.single_page is not None and args.single_page < 1:
        print("Argument error: --single-page must be >= 1")
        return 2

    try:
        proxy_url = parse_proxy_value(args.proxy)
    except Exception as exc:
        print(f"Proxy error: {exc}")
        return 2

    state_dir = Path(args.state_dir)
    output_dir = Path(args.output_dir)
    profile_dir = Path(args.profile_dir) if args.profile_dir.strip() else state_dir / "camoufox_profiles" / args.worker

    crawler = TVPLCamoufoxCrawler(
        worker=args.worker,
        org_ids=org_ids,
        ranges=ranges,
        delay=args.delay,
        proxy_url=proxy_url,
        profile_dir=profile_dir,
        output_dir=output_dir,
        headless=bool(args.headless),
        viewport_width=args.viewport_width,
        viewport_height=args.viewport_height,
        single_page=args.single_page,
        cf_auto_attempts=args.cf_auto_attempts,
        cf_manual_wait=args.cf_manual_wait,
        captcha_retries=args.captcha_retries,
        captcha_manual_wait=args.captcha_manual_wait,
        navigation_retries=args.navigation_retries,
    )

    started = time.time()
    print(
        f"[START] worker={args.worker} orgs={org_ids} ranges={ranges} "
        f"single_page={args.single_page} proxy={'on' if proxy_url else 'off'} profile={profile_dir}",
        flush=True,
    )

    try:
        stats = await crawler.run()
    except KeyboardInterrupt:
        print("Interrupted by user.")
        return 130
    except Exception as exc:
        print(f"Crawler error: {exc}")
        return 1

    elapsed = time.time() - started
    print(
        "Done. completed=True, items={}, links={}, requests={}, elapsed={:.1f}s".format(
            stats.items,
            stats.links,
            stats.requests,
            elapsed,
        )
    )
    return 0


def main() -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)-7s | %(message)s",
        datefmt="%H:%M:%S",
    )
    args = parse_args()
    return asyncio.run(async_main(args))


if __name__ == "__main__":
    raise SystemExit(main())
