from __future__ import annotations

import argparse
import json
import re
import subprocess
import sys
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlparse


SCRIPT_DIR = Path(__file__).resolve().parent
CRAWLER_FILE = SCRIPT_DIR / "08_camoufox_crawl_by_org.py"

DEFAULT_STATE_DIR = SCRIPT_DIR / "state"
DEFAULT_OUTPUT_DIR = SCRIPT_DIR / "output"
DEFAULT_PROFILES_ROOT = DEFAULT_STATE_DIR / "camoufox_profiles"
DEFAULT_LOG_DIR = SCRIPT_DIR / "logs" / "camoufox_custom_batch"
DEFAULT_RESUME_STATE_DIR = DEFAULT_STATE_DIR / "custom_batch_resume"
RESUME_LAYOUT = "custom_batch_resume_v2"

ORG_SLUGS = {
    1: "co-quan-tw",
    3: "bhxh-viet-nam",
    6: "bo-giao-duc-dao-tao",
    12: "bo-noi-vu",
    19: "bo-van-hoa-tt-dl",
    22: "chinh-phu",
    23: "chu-tich-nuoc",
    26: "quoc-hoi",
    33: "thu-tuong-chinh-phu",
    95: "tong-ldld-viet-nam",
    97: "uy-ban-tvqh",
    98: "van-phong-chinh-phu",
    104: "bo-dan-toc-ton-giao",
}


@dataclass
class Task:
    org_id: int
    start_page: int
    end_page: int
    docs_target: int
    listing_url: str


@dataclass
class Integrity:
    done: bool
    missing_pages: list[int]
    missing_links_by_page: dict[int, list[str]]
    manifest_total: int
    success_total: int


WORKERS = [f"w{i}" for i in range(1, 9)]
DEFAULT_WORKER_PLANS: dict[str, list[Task]] = {wid: [] for wid in WORKERS}


def now_iso() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%S")


def normalize_url(url: str) -> str:
    raw = str(url or "").strip()
    if not raw:
        return ""
    return raw.split("#")[0].split("?")[0].rstrip("/")


def get_file_suffix(start_page: int, end_page: int) -> str:
    if start_page > 1:
        return f"_p{start_page:03d}"
    if end_page < 999:
        return "_p001"
    return ""


def get_output_file(output_dir: Path, org_id: int, start_page: int, end_page: int) -> Path:
    slug = ORG_SLUGS.get(org_id, f"org-{org_id}")
    return output_dir / f"org_{org_id:03d}_{slug}{get_file_suffix(start_page, end_page)}.jsonl"


def _parse_org_and_page_from_url(url: str) -> tuple[int, int]:
    parsed = urlparse(url.strip())
    qs = parse_qs(parsed.query or "", keep_blank_values=True)
    org_vals = qs.get("org") or qs.get("Org")
    page_vals = qs.get("page") or qs.get("Page")
    if not org_vals:
        raise ValueError(f"URL missing org parameter: {url}")
    org_id = int(org_vals[0].strip())
    page = int(str(page_vals[0]).strip()) if page_vals and str(page_vals[0]).strip() else 1
    if org_id < 0 or page < 1:
        raise ValueError(f"Invalid org/page in URL: {url}")
    return org_id, page


def parse_plan_arg(plan_text: str) -> list[Task]:
    text = (plan_text or "").strip()
    if not text:
        return []
    tasks: list[Task] = []
    for chunk in text.split(";"):
        item = chunk.strip()
        if not item:
            continue
        if "," not in item:
            raise ValueError(f"Invalid plan item: {item}")
        docs_s, url = item.split(",", 1)
        docs = int(docs_s.strip())
        if docs <= 0:
            raise ValueError(f"Docs must be > 0: {item}")
        listing_url = url.strip()
        org_id, start_page = _parse_org_and_page_from_url(listing_url)
        pages = max(1, (docs + 19) // 20)
        tasks.append(
            Task(
                org_id=org_id,
                start_page=start_page,
                end_page=start_page + pages - 1,
                docs_target=docs,
                listing_url=listing_url,
            )
        )
    return tasks


def resolve_worker_plans(args: argparse.Namespace) -> dict[str, list[Task]]:
    raw = {wid: str(getattr(args, f"plan_{wid}", "") or "").strip() for wid in WORKERS}
    if not any(raw.values()):
        return DEFAULT_WORKER_PLANS
    return {wid: parse_plan_arg(raw[wid]) for wid in WORKERS}


def _plan_signature(tasks: list[Task]) -> list[dict[str, Any]]:
    return [
        {
            "org_id": t.org_id,
            "start_page": t.start_page,
            "end_page": t.end_page,
            "docs_target": t.docs_target,
            "listing_url": t.listing_url,
        }
        for t in tasks
    ]


def _resume_file(resume_state_dir: Path, worker_id: str) -> Path:
    resume_state_dir.mkdir(parents=True, exist_ok=True)
    return resume_state_dir / f"{worker_id}.json"


def _default_state(worker_id: str, tasks: list[Task]) -> dict[str, Any]:
    first_page = tasks[0].start_page if tasks else 1
    return {
        "layout": RESUME_LAYOUT,
        "worker": worker_id,
        "task_index": 0,
        "next_page": first_page,
        "completed": False,
        "plan": _plan_signature(tasks),
        "task_progress": {},
        "updated_at": now_iso(),
    }


def _clamp_state(state: dict[str, Any], tasks: list[Task]) -> dict[str, Any]:
    if not isinstance(state.get("task_progress"), dict):
        state["task_progress"] = {}
    if not tasks:
        state["task_index"] = 0
        state["next_page"] = 1
        state["completed"] = True
        return state
    try:
        idx_raw = int(state.get("task_index", 0))
    except Exception:
        idx_raw = 0
    idx = max(0, min(idx_raw, len(tasks)))
    state["task_index"] = idx
    if idx >= len(tasks):
        state["completed"] = True
        state["next_page"] = tasks[-1].end_page + 1
        return state
    task = tasks[idx]
    try:
        nxt_raw = int(state.get("next_page", task.start_page))
    except Exception:
        nxt_raw = task.start_page
    nxt = nxt_raw
    state["next_page"] = max(task.start_page, min(nxt, task.end_page + 1))
    state["completed"] = bool(state.get("completed", False))
    return state


def _load_state(worker_id: str, tasks: list[Task], resume_state_dir: Path, reset_resume: bool) -> tuple[dict[str, Any], str]:
    path = _resume_file(resume_state_dir, worker_id)
    if reset_resume or (not path.exists()):
        return _clamp_state(_default_state(worker_id, tasks), tasks), ("reset" if reset_resume else "fresh_init")
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        raw = {}
    if not isinstance(raw, dict):
        return _clamp_state(_default_state(worker_id, tasks), tasks), "invalid_state_reset"

    current_sig = _plan_signature(tasks)
    if raw.get("plan") != current_sig:
        return _clamp_state(_default_state(worker_id, tasks), tasks), "plan_changed_reset"

    old_layout = str(raw.get("layout", "")).strip()
    if old_layout != RESUME_LAYOUT:
        has_progress = isinstance(raw.get("task_progress"), dict) and bool(raw.get("task_progress"))
        completed = bool(raw.get("completed", False))
        # Keep only active in-flight state; reset completed/empty old state to avoid false skip.
        if has_progress and (not completed):
            raw = dict(raw)
            raw["layout"] = RESUME_LAYOUT
            raw.setdefault("worker", worker_id)
            raw.setdefault("plan", current_sig)
            return _clamp_state(raw, tasks), "layout_migrated"
        return _clamp_state(_default_state(worker_id, tasks), tasks), "layout_changed_reset"

    raw.setdefault("layout", RESUME_LAYOUT)
    raw.setdefault("worker", worker_id)
    raw.setdefault("task_progress", {})
    return _clamp_state(raw, tasks), path.name


def _save_state(path: Path, state: dict[str, Any]) -> None:
    state["updated_at"] = now_iso()
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(path)


def _task_key(task_index: int, task: Task) -> str:
    return f"{task_index}:{task.org_id}:{task.start_page}-{task.end_page}:{task.docs_target}"


def _dedup_urls(values: list[Any]) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for v in values:
        u = normalize_url(str(v))
        if not u or u in seen:
            continue
        seen.add(u)
        out.append(u)
    return out


def _merge_pdf_link_files(output_dir: Path, workers: list[str]) -> tuple[Path, int]:
    merged_file = output_dir / "pdf_urls_all.txt"
    seen: set[str] = set()
    merged: list[str] = []
    for wid in workers:
        src = output_dir / f"pdf_urls_{wid}.txt"
        if not src.exists():
            continue
        try:
            with src.open("r", encoding="utf-8") as f:
                for line in f:
                    link = str(line or "").strip()
                    if not link or link in seen:
                        continue
                    seen.add(link)
                    merged.append(link)
        except Exception:
            continue
    merged_file.parent.mkdir(parents=True, exist_ok=True)
    with merged_file.open("w", encoding="utf-8") as f:
        for link in merged:
            f.write(link + "\n")
    return merged_file, len(merged)


def _collect_worker_missing_links(
    *,
    worker_id: str,
    tasks: list[Task],
    resume_state_dir: Path,
) -> tuple[list[str], list[int]]:
    if not tasks:
        return [], []

    state_file = _resume_file(resume_state_dir, worker_id)
    if not state_file.exists():
        return [], []

    try:
        raw = json.loads(state_file.read_text(encoding="utf-8"))
    except Exception:
        return [], []

    if not isinstance(raw, dict):
        return [], []

    task_progress = raw.get("task_progress", {})
    if not isinstance(task_progress, dict):
        return [], []

    miss_links: list[str] = []
    miss_pages: set[int] = set()

    for idx, task in enumerate(tasks):
        rec = task_progress.get(_task_key(idx, task))
        if not isinstance(rec, dict):
            miss_pages.update(range(task.start_page, task.end_page + 1))
            continue
        integrity = _evaluate_integrity(task, rec)
        miss_pages.update(integrity.missing_pages)
        for links in integrity.missing_links_by_page.values():
            miss_links.extend(links)

    return sorted(_dedup_urls(miss_links)), sorted(miss_pages)


def _write_worker_missing_links(output_dir: Path, worker_id: str, links: list[str]) -> tuple[Path, int]:
    out = output_dir / f"missing_links_{worker_id}.txt"
    out.parent.mkdir(parents=True, exist_ok=True)
    with out.open("w", encoding="utf-8") as f:
        for link in links:
            f.write(link + "\n")
    return out, len(links)


def _merge_missing_link_files(output_dir: Path, workers: list[str]) -> tuple[Path, int]:
    merged_file = output_dir / "missing_links_all.txt"
    seen: set[str] = set()
    merged: list[str] = []
    for wid in workers:
        src = output_dir / f"missing_links_{wid}.txt"
        if not src.exists():
            continue
        try:
            with src.open("r", encoding="utf-8") as f:
                for line in f:
                    link = normalize_url(str(line or "").strip())
                    if not link or link in seen:
                        continue
                    seen.add(link)
                    merged.append(link)
        except Exception:
            continue
    merged_file.parent.mkdir(parents=True, exist_ok=True)
    with merged_file.open("w", encoding="utf-8") as f:
        for link in merged:
            f.write(link + "\n")
    return merged_file, len(merged)


def _ensure_task_progress(state: dict[str, Any], task_index: int, task: Task) -> dict[str, Any]:
    tp = state.setdefault("task_progress", {})
    if not isinstance(tp, dict):
        tp = {}
        state["task_progress"] = tp
    key = _task_key(task_index, task)
    cur = tp.get(key)
    if not isinstance(cur, dict):
        cur = {
            "org_id": task.org_id,
            "start_page": task.start_page,
            "end_page": task.end_page,
            "docs_target": task.docs_target,
            "verify_round": 0,
            "pages": {},
            "last_summary": {},
        }
        tp[key] = cur
    if not isinstance(cur.get("pages"), dict):
        cur["pages"] = {}
    return cur


def _ensure_page_rec(task_progress: dict[str, Any], page_num: int) -> dict[str, Any]:
    pages = task_progress.setdefault("pages", {})
    key = str(page_num)
    rec = pages.get(key)
    if not isinstance(rec, dict):
        rec = {"listing_ok": False, "attempts": 0, "manifest": [], "success_urls": [], "failed_urls": []}
        pages[key] = rec
    rec.setdefault("listing_ok", False)
    rec.setdefault("attempts", 0)
    rec.setdefault("manifest", [])
    rec.setdefault("success_urls", [])
    rec.setdefault("failed_urls", [])
    return rec


def _extract_progress_page(line: str, expected_org: int) -> int | None:
    m = re.search(r"\borg=(\d+)\s+page=(\d+)\s+->", line)
    if m and int(m.group(1)) == expected_org:
        return int(m.group(2))
    m2 = re.search(r"\blisting failed org=(\d+)\s+page=(\d+)\b", line)
    if m2 and int(m2.group(1)) == expected_org:
        return int(m2.group(2))
    return None


def _extract_json_payload(line: str, marker: str) -> dict[str, Any] | None:
    idx = line.find(marker)
    if idx < 0:
        return None
    txt = line[idx + len(marker) :].strip()
    if not txt.startswith("{"):
        return None
    try:
        obj = json.loads(txt)
    except Exception:
        return None
    return obj if isinstance(obj, dict) else None


def _update_manifest(task_progress: dict[str, Any], page_num: int, links: list[Any]) -> None:
    rec = _ensure_page_rec(task_progress, page_num)
    rec["manifest"] = _dedup_urls(list(links))
    rec["listing_ok"] = True
    rec["attempts"] = int(rec.get("attempts", 0)) + 1
    rec["success_urls"] = _dedup_urls(list(rec.get("success_urls", [])))
    rec["failed_urls"] = [u for u in _dedup_urls(list(rec.get("failed_urls", []))) if u not in set(rec["success_urls"])]


def _update_listing_fail(task_progress: dict[str, Any], page_num: int) -> None:
    rec = _ensure_page_rec(task_progress, page_num)
    rec["attempts"] = int(rec.get("attempts", 0)) + 1


def _update_doc_status(task_progress: dict[str, Any], page_num: int, payload: dict[str, Any]) -> None:
    status = str(payload.get("status", "")).strip().lower()
    urls = _dedup_urls([payload.get("url", ""), payload.get("final_url", "")])
    if not status or not urls:
        return
    rec = _ensure_page_rec(task_progress, page_num)
    ok = set(_dedup_urls(list(rec.get("success_urls", []))))
    failed = set(_dedup_urls(list(rec.get("failed_urls", []))))
    if status in {"ok", "seen"}:
        ok.update(urls)
        for u in urls:
            if u in failed:
                failed.remove(u)
    elif status == "failed":
        for u in urls:
            if u not in ok:
                failed.add(u)
    rec["success_urls"] = sorted(ok)
    rec["failed_urls"] = sorted(failed)


def _evaluate_integrity(task: Task, task_progress: dict[str, Any]) -> Integrity:
    pages = task_progress.get("pages", {})
    missing_pages: list[int] = []
    missing_links_by_page: dict[int, list[str]] = {}
    manifest_total = 0
    success_total = 0

    for p in range(task.start_page, task.end_page + 1):
        rec = pages.get(str(p))
        if not isinstance(rec, dict):
            missing_pages.append(p)
            continue
        manifest = set(_dedup_urls(list(rec.get("manifest", []))))
        if not bool(rec.get("listing_ok", False)) and not manifest:
            missing_pages.append(p)
            continue
        success = set(_dedup_urls(list(rec.get("success_urls", []))))
        manifest_total += len(manifest)
        success_total += len(manifest & success)
        miss = sorted(manifest - success)
        if miss:
            missing_links_by_page[p] = miss

    done = (len(missing_pages) == 0) and (len(missing_links_by_page) == 0)
    return Integrity(done=done, missing_pages=missing_pages, missing_links_by_page=missing_links_by_page, manifest_total=manifest_total, success_total=success_total)


def _integrity_text(task: Task, i: Integrity) -> str:
    return (
        f"org={task.org_id} pages={task.start_page}-{task.end_page} "
        f"ok={i.success_total}/{i.manifest_total} target~{task.docs_target} "
        f"missing_pages={len(i.missing_pages)} missing_links={len(i.missing_links_by_page)}"
    )


def _build_cmd(
    *,
    python_bin: str,
    worker_id: str,
    task: Task,
    range_start: int,
    range_end: int,
    output_base_start: int | None,
    output_base_end: int | None,
    listing_url: str | None,
    delay: float,
    viewport_width: int,
    viewport_height: int,
    profile_dir: Path,
    state_dir: Path,
    output_dir: Path,
    cf_manual_wait: int,
    captcha_manual_wait: int,
    captcha_retries: int,
    navigation_retries: int,
    proxy: str,
    headless: bool,
    single_page: int | None = None,
) -> list[str]:
    cmd = [
        python_bin, "-u", str(CRAWLER_FILE),
        "--worker", worker_id,
        "--orgs", str(task.org_id),
        "--ranges", f"{range_start}-{range_end}",
        "--delay", str(delay),
        "--viewport-width", str(viewport_width),
        "--viewport-height", str(viewport_height),
        "--profile-dir", str(profile_dir),
        "--state-dir", str(state_dir),
        "--output-dir", str(output_dir),
        "--cf-manual-wait", str(cf_manual_wait),
        "--captcha-manual-wait", str(captcha_manual_wait),
        "--captcha-retries", str(captcha_retries),
        "--navigation-retries", str(navigation_retries),
    ]
    if single_page is not None:
        cmd.extend(["--single-page", str(single_page)])
    if output_base_start is not None and output_base_end is not None:
        cmd.extend(["--output-base-start", str(output_base_start), "--output-base-end", str(output_base_end)])
    if str(listing_url or "").strip():
        cmd.extend(["--listing-url", str(listing_url).strip()])
    if proxy.strip():
        cmd.extend(["--proxy", proxy.strip()])
    if headless:
        cmd.append("--headless")
    return cmd


def _run_crawler(
    *,
    cmd: list[str],
    task: Task,
    task_index: int,
    page_min: int,
    page_max: int,
    state: dict[str, Any],
    state_file: Path,
    task_progress: dict[str, Any],
) -> int:
    proc = subprocess.Popen(cmd, cwd=str(SCRIPT_DIR), stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, bufsize=1)
    dirty = False
    last_save = time.monotonic()
    try:
        if proc.stdout is not None:
            for raw in proc.stdout:
                print(raw, end="", flush=True)
                changed = False

                page = _extract_progress_page(raw, task.org_id)
                if page is not None and page_min <= page <= page_max:
                    nxt = min(task.end_page + 1, page + 1)
                    if nxt > int(state.get("next_page", page_min)) or int(state.get("task_index", -1)) != task_index:
                        state["task_index"] = task_index
                        state["next_page"] = nxt
                        state["completed"] = False
                        changed = True

                payload = _extract_json_payload(raw, "PAGE_MANIFEST ")
                if payload:
                    try:
                        org = int(payload.get("org", -1))
                        pnum = int(payload.get("page", -1))
                    except Exception:
                        org = -1
                        pnum = -1
                    links = payload.get("links", [])
                    if org == task.org_id and page_min <= pnum <= page_max and isinstance(links, list):
                        _update_manifest(task_progress, pnum, links)
                        changed = True

                payload = _extract_json_payload(raw, "PAGE_LISTING_FAIL ")
                if payload:
                    try:
                        org = int(payload.get("org", -1))
                        pnum = int(payload.get("page", -1))
                    except Exception:
                        org = -1
                        pnum = -1
                    if org == task.org_id and page_min <= pnum <= page_max:
                        _update_listing_fail(task_progress, pnum)
                        changed = True

                payload = _extract_json_payload(raw, "DOC_STATUS ")
                if payload:
                    try:
                        org = int(payload.get("org", -1))
                        pnum = int(payload.get("page", -1))
                    except Exception:
                        org = -1
                        pnum = -1
                    if org == task.org_id and page_min <= pnum <= page_max:
                        _update_doc_status(task_progress, pnum, payload)
                        changed = True

                if changed:
                    state["task_index"] = task_index
                    state["completed"] = False
                    dirty = True
                    if time.monotonic() - last_save >= 2.0:
                        _save_state(state_file, state)
                        dirty = False
                        last_save = time.monotonic()
    finally:
        code = proc.wait()
    if dirty:
        _save_state(state_file, state)
    return int(code)


def run_worker_plan(
    *,
    worker_id: str,
    tasks: list[Task],
    python_bin: str,
    delay: float,
    viewport_width: int,
    viewport_height: int,
    cf_manual_wait: int,
    captcha_manual_wait: int,
    captcha_retries: int,
    navigation_retries: int,
    verify_max_rounds: int,
    proxy: str,
    state_dir: Path,
    resume_state_dir: Path,
    output_dir: Path,
    profiles_root: Path,
    headless: bool,
    reset_resume: bool,
) -> int:
    profile_dir = profiles_root / worker_id
    profile_dir.mkdir(parents=True, exist_ok=True)

    state_file = _resume_file(resume_state_dir, worker_id)
    state, source = _load_state(worker_id, tasks, resume_state_dir, reset_resume)
    _save_state(state_file, state)

    print(f"[{now_iso()}] [{worker_id}] profile={profile_dir} proxy={'on' if proxy.strip() else 'off'} viewport={viewport_width}x{viewport_height}", flush=True)
    print(f"[{now_iso()}] [{worker_id}] resume_source={source} task_index={state.get('task_index')} next_page={state.get('next_page')} completed={state.get('completed')}", flush=True)

    if bool(state.get("completed", False)):
        print(f"[{now_iso()}] [{worker_id}] already completed from resume state, skip.", flush=True)
        return 0

    try:
        start_idx_raw = int(state.get("task_index", 0))
    except Exception:
        start_idx_raw = 0
    start_idx = max(0, min(start_idx_raw, len(tasks)))

    for i in range(start_idx, len(tasks)):
        task = tasks[i]
        task_progress = _ensure_task_progress(state, i, task)

        if i == start_idx:
            try:
                start_page_raw = int(state.get("next_page", task.start_page))
            except Exception:
                start_page_raw = task.start_page
            start_page = max(task.start_page, min(start_page_raw, task.end_page + 1))
        else:
            start_page = task.start_page

        if start_page <= task.end_page:
            print(f"[{now_iso()}] [{worker_id}] task {i + 1}/{len(tasks)} org={task.org_id} pages={start_page}-{task.end_page} target_docs~{task.docs_target} (base={task.start_page}-{task.end_page})", flush=True)
            cmd = _build_cmd(
                python_bin=python_bin,
                worker_id=worker_id,
                task=task,
                range_start=start_page,
                range_end=task.end_page,
                output_base_start=task.start_page,
                output_base_end=task.end_page,
                listing_url=task.listing_url,
                delay=delay,
                viewport_width=viewport_width,
                viewport_height=viewport_height,
                profile_dir=profile_dir,
                state_dir=state_dir,
                output_dir=output_dir,
                cf_manual_wait=cf_manual_wait,
                captcha_manual_wait=captcha_manual_wait,
                captcha_retries=captcha_retries,
                navigation_retries=navigation_retries,
                proxy=proxy,
                headless=headless,
            )
            rc = _run_crawler(
                cmd=cmd,
                task=task,
                task_index=i,
                page_min=start_page,
                page_max=task.end_page,
                state=state,
                state_file=state_file,
                task_progress=task_progress,
            )
            print(f"[{now_iso()}] [{worker_id}] task rc={rc}", flush=True)
            if rc != 0:
                return rc

        integrity = _evaluate_integrity(task, task_progress)
        task_progress["last_summary"] = {
            "checked_at": now_iso(),
            "manifest_total": integrity.manifest_total,
            "success_total": integrity.success_total,
            "missing_pages": integrity.missing_pages,
            "missing_links_pages": sorted(list(integrity.missing_links_by_page.keys())),
        }
        _save_state(state_file, state)

        verify_round = int(task_progress.get("verify_round", 0))
        while not integrity.done:
            if verify_round >= max(1, int(verify_max_rounds)):
                print(f"[{now_iso()}] [{worker_id}] integrity NOT satisfied after {verify_round} rounds: {_integrity_text(task, integrity)}", flush=True)
                state["task_index"] = i
                state["next_page"] = max(task.start_page, min(int(state.get("next_page", task.end_page + 1)), task.end_page + 1))
                state["completed"] = False
                _save_state(state_file, state)
                return 2

            pages_to_retry = sorted(set(integrity.missing_pages + list(integrity.missing_links_by_page.keys())))
            verify_round += 1
            task_progress["verify_round"] = verify_round
            print(f"[{now_iso()}] [{worker_id}] verify round {verify_round}/{verify_max_rounds} retry_pages={pages_to_retry[:20]}{'...' if len(pages_to_retry) > 20 else ''}", flush=True)

            for page_num in pages_to_retry:
                cmd = _build_cmd(
                    python_bin=python_bin,
                    worker_id=worker_id,
                    task=task,
                    range_start=task.start_page,
                    range_end=task.end_page,
                    output_base_start=task.start_page,
                    output_base_end=task.end_page,
                    listing_url=task.listing_url,
                    delay=delay,
                    viewport_width=viewport_width,
                    viewport_height=viewport_height,
                    profile_dir=profile_dir,
                    state_dir=state_dir,
                    output_dir=output_dir,
                    cf_manual_wait=cf_manual_wait,
                    captcha_manual_wait=captcha_manual_wait,
                    captcha_retries=captcha_retries,
                    navigation_retries=navigation_retries,
                    proxy=proxy,
                    headless=headless,
                    single_page=page_num,
                )
                rc = _run_crawler(
                    cmd=cmd,
                    task=task,
                    task_index=i,
                    page_min=task.start_page,
                    page_max=task.end_page,
                    state=state,
                    state_file=state_file,
                    task_progress=task_progress,
                )
                print(f"[{now_iso()}] [{worker_id}] verify page={page_num} rc={rc}", flush=True)
                if rc != 0:
                    return rc

            integrity = _evaluate_integrity(task, task_progress)
            task_progress["last_summary"] = {
                "checked_at": now_iso(),
                "manifest_total": integrity.manifest_total,
                "success_total": integrity.success_total,
                "missing_pages": integrity.missing_pages,
                "missing_links_pages": sorted(list(integrity.missing_links_by_page.keys())),
            }
            _save_state(state_file, state)

        print(f"[{now_iso()}] [{worker_id}] task integrity OK: {_integrity_text(task, integrity)}", flush=True)
        if integrity.success_total < task.docs_target:
            print(f"[{now_iso()}] [{worker_id}] warning: success({integrity.success_total}) < target({task.docs_target}) after strict verification.", flush=True)

        state["task_index"] = i + 1
        if i + 1 < len(tasks):
            state["next_page"] = tasks[i + 1].start_page
            state["completed"] = False
        else:
            state["next_page"] = task.end_page + 1
            state["completed"] = True
        _save_state(state_file, state)

    return 0


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run custom TVPL batch with up to 8 parallel Camoufox workers")
    parser.add_argument("--worker-runner", type=str, default="", choices=["", *WORKERS])
    parser.add_argument("--python-bin", type=str, default=sys.executable)
    parser.add_argument("--delay", type=float, default=15.0)
    parser.add_argument("--viewport-width", type=int, default=1600)
    parser.add_argument("--viewport-height", type=int, default=900)
    parser.add_argument("--cf-manual-wait", type=int, default=30)
    parser.add_argument("--captcha-manual-wait", type=int, default=30)
    parser.add_argument("--captcha-retries", type=int, default=8)
    parser.add_argument("--navigation-retries", type=int, default=4)
    parser.add_argument("--verify-max-rounds", type=int, default=4)
    parser.add_argument("--headless", action="store_true")
    parser.add_argument("--state-dir", type=str, default=str(DEFAULT_STATE_DIR))
    parser.add_argument("--resume-state-dir", type=str, default=str(DEFAULT_RESUME_STATE_DIR))
    parser.add_argument("--output-dir", type=str, default=str(DEFAULT_OUTPUT_DIR))
    parser.add_argument("--profiles-root", type=str, default=str(DEFAULT_PROFILES_ROOT))
    parser.add_argument("--log-dir", type=str, default=str(DEFAULT_LOG_DIR))
    parser.add_argument("--fresh-profiles", action="store_true")
    parser.add_argument("--reset-resume", action="store_true")
    for wid in WORKERS:
        parser.add_argument(f"--plan-{wid}", type=str, default="")
    for wid in WORKERS:
        parser.add_argument(f"--proxy-{wid}", type=str, default="")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    worker_plans = resolve_worker_plans(args)
    state_dir = Path(args.state_dir)
    resume_state_dir = Path(args.resume_state_dir)
    output_dir = Path(args.output_dir)
    profiles_root = Path(args.profiles_root)
    log_dir = Path(args.log_dir)
    log_dir.mkdir(parents=True, exist_ok=True)

    if args.worker_runner:
        proxy_map = {wid: str(getattr(args, f"proxy_{wid}", "") or "") for wid in WORKERS}
        return run_worker_plan(
            worker_id=args.worker_runner,
            tasks=worker_plans.get(args.worker_runner, []),
            python_bin=args.python_bin,
            delay=args.delay,
            viewport_width=args.viewport_width,
            viewport_height=args.viewport_height,
            cf_manual_wait=args.cf_manual_wait,
            captcha_manual_wait=args.captcha_manual_wait,
            captcha_retries=args.captcha_retries,
            navigation_retries=args.navigation_retries,
            verify_max_rounds=args.verify_max_rounds,
            proxy=proxy_map[args.worker_runner],
            state_dir=state_dir,
            resume_state_dir=resume_state_dir,
            output_dir=output_dir,
            profiles_root=profiles_root,
            headless=bool(args.headless),
            reset_resume=bool(args.reset_resume),
        )

    if args.fresh_profiles:
        profiles_root = profiles_root / f"custom_batch_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    profiles_root.mkdir(parents=True, exist_ok=True)
    print(f"Profiles root: {profiles_root}", flush=True)
    print(f"Logs dir: {log_dir}", flush=True)
    for wid in WORKERS:
        tasks = worker_plans.get(wid, [])
        desc = "; ".join(f"org={t.org_id} pages={t.start_page}-{t.end_page} docs~{t.docs_target}" for t in tasks) if tasks else "(no tasks)"
        print(f"[plan {wid}] {desc}", flush=True)

    common_cmd = [
        args.python_bin, str(Path(__file__).resolve()),
        "--python-bin", args.python_bin,
        "--delay", str(args.delay),
        "--viewport-width", str(args.viewport_width),
        "--viewport-height", str(args.viewport_height),
        "--cf-manual-wait", str(args.cf_manual_wait),
        "--captcha-manual-wait", str(args.captcha_manual_wait),
        "--captcha-retries", str(args.captcha_retries),
        "--navigation-retries", str(args.navigation_retries),
        "--verify-max-rounds", str(args.verify_max_rounds),
        "--state-dir", str(state_dir),
        "--resume-state-dir", str(resume_state_dir),
        "--output-dir", str(output_dir),
        "--profiles-root", str(profiles_root),
        "--log-dir", str(log_dir),
    ]
    for wid in WORKERS:
        common_cmd.extend([f"--plan-{wid}", str(getattr(args, f"plan_{wid}", "") or "")])
    for wid in WORKERS:
        common_cmd.extend([f"--proxy-{wid}", str(getattr(args, f"proxy_{wid}", "") or "")])
    if args.headless:
        common_cmd.append("--headless")
    if args.reset_resume:
        common_cmd.append("--reset-resume")

    procs: list[tuple[str, subprocess.Popen, Any]] = []
    for wid in WORKERS:
        if not worker_plans.get(wid):
            print(f"[{wid}] skip (no tasks configured).", flush=True)
            continue
        worker_log = log_dir / f"{wid}.log"
        f = worker_log.open("a", encoding="utf-8")
        f.write(f"[{now_iso()}] START {wid}\n")
        p = subprocess.Popen(common_cmd + ["--worker-runner", wid], cwd=str(SCRIPT_DIR), stdout=f, stderr=subprocess.STDOUT, text=True)
        procs.append((wid, p, f))
        print(f"[{wid}] started pid={p.pid} -> {worker_log}", flush=True)

    if not procs:
        print("No worker launched (all plans empty).", flush=True)
        return 0

    rc = 0
    for wid, p, f in procs:
        code = p.wait()
        f.write(f"[{now_iso()}] EXIT rc={code}\n")
        f.close()
        print(f"[{wid}] exited rc={code}", flush=True)
        if code != 0 and rc == 0:
            rc = code

    active_workers = [wid for wid, _, _ in procs]
    for wid in active_workers:
        links, missing_pages = _collect_worker_missing_links(
            worker_id=wid,
            tasks=worker_plans.get(wid, []),
            resume_state_dir=resume_state_dir,
        )
        miss_file, miss_count = _write_worker_missing_links(output_dir, wid, links)
        print(
            f"[missing-links] {wid}: links={miss_count} missing_pages={len(missing_pages)} -> {miss_file}",
            flush=True,
        )

    merged_missing_file, merged_missing_count = _merge_missing_link_files(output_dir, active_workers)
    print(f"[missing-links] merged={merged_missing_count} -> {merged_missing_file}", flush=True)

    merged_pdf_file, merged_pdf_count = _merge_pdf_link_files(output_dir, active_workers)
    print(f"[pdf-links] merged={merged_pdf_count} -> {merged_pdf_file}", flush=True)
    return rc


if __name__ == "__main__":
    raise SystemExit(main())
