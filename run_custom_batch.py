from __future__ import annotations

import argparse
import json
import subprocess
import sys
from pathlib import Path
from urllib.parse import parse_qs, urlparse


SCRIPT_DIR = Path(__file__).resolve().parent
DEFAULT_CONFIG_PATH = SCRIPT_DIR / "config" / "custom_batch.json"
PARALLEL_RUNNER = SCRIPT_DIR / "09_parallel_camoufox_custom_batch.py"
WORKERS = ("w1", "w2", "w3")


DEFAULT_SETTINGS: dict[str, object] = {
    "delay": 11,
    "viewport_width": 1600,
    "viewport_height": 900,
    "cf_manual_wait": 30,
    "captcha_manual_wait": 30,
    "captcha_retries": 8,
    "navigation_retries": 4,
    "verify_max_rounds": 4,
    "output_dir": "output",
    "state_dir": "state",
    "resume_state_dir": "state/custom_batch_resume",
    "profiles_root": "state/camoufox_profiles",
    "log_dir": "logs/camoufox_custom_batch",
    "headless": False,
    "fresh_profiles": True,
    "reset_resume": False,
}


def load_json(path: Path) -> dict:
    if not path.exists():
        raise FileNotFoundError(f"Config not found: {path}")
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as e:
        raise ValueError(f"Invalid JSON at {path}: {e}") from e


def to_int(value: object, key: str) -> int:
    try:
        return int(value)
    except Exception as e:
        raise ValueError(f"Setting `{key}` must be integer, got: {value!r}") from e


def to_float(value: object, key: str) -> float:
    try:
        return float(value)
    except Exception as e:
        raise ValueError(f"Setting `{key}` must be number, got: {value!r}") from e


def _validate_task_url(url: str, worker: str, idx: int) -> None:
    parsed = urlparse(url)
    if not parsed.scheme or not parsed.netloc:
        raise ValueError(f"{worker}.tasks[{idx}].url is not a valid URL: {url}")
    qs = parse_qs(parsed.query or "", keep_blank_values=True)
    org_vals = qs.get("org") or qs.get("Org")
    if not org_vals:
        raise ValueError(f"{worker}.tasks[{idx}].url must include `org=` param: {url}")


def build_plan(worker: str, tasks: list[dict]) -> str:
    parts: list[str] = []
    for idx, task in enumerate(tasks):
        if not isinstance(task, dict):
            raise ValueError(f"{worker}.tasks[{idx}] must be object")
        docs = to_int(task.get("docs"), f"{worker}.tasks[{idx}].docs")
        if docs <= 0:
            raise ValueError(f"{worker}.tasks[{idx}].docs must be > 0")
        url = str(task.get("url", "")).strip()
        if not url:
            raise ValueError(f"{worker}.tasks[{idx}].url is empty")
        _validate_task_url(url, worker, idx)
        parts.append(f"{docs},{url}")
    return ";".join(parts)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run TVPL custom batch crawl (3 workers) from config JSON"
    )
    parser.add_argument(
        "--config",
        type=str,
        default=str(DEFAULT_CONFIG_PATH),
        help="Path to config JSON",
    )
    parser.add_argument(
        "--python-bin",
        type=str,
        default=sys.executable,
        help="Python executable to run crawler scripts",
    )
    parser.add_argument("--dry-run", action="store_true", help="Print command only, do not run")
    parser.add_argument("--headless", action="store_true", help="Force headless mode")
    parser.add_argument("--reset-resume", action="store_true", help="Force reset resume state")
    parser.add_argument("--fresh-profiles", action="store_true", help="Force fresh profile root")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    config_path = Path(args.config).resolve()
    cfg = load_json(config_path)
    if not isinstance(cfg, dict):
        raise ValueError("Config root must be object")

    settings = dict(DEFAULT_SETTINGS)
    user_settings = cfg.get("settings", {})
    if user_settings is not None:
        if not isinstance(user_settings, dict):
            raise ValueError("`settings` must be object")
        settings.update(user_settings)

    workers = cfg.get("workers", {})
    if not isinstance(workers, dict):
        raise ValueError("`workers` must be object")

    plan_map: dict[str, str] = {}
    proxy_map: dict[str, str] = {}
    total_tasks = 0
    for wid in WORKERS:
        wcfg = workers.get(wid, {})
        if wcfg is None:
            wcfg = {}
        if not isinstance(wcfg, dict):
            raise ValueError(f"`workers.{wid}` must be object")
        tasks = wcfg.get("tasks", [])
        if tasks is None:
            tasks = []
        if not isinstance(tasks, list):
            raise ValueError(f"`workers.{wid}.tasks` must be array")
        plan = build_plan(wid, tasks)
        plan_map[wid] = plan
        proxy_map[wid] = str(wcfg.get("proxy", "") or "").strip()
        total_tasks += len(tasks)

    if total_tasks == 0:
        raise ValueError("No tasks configured. Add at least one task in workers.w1/w2/w3")

    # resolve dirs relative to project root when not absolute
    def _resolve_dir(value: object, key: str) -> str:
        raw = str(value or "").strip()
        if not raw:
            raise ValueError(f"Setting `{key}` cannot be empty")
        p = Path(raw)
        if not p.is_absolute():
            p = (SCRIPT_DIR / p).resolve()
        p.mkdir(parents=True, exist_ok=True)
        return str(p)

    output_dir = _resolve_dir(settings.get("output_dir"), "output_dir")
    state_dir = _resolve_dir(settings.get("state_dir"), "state_dir")
    resume_state_dir = _resolve_dir(settings.get("resume_state_dir"), "resume_state_dir")
    profiles_root = _resolve_dir(settings.get("profiles_root"), "profiles_root")
    log_dir = _resolve_dir(settings.get("log_dir"), "log_dir")

    cmd = [
        args.python_bin,
        str(PARALLEL_RUNNER),
        "--python-bin",
        args.python_bin,
        "--delay",
        str(to_float(settings.get("delay"), "delay")),
        "--viewport-width",
        str(to_int(settings.get("viewport_width"), "viewport_width")),
        "--viewport-height",
        str(to_int(settings.get("viewport_height"), "viewport_height")),
        "--cf-manual-wait",
        str(to_int(settings.get("cf_manual_wait"), "cf_manual_wait")),
        "--captcha-manual-wait",
        str(to_int(settings.get("captcha_manual_wait"), "captcha_manual_wait")),
        "--captcha-retries",
        str(to_int(settings.get("captcha_retries"), "captcha_retries")),
        "--navigation-retries",
        str(to_int(settings.get("navigation_retries"), "navigation_retries")),
        "--verify-max-rounds",
        str(to_int(settings.get("verify_max_rounds"), "verify_max_rounds")),
        "--output-dir",
        output_dir,
        "--state-dir",
        state_dir,
        "--resume-state-dir",
        resume_state_dir,
        "--profiles-root",
        profiles_root,
        "--log-dir",
        log_dir,
        "--plan-w1",
        plan_map["w1"],
        "--plan-w2",
        plan_map["w2"],
        "--plan-w3",
        plan_map["w3"],
        "--proxy-w1",
        proxy_map["w1"],
        "--proxy-w2",
        proxy_map["w2"],
        "--proxy-w3",
        proxy_map["w3"],
    ]

    if bool(settings.get("headless")) or bool(args.headless):
        cmd.append("--headless")
    if bool(settings.get("fresh_profiles")) or bool(args.fresh_profiles):
        cmd.append("--fresh-profiles")
    if bool(settings.get("reset_resume")) or bool(args.reset_resume):
        cmd.append("--reset-resume")

    print(f"Config: {config_path}")
    print(f"Total tasks: {total_tasks}")
    for wid in WORKERS:
        print(f"  {wid}: tasks={len(workers.get(wid, {}).get('tasks', []) or [])} proxy={'on' if proxy_map[wid] else 'off'}")
    print("Command:")
    print(" ".join(f'"{x}"' if " " in x else x for x in cmd))

    if args.dry_run:
        return 0

    return subprocess.call(cmd, cwd=str(SCRIPT_DIR))


if __name__ == "__main__":
    raise SystemExit(main())

