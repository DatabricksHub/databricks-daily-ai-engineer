"""
main.py — Entry point for the Databricks AI Engineer system.

Usage:
  # Full run (requires Databricks + GitHub credentials)
  python main.py

  # Dry-run: generate project locally, skip Databricks + GitHub
  python main.py --dry-run

  # Run with retries enabled
  python main.py --max-retries 2

  # Schedule: run every 24 hours (daemon mode)
  python main.py --schedule 24
"""

import argparse
import sys
import time

# Force UTF-8 output on Windows so log files don't fail on unicode
if sys.stdout.encoding and sys.stdout.encoding.lower() != 'utf-8':
    sys.stdout.reconfigure(encoding='utf-8', errors='replace')
    sys.stderr.reconfigure(encoding='utf-8', errors='replace')
from pathlib import Path

# ── Load .env file automatically ──────────────────────────────────────────────
# This reads your .env file so you don't need to manually export variables
try:
    from dotenv import load_dotenv
    env_path = Path(__file__).parent / ".env"
    if env_path.exists():
        load_dotenv(env_path)
        print(f"[OK] Loaded environment from {env_path}")
    else:
        print(f"[WARN] No .env file found at {env_path} - using system environment variables")
except ImportError:
    print("[WARN] python-dotenv not installed. Run: pip install python-dotenv")

# Ensure project root is on PYTHONPATH regardless of where you call from
sys.path.insert(0, str(Path(__file__).parent))

from supervisor import Supervisor
from utils.logger import get_logger

logger = get_logger("main", "Main")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="🤖 Databricks Daily AI Engineer — Autonomous demo generator",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Free LLM options (no credit card needed):
  Groq   → sign up at console.groq.com   → set GROQ_API_KEY in .env
  Gemini → sign up at aistudio.google.com → set GEMINI_API_KEY in .env
  Ollama → install at ollama.com          → set LLM_PROVIDER=ollama in .env

.env file example:
  LLM_PROVIDER=groq
  GROQ_API_KEY=gsk_xxxxxxxxxxxx
  GITHUB_TOKEN=ghp_xxxxxxxxxxxx
  GITHUB_REPO=your-username/databricks-daily-ai-engineer
  DATABRICKS_HOST=https://adb-xxx.azuredatabricks.net
  DATABRICKS_TOKEN=dapi_xxxxxxxxxxxx
        """,
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Generate project locally; skip Databricks execution and GitHub publishing.",
    )
    parser.add_argument(
        "--max-retries",
        type=int,
        default=1,
        metavar="N",
        help="Number of retries for retryable agents (default: 1).",
    )
    parser.add_argument(
        "--schedule",
        type=float,
        default=0,
        metavar="HOURS",
        help="Run every N hours in daemon mode (0 = run once and exit).",
    )
    parser.add_argument(
        "--daily",
        action="store_true",
        help="Run once daily at 06:00 AM (daemon mode). Equivalent to --schedule 24 with 6am alignment.",
    )
    return parser.parse_args()


def run_once(dry_run: bool, max_retries: int) -> bool:
    """Execute a single pipeline run. Returns True on success."""
    supervisor = Supervisor(dry_run=dry_run, max_retries=max_retries)
    report = supervisor.run()
    return report.overall_success


def main() -> None:
    args = parse_args()

    # --daily: sleep until 06:00 then run every 24h
    if args.daily:
        import time as _time
        from datetime import datetime as _dt, timedelta as _td
        while True:
            now  = _dt.now()
            target = now.replace(hour=6, minute=0, second=0, microsecond=0)
            if now >= target:
                target += _td(days=1)
            wait = (target - now).total_seconds()
            logger.info(f"Daily mode: next run at {target.strftime('%Y-%m-%d 06:00:00')} (sleeping {wait/3600:.1f}h)")
            try:
                _time.sleep(wait)
            except KeyboardInterrupt:
                logger.info("Shutting down. Goodbye!")
                sys.exit(0)
            try:
                run_once(args.dry_run, args.max_retries)
            except Exception as exc:
                logger.error(f"Pipeline error: {exc}")

    if args.schedule > 0:
        interval_s = args.schedule * 3600
        logger.info(
            f"🕐 Daemon mode: running every {args.schedule}h "
            f"(Ctrl+C to stop)"
        )
        while True:
            try:
                success = run_once(args.dry_run, args.max_retries)
            except KeyboardInterrupt:
                logger.info("Shutting down. Goodbye!")
                sys.exit(0)
            except Exception as exc:
                logger.error(f"Unexpected pipeline error: {exc}")
                success = False

            status = "✅" if success else "❌"
            logger.info(
                f"{status} Run complete. "
                f"Next run in {args.schedule}h "
                f"(sleeping {interval_s:.0f}s) …"
            )
            try:
                time.sleep(interval_s)
            except KeyboardInterrupt:
                logger.info("Shutting down. Goodbye!")
                sys.exit(0)
    else:
        success = run_once(args.dry_run, args.max_retries)
        sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()