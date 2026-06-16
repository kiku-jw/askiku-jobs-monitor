from __future__ import annotations

import argparse
from pathlib import Path

from .jobs import DEFAULT_LIMIT, DEFAULT_MAX_AGE_HOURS, drain_jobs_alerts, jobs_status_panel
from .storage import Storage


def main() -> int:
    parser = argparse.ArgumentParser(description="Run the portable Askiku jobs monitor.")
    parser.add_argument(
        "--db",
        default="askiku_jobs.sqlite3",
        help="SQLite state path. Default: askiku_jobs.sqlite3",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    run_parser = subparsers.add_parser("run", help="Scan sources and print a digest.")
    run_parser.add_argument("--mode", default="heavy", choices=("light", "heavy", "new", "backfill"))
    run_parser.add_argument("--limit", type=int, default=DEFAULT_LIMIT)
    run_parser.add_argument("--max-age-hours", type=int, default=DEFAULT_MAX_AGE_HOURS)
    run_parser.add_argument("--dry-run", action="store_true")

    subparsers.add_parser("status", help="Print the last monitor status.")

    args = parser.parse_args()
    storage = Storage(Path(args.db).expanduser())
    storage.initialize()

    if args.command == "status":
        print(jobs_status_panel(storage))
        return 0

    result = drain_jobs_alerts(
        storage=storage,
        limit=args.limit,
        dry_run=args.dry_run,
        mode=args.mode,
        max_age_hours=args.max_age_hours,
    )
    message = result.get("message") or "[SILENT]"
    print(message)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
