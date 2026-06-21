from __future__ import annotations

import argparse
import json
from pathlib import Path

from .jobs import (
    COMPANY_VERIFICATION_EVIDENCE_TYPES,
    DEFAULT_LIMIT,
    DEFAULT_MAX_AGE_HOURS,
    drain_jobs_alerts,
    jobs_status_panel,
    record_company_verification_evidence,
)
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

    verify_parser = subparsers.add_parser(
        "verify-company",
        help="Record manual or official company reservation evidence.",
    )
    verify_parser.add_argument("--company", required=True)
    verify_parser.add_argument(
        "--evidence-type",
        required=True,
        choices=sorted(COMPANY_VERIFICATION_EVIDENCE_TYPES),
    )
    verify_parser.add_argument("--source-url", default="")
    verify_parser.add_argument("--quote", required=True)
    verify_parser.add_argument("--reviewer-note", default="")
    verify_parser.add_argument("--source", default="manual")
    verify_parser.add_argument("--company-legal-name", default="")
    verify_parser.add_argument("--company-edrpou", default="")

    args = parser.parse_args()
    storage = Storage(Path(args.db).expanduser())
    storage.initialize()

    if args.command == "status":
        print(jobs_status_panel(storage))
        return 0

    if args.command == "verify-company":
        result = record_company_verification_evidence(
            storage=storage,
            company=args.company,
            evidence_type=args.evidence_type,
            source_url=args.source_url,
            quote=args.quote,
            reviewer_note=args.reviewer_note,
            source=args.source,
            company_legal_name=args.company_legal_name,
            company_edrpou=args.company_edrpou,
        )
        print(json.dumps(result, ensure_ascii=False, sort_keys=True))
        return 0 if result.get("stored") else 2

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
