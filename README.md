# Askiku Jobs Monitor

Portable job-search monitor extracted from Askiku.

It searches Ukrainian job boards for remote IT roles, keeps strict reservation evidence, scores fit, and produces a compact digest that another runtime can send through Telegram, email, a web UI, or any other operator surface.

It also keeps a review queue for companies whose vacancies look relevant but lack direct reservation evidence. Verified employer evidence can be recorded back into the same SQLite state and used by later scans.

## Current Gate Policy

Shown candidates must have:

- direct or adjacent reservation evidence
- remote work signal
- no defense, miltech, battlefield, drone, clearance, or war-adjacent signal
- enough fit score to be useful

Higher-education requirements are not a hide gate. They are reported as an education risk and can lower score.

Direct reservation plus remote can soft-pass below the normal score threshold when the score is still at least 45/100. The digest marks those as low fit.

If a posting explicitly mentions remote work but also mentions office or hybrid wording, it is treated as remote with a risk label instead of being hidden.

Diia.City residency is context only. It must not make a vacancy alertable by itself.

## Sources

- DOU RSS
- Work.ua
- Robota.ua
- Djinni RSS
- Happy Monday
- Jobs.ua
- Diia.City registry context

## Quick Start

```bash
python3 -m venv .venv
. .venv/bin/activate
pip install -e .
askiku-jobs run --mode heavy --dry-run --limit 3
askiku-jobs status
```

Record checked company evidence:

```bash
askiku-jobs verify-company \
  --company "Example Tech" \
  --evidence-type official_career_claim \
  --source-url "https://example.com/careers" \
  --quote "We provide employee reservation for eligible roles."
```

Programmatic use:

```python
from pathlib import Path

from askiku_jobs import Storage, drain_jobs_alerts, record_company_verification_evidence

storage = Storage(Path("askiku_jobs.sqlite3"))
storage.initialize()

record_company_verification_evidence(
    storage=storage,
    company="Example Tech",
    evidence_type="official_career_claim",
    source_url="https://example.com/careers",
    quote="We provide employee reservation for eligible roles.",
)

result = drain_jobs_alerts(storage=storage, mode="heavy", dry_run=True, limit=3)
print(result["message"] or "[SILENT]")
```

## Runtime Notes

- `dry_run=True` never marks candidates as sent.
- `light` skips company discovery.
- `heavy` spends a bounded company-discovery budget to look for neighboring vacancies with reservation evidence and can promote up to 3 adjacent matches per run.
- `backfill` disables freshness gates and is intended for manual research, not cron alerts.
- `verify-company` supports `official_career_claim`, `official_criticality_decision`, `employer_verified_manual`, and `sector_hint`.
- HTTPS fetching uses `certifi` so local Python CA-store issues are less likely to make every source look empty.

## Tests

```bash
python3 -m unittest discover -s tests -v
python3 -m py_compile src/askiku_jobs/jobs.py src/askiku_jobs/storage.py src/askiku_jobs/cli.py
```
