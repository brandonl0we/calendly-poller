# calendly-poller

Small Python automation project that keeps Airtable rep capacity data updated from Calendly.

## What this repo does

- `scripts/calendly_sync.py`: pulls Calendly availability and updates Airtable capacity fields.
- `scripts/weekly_reset.py`: resets each rep's "Booked This Week" to 0 every Monday.

## Quick start (local run)

1. Create and activate a virtual environment:
   python3 -m venv .venv
   source .venv/bin/activate

2. Install dependencies:
   pip install -r scripts/requirements.txt

3. Create local env file:
   cp .env.example .env

4. Put your real tokens in `.env`:
   CALENDLY_API_TOKEN=...
   AIRTABLE_API_TOKEN=...

5. Load env vars and run:
   set -a
   source .env
   set +a
   python scripts/calendly_sync.py
