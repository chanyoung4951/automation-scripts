# Strava Runner Analytics (Python + DuckDB + Parquet + Streamlit)

Minimal local pipeline for runner-focused Strava analytics:

1. Daily sync activities from Strava API (incremental).
2. Store normalized data in partitioned Parquet files.
3. Query historical data using DuckDB.
4. Explore metrics in a Streamlit dashboard.

## Setup

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r strava_runner/requirements.txt
cp strava_runner/.env.example .env
```

Populate `.env`:

- `STRAVA_CLIENT_ID`
- `STRAVA_CLIENT_SECRET`
- `STRAVA_REFRESH_TOKEN`
- `STRAVA_ACCESS_TOKEN` (optional, will be refreshed)

## Daily sync

```bash
python strava_runner/sync_strava.py
```

This will:

- refresh OAuth token
- fetch activities since last sync timestamp
- write normalized rows to Parquet partitioned by `year/month`
- update `data/state/strava_sync_state.json`

## Dashboard

```bash
streamlit run strava_runner/app.py
```

## Data layout

```text
strava_runner/data/
  parquet/activities/year=YYYY/month=MM/part-*.parquet
  state/strava_sync_state.json
```
