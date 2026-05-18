# NYC Taxi Real-Time Streaming Pipeline

A production-grade Azure data engineering pipeline that ingests synthetic NYC taxi ride events, processes them in real-time with four concurrent Stream Analytics window queries, enriches them in an Azure Function, and serves a Power BI–ready Gold table through dbt.

---

## Architecture

```
Event Producer (Python)
        │  ~10 events/sec
        ▼
Azure Event Hubs  ──────────────────────────────────┐
(4 partitions)                                      │
        │                                           │
        │  consumer group: stream-analytics         │  consumer group: surge-alerts
        ▼                                           ▼
Azure Stream Analytics                      Azure Function App
  ┌──────────────────────────┐                (surge_alerts trigger)
  │  1. Bronze pass-through  │──► ADLS Gen2 /bronze/  (raw Parquet)
  │  2. Tumbling 5-min       │──► ADLS Gen2 /silver/  (revenue windows)
  │  3. Sliding  2-min       │──► Event Hub  (surge alert events)
  │  4. Hopping  10m/1m hop  │──► ADLS Gen2 /silver/  (driver leaderboard)
  └──────────────────────────┘         │
                                       ▼
                               Azure SQL / Synapse
                               dbo.surge_alerts (MERGE upsert)
        │
        ▼
      dbt (Silver → Gold)
        ├── silver_window_aggregates  (view — typed Silver data)
        └── gold_borough_revenue      (table — hourly revenue + surge premium)
                │
                ▼
          Power BI Dashboard
```

---

## Project Structure

```
├── src/
│   └── event_producer.py        # Publishes synthetic taxi events to Event Hubs
├── stream_analytics/
│   └── queries.sql              # 4 concurrent ASA window queries
├── function_app/
│   └── function_app.py          # Azure Function — surge alert upsert
├── dbt/
│   ├── dbt_project.yml
│   ├── profiles.yml
│   └── models/
│       ├── silver/              # silver_window_aggregates.sql
│       └── gold/                # gold_borough_revenue.sql
├── terraform/
│   └── main.tf                  # Full Azure infrastructure as code
├── tests/
│   └── test_pipeline.py         # 13 unit tests (no Azure credentials needed)
└── .github/workflows/
    └── ci.yml                   # CI: lint → test → dbt compile → tf plan → deploy
```

---

## What Each Component Does

### `src/event_producer.py`
Simulates a real NYC taxi fleet. Generates ~10 ride events per second with realistic fields: borough, fare, surge multiplier, driver ID, distance. Uses `DefaultAzureCredential` — no passwords in code. Run locally or containerise for demo purposes.

### `stream_analytics/queries.sql`
Four concurrent SQL queries running inside a single Stream Analytics job:

| Query | Window Type | Purpose |
|-------|-------------|---------|
| Bronze pass-through | None (passthrough) | Archive every event to ADLS Gen2 |
| Revenue dashboard | Tumbling 5 min | Non-overlapping revenue buckets per borough |
| Surge alert | Sliding 2 min | Fires when `avg_surge > 1.8` in any borough |
| Driver leaderboard | Hopping 10 min / 1 min hop | Rolling top-driver ranking updated each minute |

**Why three window types?** Each solves a different business problem:
- **Tumbling** — each ride counted once; clean billing/revenue aggregation
- **Sliding** — reacts to every new event; perfect for real-time alerts
- **Hopping** — overlapping windows give smooth leaderboard updates

### `function_app/function_app.py`
Azure Function triggered by the `surge-alerts` consumer group. Uses a `MERGE` statement (upsert) so re-deliveries are idempotent — safe for at-least-once delivery semantics.

### `dbt/models/`
- **Silver** (`silver_window_aggregates.sql`) — typed view over raw Stream Analytics output; no business logic
- **Gold** (`gold_borough_revenue.sql`) — hourly aggregation + `surge_premium_usd` (extra revenue from surge), consumed directly by Power BI

### `terraform/main.tf`
Provisions all Azure resources with one `terraform apply`:
- Resource Group, Event Hubs Namespace + Hub + 2 consumer groups
- ADLS Gen2 with `bronze` and `silver` containers
- Stream Analytics Job (3 streaming units)
- Linux Function App (Consumption plan)

### `.github/workflows/ci.yml`
Four-stage pipeline:
1. **Lint + Tests** — `ruff` + `pytest` on every push
2. **dbt compile** — validates SQL models without a live database
3. **Terraform plan** — shows infrastructure diff on every PR
4. **Deploy Function** — auto-deploys to Azure on merge to `main`

---

## Running Locally

```bash
# 1. Create virtual environment
python -m venv .venv && source .venv/bin/activate   # Windows: .venv\Scripts\activate

# 2. Install dependencies
pip install -r requirements.txt

# 3. Run unit tests (no Azure account needed)
pytest tests/ -v

# 4. Set environment variables (copy from template)
cp .env.example .env
# Edit .env with your Event Hub namespace and name

# 5. Start producer (needs Azure login)
az login
python src/event_producer.py
```

---

## Skills Demonstrated

| Category | Technology |
|----------|-----------|
| Streaming | Azure Event Hubs, Stream Analytics (3 window types) |
| Storage | ADLS Gen2 (medallion: Bronze/Silver/Gold) |
| Compute | Azure Functions (event-driven, idempotent) |
| Transformation | dbt (Synapse adapter, view + table materialisation) |
| Infrastructure | Terraform (full IaC, remote state) |
| CI/CD | GitHub Actions (lint → test → compile → plan → deploy) |
| Data Quality | dbt schema tests (not_null), idempotent MERGE |
| Auth | DefaultAzureCredential, Managed Identity |

---

## Dataset

Synthetic data modelled on the [NYC TLC Trip Record Dataset](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page). No real PII is used.
