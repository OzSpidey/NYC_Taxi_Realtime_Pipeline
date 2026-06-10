# NYC Taxi Real-Time Streaming Pipeline

![CI](https://github.com/OzSpidey/azure-realtime-pipeline/actions/workflows/ci.yml/badge.svg)
![Python](https://img.shields.io/badge/Python-3.11-blue?logo=python)
![Azure](https://img.shields.io/badge/Azure-Event%20Hubs%20%7C%20Stream%20Analytics%20%7C%20Functions-0078D4?logo=microsoft-azure)
![dbt](https://img.shields.io/badge/dbt-Synapse-FF694B?logo=dbt)
![Terraform](https://img.shields.io/badge/IaC-Terraform-7B42BC?logo=terraform)
![License](https://img.shields.io/badge/license-MIT-green)

> A production-grade, end-to-end **real-time data engineering pipeline** on Azure, from raw event ingestion to a Power BI–ready Gold table, built to demonstrate the skills that get data engineers hired.

---

## What This Project Does

This pipeline simulates a live NYC taxi fleet streaming thousands of ride events per minute into Azure. It processes those events in real-time using **three different window strategies**, alerts on surge pricing, stores everything in a medallion data lake, and serves a clean Gold table to a BI dashboard, all provisioned with a single Terraform command and deployed automatically via GitHub Actions.

```
Event Producer (Python)
        │  ~10 events / sec
        ▼
┌─────────────────────────────────────────────────────────┐
│              Azure Event Hubs (4 partitions)            │
│        consumer group A          consumer group B       │
└──────────────┬──────────────────────────────┬───────────┘
               │                              │
               ▼                              ▼
  Azure Stream Analytics            Azure Function App
  ┌─────────────────────┐          (surge-alerts trigger)
  │  1. Bronze dump      │──► ADLS Gen2 /bronze/  (raw Parquet)
  │  2. Tumbling 5 min   │──► ADLS Gen2 /silver/  (revenue windows)
  │  3. Sliding  2 min   │──► Surge alerts → SQL MERGE upsert
  │  4. Hopping  10/1min │──► ADLS Gen2 /silver/  (driver leaderboard)
  └─────────────────────┘
               │
               ▼
        dbt (Silver → Gold)
        ├── silver_window_aggregates  (typed view)
        └── gold_borough_revenue      (hourly table + surge premium KPI)
               │
               ▼
        Power BI Dashboard
```

---

## Tech Stack

| Layer | Technology | Purpose |
|---|---|---|
| **Ingestion** | Azure Event Hubs | Distributed message bus, 4 partitions |
| **Stream Processing** | Azure Stream Analytics | Tumbling / Sliding / Hopping window queries |
| **Event-driven Compute** | Azure Functions (Python) | Idempotent surge alert upserts |
| **Data Lake** | ADLS Gen2 | Bronze + Silver medallion storage |
| **Transformation** | dbt (Synapse adapter) | Silver → Gold, schema tests |
| **Serving** | Azure Synapse / SQL | Gold table queried by Power BI |
| **Infrastructure** | Terraform | Full IaC, remote state in Azure Blob |
| **CI/CD** | GitHub Actions | Lint → Test → dbt compile → TF plan → Deploy |
| **Language** | Python 3.11 | Event producer, Function App, tests |
| **Testing** | pytest | 15 unit tests, zero Azure credentials needed |
| **Linting** | ruff | Fast Python linter |

---

## Project Structure

```
azure-realtime-pipeline/
│
├── src/
│   └── event_producer.py          # Async event publisher → Event Hubs
│
├── stream_analytics/
│   └── queries.sql                # 4 concurrent window queries
│
├── function_app/
│   └── function_app.py            # Azure Function, surge alert MERGE upsert
│
├── dbt/
│   ├── dbt_project.yml
│   ├── profiles.yml
│   └── models/
│       ├── silver/
│       │   └── silver_window_aggregates.sql
│       └── gold/
│           ├── gold_borough_revenue.sql
│           └── schema.yml          # dbt data quality tests
│
├── terraform/
│   └── main.tf                    # All Azure resources as code
│
├── tests/
│   └── test_pipeline.py           # 15 unit tests, no Azure account needed
│
├── .github/workflows/
│   └── ci.yml                     # Full CI/CD pipeline
│
├── requirements.txt
└── README.md
```

---

## Key Concepts Demonstrated

**Three window types, and why each one matters:**

| Window | Query | Business Use Case |
|---|---|---|
| **Tumbling** (5 min) | Revenue dashboard | Non-overlapping buckets, each ride counted once; clean for billing |
| **Sliding** (2 min) | Surge alerts | Fires on every new event, looks back 2 min, instant reaction |
| **Hopping** (10 min / 1 min hop) | Driver leaderboard | Overlapping windows, smooth rolling rankings updated every minute |

**Idempotent writes**, the Azure Function uses a SQL `MERGE` statement so re-delivered events (Azure guarantees at-least-once) never create duplicate rows.

**Medallion architecture**, raw events land in Bronze, Stream Analytics aggregates land in Silver, dbt promotes to Gold. Power BI only ever touches Gold.

**Zero-credential local testing**, all 15 unit tests mock the Azure SDK at import time, so anyone can clone and run `pytest` without an Azure account.

---

## How to Run It

### Prerequisites

- Python 3.11+
- An Azure account (free tier works)
- Azure CLI, [install guide](https://learn.microsoft.com/en-us/cli/azure/install-azure-cli)
- Terraform >= 1.5, [install guide](https://developer.hashicorp.com/terraform/install)
- dbt-synapse, installed via `requirements.txt`

---

### Step 1, Clone the repo

```bash
git clone https://github.com/OzSpidey/azure-realtime-pipeline.git
cd azure-realtime-pipeline
```

---

### Step 2, Set up Python environment

```bash
python -m venv .venv

# Windows
.venv\Scripts\activate

# Mac / Linux
source .venv/bin/activate

pip install -r requirements.txt
```

---

### Step 3, Run the unit tests (no Azure needed)

```bash
pytest tests/ -v
```

All 15 tests should pass immediately, no credentials, no network.

---

### Step 4, Provision Azure infrastructure

```bash
az login

cd terraform
terraform init
terraform apply -var="env=dev"
```

This creates: Resource Group, Event Hubs namespace + hub + 2 consumer groups, ADLS Gen2 with Bronze and Silver containers, Stream Analytics job (3 streaming units), and a Linux Function App on a Consumption plan.

---

### Step 5, Configure environment variables

Create a `.env` file in the project root:

```env
EVENT_HUB_NAMESPACE=<your-namespace>.servicebus.windows.net
EVENT_HUB_NAME=taxi-events
EVENTS_PER_SECOND=10
```

---

### Step 6, Deploy the Stream Analytics queries

In the Azure Portal:
1. Navigate to your Stream Analytics job (`asa-nyctaxi-dev`)
2. Open **Query** and paste the contents of `stream_analytics/queries.sql`
3. Configure the inputs and outputs to match the Terraform-created resources
4. Click **Start job**

---

### Step 7, Deploy the Azure Function

```bash
# From project root, uses the GitHub Actions workflow on push to main,
# or deploy manually with the Azure Functions Core Tools:
func azure functionapp publish func-surge-dev --python
```

---

### Step 8, Start the event producer

```bash
python src/event_producer.py
```

Events start flowing at ~10/sec. Watch ADLS Gen2 Bronze container fill up with Parquet files partitioned by year/month/day.

---

### Step 9, Run dbt transformations

```bash
cd dbt

# Set your Synapse connection env vars
export DBT_SYNAPSE_SERVER=<your-workspace>.sql.azuresynapse.net
export DBT_SYNAPSE_DATABASE=<your-db>

dbt run
dbt test
```

This creates `silver_window_aggregates` (view) and `gold_borough_revenue` (table with `surge_premium_usd` KPI).

---

### CI/CD (automatic after setup)

Push any branch → GitHub Actions runs:
1. `ruff` lint check
2. `pytest` unit tests
3. `dbt compile` (syntax validation without a live DB)
4. `terraform plan` (on PRs only)
5. Auto-deploy Function App (on merge to `main`)

Add these secrets to your GitHub repo settings to enable deploy:
`ARM_CLIENT_ID`, `ARM_CLIENT_SECRET`, `ARM_SUBSCRIPTION_ID`, `ARM_TENANT_ID`, `AZURE_CREDENTIALS`, `FUNCTION_APP_NAME`

---

## Dataset

Synthetic data modelled on the [NYC TLC Trip Record Dataset](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page). No real PII is used. Schema matches real taxi data fields so the project is drop-in compatible with the actual dataset.

---

## Skills Demonstrated

`Azure Event Hubs` · `Stream Analytics` · `ADLS Gen2` · `Azure Functions` · `dbt` · `Synapse` · `Terraform` · `GitHub Actions` · `Medallion Architecture` · `Windowed Aggregations` · `Idempotent Writes` · `Python async` · `pytest` · `DefaultAzureCredential`
