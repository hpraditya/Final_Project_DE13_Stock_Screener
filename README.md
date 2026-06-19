# Warren Buffett Stock Screener — IDX Data Pipeline

Automated daily pipeline yang men-screen saham IDX (Bursa Efek Indonesia) menggunakan 7 kriteria fundamental Warren Buffett. Data ditarik dari [Sectors Financial API v2](https://api.sectors.app/v2), diproses melalui arsitektur medallion (Bronze → Silver → Gold), dan disimpan di PostgreSQL untuk konsumsi dashboard/laporan.

---

## Architecture

```
Sectors API v2
      │
      ▼
┌─────────────┐    ┌───────────────────┐    ┌──────────────────┐
│   BRONZE    │    │      SILVER       │    │      GOLD        │
│  (Parquet)  │───▶│    (Parquet)      │───▶│   (PostgreSQL)   │
│  SeaweedFS  │    │    SeaweedFS      │    │  fct_buffett_    │
│             │    │  DuckDB transform │    │   screening      │
└─────────────┘    └───────────────────┘    └──────────────────┘
                                                     │
                           Apache Airflow 2.9 (orchestration)
                           Great Expectations  (data quality gates)
```

**Data lake:** SeaweedFS S3-compatible (`s3://lake/bronze/` dan `s3://lake/silver/`)
**Warehouse:** PostgreSQL 15 schema `gold`

---

## Tech Stack

| Layer | Tool |
|-------|------|
| Orchestration | Apache Airflow 2.9.0 (LocalExecutor) |
| Storage | SeaweedFS (S3-compatible filer API) |
| Transform | DuckDB (bronze→silver), pandas (silver→gold) |
| Warehouse | PostgreSQL 15 |
| Data Quality | Great Expectations 0.18.x |
| API Client | Sectors Financial API v2 |
| Infra | Docker Compose |

---

## Buffett Screening Criteria

Semua 7 kriteria harus terpenuhi → status **LOLOS**.

| # | Metrik | Threshold |
|---|--------|-----------|
| 1 | ROE (Return on Equity) | ≥ 15% |
| 2 | DER (Debt-to-Equity) | ≤ 0.5 |
| 3 | FCF (Free Cash Flow) | > 0 |
| 4 | EPS Growth YoY | ≥ 10% |
| 5 | PER (Price-to-Earnings) | ≤ 15× |
| 6 | PBV (Price-to-Book) | ≤ 1.5× |
| 7 | Graham Combined (PER × PBV) | ≤ 22.5 |

---

## Quick Start

### Prerequisites
- Docker & Docker Compose
- API key dari [sectors.app](https://sectors.app)

### 1. Clone & konfigurasi

```bash
git clone <repo-url>
cd Data_Pipeline_Project_DE13
cp .env.example .env   # isi variabel sesuai environment Anda
```

### 2. Jalankan stack

```bash
docker compose up -d
```

Service yang berjalan:
- Airflow Webserver → `http://localhost:8080` (user: `airflow`, pass: `airflow`)
- Airflow Scheduler
- PostgreSQL → port `5432`
- SeaweedFS Master → port `9333`
- SeaweedFS Volume → port `8080` (internal)
- SeaweedFS Filer → S3 API port `8333`, UI port `8888`

### 3. Inisialisasi warehouse

```bash
# Buat tabel gold dan seed dim_date
docker compose exec airflow-webserver python scripts/seed_dim_date.py
```

### 4. Trigger DAG

Buka Airflow UI → enable DAG `warren_buffett_screener` → trigger manual atau tunggu jadwal harian.

---

## Environment Variables

Salin `.env.example` ke `.env` dan isi nilainya. **Jangan commit `.env`.**

```
SECTOR_API_KEY=          # API key dari sectors.app
SEAWEEDFS_ENDPOINT=      # http://seaweedfs-filer:8333
SEAWEEDFS_ACCESS_KEY=    # S3 access key SeaweedFS
SEAWEEDFS_SECRET_KEY=    # S3 secret key SeaweedFS
SEAWEEDFS_BUCKET=        # nama bucket (default: lake)
POSTGRES_HOST=           # hostname PostgreSQL
POSTGRES_PORT=           # port PostgreSQL (default: 5432)
POSTGRES_DB=             # nama database
POSTGRES_USER=           # user PostgreSQL
POSTGRES_PASSWORD=       # password PostgreSQL
AIRFLOW__CORE__FERNET_KEY=
AIRFLOW__WEBSERVER__SECRET_KEY=
```

---

## Project Structure

```
├── airflow/
│   └── dags/
│       ├── warren_buffett_screener.py   # DAG utama
│       └── utils/
│           ├── storage.py               # baca/tulis Parquet ke SeaweedFS
│           └── sector_client.py         # wrapper Airflow untuk extraction
├── extraction/
│   ├── client.py                        # Sectors API v2 client + flattener
│   ├── endpoints.py                     # mapping domain → API section
│   └── schemas/                         # dataclass schema per domain
├── transform/
│   ├── silver/
│   │   ├── clean_financials.py          # bronze → silver financial_statements
│   │   ├── clean_profile.py             # bronze → silver company_profile
│   │   └── clean_valuation.py           # bronze → silver valuation_ratios
│   └── gold/
│       ├── metrics_buffett.py           # silver → gold screening + PostgreSQL load
│       ├── scoring.py                   # 7 kriteria Buffett → status LOLOS/TIDAK
│       └── queries/                     # SQL DuckDB untuk kalkulasi metrik
├── warehouse/
│   ├── ddl/                             # CREATE TABLE scripts (001–007)
│   ├── views/                           # v_latest_screening
│   └── seeds/                           # dim_date 2024–2030
├── quality/
│   ├── great_expectations.yml
│   ├── expectations/
│   │   ├── bronze_raw_suite.json        # gate setelah ingestion
│   │   └── gold_metrics_suite.json      # gate setelah load ke PostgreSQL
│   └── checkpoints/
├── scripts/
│   ├── backfill_fundamentals.py         # backfill 2 tahun data historis
│   └── seed_dim_date.py                 # seed tabel dim_date
├── tests/                               # pytest unit tests
├── seaweedfs/config/filer.toml
├── Dockerfile                           # extends apache/airflow:2.9.0
├── docker-compose.yml
└── requirements.txt
```

---

## DAG Flow

```
extract_company_profile ──┐
extract_financial_stmts ──┼──▶ gate_bronze ──▶ clean_profile ──┐
extract_valuation_ratios ─┘                    clean_financials ─┼──▶ compute_gold_metrics ──▶ gate_gold ──▶ load_warehouse
                                               clean_valuation ──┘
```

| Task | Keterangan |
|------|-----------|
| `extract_*` | Tarik data dari Sectors API v2, tulis ke bronze Parquet di SeaweedFS |
| `gate_bronze` | Validasi raw data (row count, null threshold, ticker format) |
| `clean_*` | DuckDB transform: normalisasi, dedup, konversi tipe, tulis ke silver |
| `compute_gold_metrics` | Kalkulasi ROE/DER/FCF/EPS growth/PER/PBV, scoring, upsert PostgreSQL |
| `gate_gold` | Validasi tabel hasil screening (criteria_passed range, status values) |
| `load_warehouse` | Log summary statistik LOLOS/TIDAK per sektor |

---

## Running Tests

```bash
# Install dependencies lokal
pip install -r requirements.txt

# Jalankan semua tests
pytest

# Output contoh
tests/test_client.py          (12 tests) — API client + field mapping v2
tests/test_silver_transform.py (9 tests) — DuckDB endpoint, dedup, S3 write
tests/test_screening_logic.py (10 tests) — scoring criteria + NaN edge cases
```

---

## Data Model (Gold Layer)

**`gold.fct_buffett_screening`** — tabel utama hasil screening

```
ticker | run_date | period | company_name | sector
roe | der | fcf | fcf_margin | eps_growth_yoy | eps_cagr_5y
per | pbv | graham_combined | criteria_passed | status
```

**`gold.v_latest_screening`** — view terbaru, join dengan `dim_company`, diurutkan berdasarkan `criteria_passed DESC`.

---

## Backfill

Untuk mengisi data historis 2 tahun ke belakang (khusus sektor oil/gas/coal):

```bash
python scripts/backfill_fundamentals.py
```

---

## Notes

- API Sectors v2 hanya menyediakan data **annual** (bukan quarterly). `period` berformat `"2024"`.
- Data coverage: emiten IDX sektor **Energy** (oil, gas, coal) — kurang lebih 30–50 emiten.
- SeaweedFS S3 endpoint untuk DuckDB httpfs **tidak boleh** menyertakan scheme `http://` — pipeline menangani ini otomatis via `_duckdb_endpoint()`.
- DAG berjalan dengan `schedule_interval="@daily"`, `retries=2`, `retry_delay=5m`.
