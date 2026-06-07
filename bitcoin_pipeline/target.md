# Bitcoin Market Intelligence Pipeline — Project Context

## Mục tiêu project

Xây một **end-to-end Data Engineering pipeline** tự động thu thập, xử lý và phân tích
dữ liệu thị trường Bitcoin. Project phục vụ 2 mục đích song song:

1. **Portfolio DE** — showcase đầy đủ tech stack khi apply fresher DE job

Business question cần trả lời:
- Price trend theo ngày/tuần (MA7, MA30, volatility)
- Fear & Greed Index có tương quan với price movement không?
- Ngày nào volume bất thường? Liên quan event gì?
- On-chain signals có predict được market không?

---

## Architecture tổng quan

```
[INGESTION]          [STORAGE]             [PROCESSING]        [SERVE]
CoinGecko API   ──►  S3 Bronze (raw)  ──►  Spark/Glue    ──►  Metabase
Binance API     ──►  S3 Silver        ──►  dbt Core      ──►  Superset
Fear&Greed API  ──►  S3 Gold / Redshift──► Great Expect.  ──►  dbt Docs
                         ▲
                    Airflow DAGs (orchestration)
                    Terraform (IaC — tạo toàn bộ AWS infra)
```

**Pattern:** Medallion Architecture (Bronze → Silver → Gold)
**Platform:** AWS (S3, Glue, Redshift, Athena)
**Region:** ap-southeast-1 (Singapore)

---

## Tech stack chi tiết

| Layer | Tool | Lý do chọn |
|---|---|---|
| Ingestion | Python scripts (custom) | Full control, dễ extend |
| Streaming | Kafka + Binance WebSocket | Simulate real-time cho portfolio |
| Orchestration | Apache Airflow | Industry standard, DAG-based |
| Transformation | dbt Core | Analytics Engineering best practice |
| Storage | S3 (Parquet) + Redshift/Athena | Cost-effective, serverless query |
| Data Quality | Great Expectations | Production-grade validation |
| BI | Metabase / Superset | Free, self-hosted |
| IaC | Terraform | Reproducible AWS infra |
| Containerization | Docker + Docker Compose | Local dev environment |

---

## Cấu trúc thư mục (target state)

```
bitcoin_pipeline/
├── CLAUDE.md                   ← file này
├── README.md
├── docker-compose.yml          ← local: Airflow + Kafka + Postgres
├── requirements.txt
│
├── terraform/                  ← AWS infrastructure as code
│   ├── main.tf
│   ├── variables.tf
│   ├── s3.tf                   ← Bronze/Silver/Gold buckets
│   ├── redshift.tf
│   ├── glue.tf
│   └── outputs.tf
│
├── ingestion/                  ← Data source fetchers [DONE]
│   ├── sources/
│   │   ├── coingecko.py        ← Historical price, market cap, volume
│   │   ├── binance.py          ← OHLCV klines + WebSocket stream
│   │   └── fear_greed.py       ← Sentiment index (alternative.me)
│   └── utils/
│       ├── base_fetcher.py     ← Abstract base: retry, rate limit, session
│       └── s3_writer.py        ← Parquet writer với Hive partitioning
│
├── dags/                       ← Airflow DAGs
│   ├── btc_daily_ingestion.py  ← Daily batch: 3 sources → S3 Bronze [DONE]
│   ├── btc_dbt_transform.py    ← Trigger dbt runs [TODO]
│   └── btc_data_quality.py     ← Great Expectations checks [TODO]
│
├── dbt/                        ← dbt project [TODO]
│   ├── dbt_project.yml
│   ├── profiles.yml
│   ├── models/
│   │   ├── staging/            ← Silver: clean + type cast raw data
│   │   │   ├── stg_coingecko_market_chart.sql
│   │   │   ├── stg_binance_klines.sql
│   │   │   └── stg_fear_greed_index.sql
│   │   ├── intermediate/       ← Join + enrich
│   │   │   └── int_btc_daily_enriched.sql
│   │   └── marts/              ← Gold: business-ready aggregations
│   │       ├── mart_btc_price_analysis.sql
│   │       ├── mart_btc_sentiment_correlation.sql
│   │       └── mart_btc_anomaly_detection.sql
│   └── tests/
│
├── tests/                      ← Unit tests
│   └── test_apis.py            ← API smoke tests [DONE]
│
└── config/
    └── settings.py             ← Centralized config (env vars) [TODO]
```

---

## Data sources & schemas

### 1. CoinGecko — `bronze/coingecko/market_chart/`
```
date            TIMESTAMP (UTC)
coin_id         STRING          e.g. "bitcoin"
price_usd       FLOAT
market_cap_usd  FLOAT
volume_usd      FLOAT
ingested_at     TIMESTAMP
```

### 2. Binance — `bronze/binance/klines_1d/` và `klines_1h/`
```
open_time       TIMESTAMP (UTC)
close_time      TIMESTAMP (UTC)
symbol          STRING          e.g. "BTCUSDT"
interval        STRING          e.g. "1d", "1h"
open            FLOAT
high            FLOAT
low             FLOAT
close           FLOAT
volume          FLOAT
quote_volume    FLOAT
num_trades      INT
ingested_at     TIMESTAMP
```

### 3. Fear & Greed — `bronze/feargreed/index/`
```
date                TIMESTAMP (UTC)
fear_greed_value    INT     (0–100)
sentiment_label     STRING  e.g. "Extreme Fear", "Greed"
sentiment_bucket    STRING  (coarser classification)
ingested_at         TIMESTAMP
```

---

## S3 partitioning convention

**Hive-style partitioning** — tất cả Bronze data đều theo format:
```
s3://bitcoin-pipeline-bronze/{source}/{dataset}/year={YYYY}/month={MM}/day={DD}/
```

Ví dụ:
```
s3://bitcoin-pipeline-bronze/coingecko/market_chart/year=2024/month=01/day=15/market_chart_20240115_020000.parquet
```

Lý do: Athena và Spark đọc nhanh hơn nhờ **partition pruning** — chỉ scan đúng folder cần thiết.

---

## Airflow DAGs hiện có

### `btc_daily_ingestion` [DONE]
- Schedule: `0 2 * * *` (02:00 UTC daily)
- Tasks: `ingest_coingecko` ║ `ingest_binance` ║ `ingest_fear_greed` → `validate_bronze`
- 3 ingestion tasks chạy **parallel**, validate chạy sau khi tất cả pass
- Dùng XCom để pass S3 paths giữa tasks
- `trigger_rule="all_success"` cho validate task

---

## Coding conventions

- **Python 3.11+**, type hints bắt buộc trên tất cả functions
- Docstrings theo **Google style**
- Tất cả data files: **Parquet format** (không dùng CSV)
- Logging dùng `logging` module (không dùng `print`)
- Exceptions phải explicit — không dùng bare `except:`
- Mỗi source fetcher phải kế thừa `BaseFetcher`
- S3 writes phải đi qua `S3Writer` class (không dùng boto3 trực tiếp trong task functions)

---

## Progress & next steps

### ✅ Done
- [x] Ingestion scripts: `coingecko.py`, `binance.py`, `fear_greed.py`
- [x] Base utilities: `base_fetcher.py`, `s3_writer.py`
- [x] Airflow DAG: `btc_daily_ingestion.py`
- [x] API smoke test: `tests/test_apis.py`
- [x] **Phase 1 — Bronze local ingestion ổn định** (`run_local_bronze_ingestion.py`)
  - Chuẩn hóa logging (timestamps, level, name)
  - Validation per-dataset: not empty, required columns (subset check), no nulls in key cols, datetime types
  - Run summary table: source, dataset, status, row count, min/max timestamp
  - Continue-on-failure: thử tất cả 4 datasets, exit code 1 nếu có fail
  - 4/4 datasets PASS: coingecko(31), binance_1d(886), binance_1h(821), feargreed(365)
- [x] **Phase 2 — Local analytics MVP cho portfolio** (`README.md`, dbt + DuckDB, Streamlit)
  - Thêm root `README.md` làm portfolio entrypoint: architecture, business questions, setup/run commands, output contract, data quality, interview talking points
  - Thêm `.env.example` và `bitcoin_pipeline/config/settings.py` cho `DATA_DIR`, `DUCKDB_PATH`, S3/AWS defaults
  - Thêm dbt project chạy trên DuckDB, đọc Bronze Parquet bằng `read_parquet(...)`
  - Silver staging models: CoinGecko market chart, Binance 1d/1h klines, Fear & Greed index
  - Gold marts: price trend MA7/MA30/volatility, sentiment correlation, volume anomaly detection
  - Thêm dbt tests cho not-null, accepted interval values, Fear & Greed range, price/volume sanity
  - Thêm Streamlit dashboard đọc Gold marts, không đọc trực tiếp Bronze raw files
  - Thêm Dockerfile và `docker-compose.yml` cho dashboard local demo

### 🔲 Next — làm theo thứ tự này
1. **Verify local analytics MVP end-to-end**: install new dependencies, run Bronze ingestion, `dbt run`, `dbt test`, and open Streamlit
2. **Add dashboard screenshot to README** sau khi UI chạy ổn định
3. **Phase 3 — MinIO/S3-compatible Bronze**: sửa `S3Writer` nhận `endpoint_url`, dựng MinIO bằng Docker
4. **Airflow local Docker** — Airflow + Postgres metadata DB, orchestrate Bronze + dbt
5. **Great Expectations** — data quality checks tại Bronze và Silver
6. **Cloud phase** — AWS S3/Athena hoặc Redshift, Terraform, CI/CD
7. **Streaming bonus** — Kafka + Binance WebSocket sau khi batch pipeline ổn định

### ⚠️ Known issues / decisions pending
- Redshift vs Athena: Redshift tốt hơn cho complex queries nhưng tốn tiền hơn.
  Với fresher portfolio, **Athena trước** (serverless, pay-per-query) rồi migrate sau.
- Kafka layer hiện chỉ có code trong `binance.py` — chưa setup broker.
  Plan: dùng Docker Compose để run Kafka locally trước khi deploy lên MSK.
- Local MVP dùng DuckDB trước để reviewer có thể chạy demo nhanh mà chưa cần AWS/Airflow.

---

## Người phát triển

- **Tên:** Uyên
- **Background:** Sinh viên IS năm cuối, đang làm thesis
- **Skill hiện tại:** SQL, Python/Pandas, Docker/Linux, AWS cơ bản
- **Đang học thêm:** Airflow, dbt, Spark, Terraform
- **Mục tiêu:** Fresher DE job tại outsource/service company ở Việt Nam

---

## Khi làm việc trong project này

1. **Đọc file này trước** khi làm bất kỳ task nào
2. Giữ đúng folder structure đã định nghĩa ở trên
3. Mọi function mới phải có type hints + docstring
4. Sau khi viết code xong, chạy `tests/test_apis.py` để verify
5. Update phần **Progress** trong file này khi hoàn thành task
