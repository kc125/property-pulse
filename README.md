# 🏠 Property Market Analytics Platform — Azure Cloud

![Azure](https://img.shields.io/badge/Azure-Cloud-blue?logo=microsoft-azure)
![Databricks](https://img.shields.io/badge/Databricks-PySpark-red?logo=apache-spark)
![Delta Lake](https://img.shields.io/badge/Delta-Lake-00ADD8)
![CI/CD](https://img.shields.io/badge/GitHub_Actions-CI/CD-2088FF?logo=github-actions)
![Python](https://img.shields.io/badge/Python-3.9-yellow?logo=python)

---

## 📋 Project Overview

An **end-to-end real-time data engineering pipeline** built on Azure,
processing **162,000+ property listing events** over 3 days of continuous
streaming. The pipeline ingests live property market events, processes them
through **Medallion Architecture** (Bronze → Silver → Gold) and serves
business-ready insights via Delta Lake tables registered in Unity Catalog.

---

## 🏗️ Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                        INGESTION LAYER                          │
│                                                                 │
│   Python Producer ──→ Azure Event Hubs ──→ Structured Streaming │
│   CSV Files       ──→ ADLS Gen2        ──→ Auto Loader          │
└────────────────────────────┬────────────────────────────────────┘
                             ↓
┌─────────────────────────────────────────────────────────────────┐
│                      PROCESSING LAYER                           │
│                                                                 │
│   Bronze Delta  ──→  Silver Delta  ──→  Gold Delta              │
│   (Raw/Append)       (Clean/SCD)        (Aggregated)            │
└────────────────────────────┬────────────────────────────────────┘
                             ↓
┌─────────────────────────────────────────────────────────────────┐
│                     GOVERNANCE LAYER                            │
│                                                                 │
│   Unity Catalog  |  Azure Key Vault  |  Delta Lake ACID         │
└─────────────────────────────────────────────────────────────────┘
                             ↓
┌─────────────────────────────────────────────────────────────────┐
│                   ORCHESTRATION & DEVOPS                        │
│                                                                 │
│   Azure Data Factory  |  Databricks Workflows  |  GitHub Actions│
└─────────────────────────────────────────────────────────────────┘
```

---

## 🛠️ Tech Stack

| Layer | Tool | Purpose |
|-------|------|---------|
| Streaming Ingestion | Azure Event Hubs | Real-time JSON events |
| File Ingestion | Databricks Auto Loader | CSV batch files |
| Processing | Azure Databricks (PySpark) | Transformations & aggregations |
| Storage | ADLS Gen2 + Delta Lake | Medallion Architecture |
| Orchestration | Azure Data Factory | Hourly batch pipeline |
| Streaming Orchestration | Databricks Workflows | Continuous Bronze stream |
| Secret Management | Azure Key Vault | Zero hardcoded credentials |
| Governance | Unity Catalog | Table registry & lineage |
| CI/CD | GitHub Actions | Lint → Test → Deploy |
| Version Control | Git (GitFlow) | Branch strategy & PRs |
| Language | Python, PySpark, SQL | Core development |
| Visualisation | Power BI | Business dashboards |

---

## 📁 Project Structure

```
property-pulse/
│
├── 📂 bronze/
│     ├── 01_stream_to_bronze.py        # Event Hubs Structured Streaming
│     └── 05_autoloader_to_bronze.py    # Auto Loader CSV ingestion
│
├── 📂 silver/
│     ├── 02_bronze_to_silver.py        # Transformations & quality checks
│     └── 07_scd_agent_master.py        # SCD Type 1 & 2 implementation
│
├── 📂 gold/
│     ├── 03_silver_to_gold.py          # Core aggregations (Spark SQL)
│     └── 08_gold_joins.py              # Star Schema joins & analytics
│
├── 📂 optimizations/
│     └── 06_liquid_clustering.py       # Delta Lake optimisations
│
├── 📂 tests/
│     └── test_transformations.py       # pytest unit tests
│
├── 📂 .github/
│     └── 📂 workflows/
│           └── deploy.yml              # GitHub Actions CI/CD pipeline
│
├── 04_register_unity_catalog.py        # Unity Catalog registration
├── .gitignore
└── README.md
```

---

## 📦 Pipeline Layers

### 🥉 Bronze — Raw Ingestion Layer

- **Event Hubs Streaming** → PySpark Structured Streaming reads JSON events
- **Auto Loader** → cloudFiles format detects new CSV files automatically
- **Delta Lake** → Append-only immutable raw storage
- **Checkpoint** → Fault-tolerant offset tracking for zero data loss
- **Auto Compact** → Prevents small file accumulation

### 🥈 Silver — Transformation Layer

- **6 Data Quality Checks** → Completeness, Validity, Range, Volume, Uniqueness, Timeliness
- **Type Casting** → Proper data types enforced (LongType for price, TimestampType)
- **Deduplication** → dropDuplicates on listing_id + event_type + timestamp
- **Deletion Vectors** → GDPR-compliant fast record deletion
- **SCD Type 1** → Agent tier updates via Delta MERGE (no history)
- **SCD Type 2** → Agent city changes with full history tracking
- **Liquid Clustering** → CLUSTER BY (city, event_type) for query optimisation

### 🥇 Gold — Business Layer

- **8 Delta Tables** → Business-ready aggregations via Spark SQL
- **Star Schema** → Silver fact table joined with Agent Master dimension
- **Window Functions** → RANK, DENSE_RANK, LAG for agent rankings and trends
- **CTEs** → Complex business logic for market benchmarking
- **OPTIMIZE + VACUUM** → File compaction and storage management
- **Unity Catalog** → All tables registered with metadata and lineage

---

## 📊 Gold Tables

| Table | Description | Key Metrics |
|-------|-------------|-------------|
| `avg_price_by_city` | Average property price per city | avg, max, min price |
| `listing_count_by_event` | Event type distribution | NEW, CHANGED, REMOVED |
| `price_trend_by_locality` | Daily price trends by area | avg price, listing count |
| `top_active_cities` | City activity ranking | total events, breakdown |
| `agent_performance` | Agent listing analytics | listings, avg price |
| `agent_vs_city_benchmark` | Agent vs market comparison | above/below average flag |
| `top_agents_by_tier` | Tier-wise agent ranking | RANK() within tier |
| `agent_city_movement_impact` | SCD2 city change analysis | performance pre/post move |

---

## 🔐 Security Architecture

```
Azure Key Vault
  ├── eventhub-connection-string    ← Event Hubs access
  ├── adls-account-key              ← ADLS Gen2 access
  └── adls-account-name             ← Storage account config

Databricks Secret Scope
  └── property-pulse-scope          ← Backed by Key Vault

Result → Zero hardcoded credentials across all notebooks
```

---

## 🔄 CI/CD Pipeline

```
Developer pushes to feature branch
              ↓
┌─────────────────────────────────┐
│  Stage 1 — Lint (flake8)        │  ← ALL pushes
│  Code quality & PEP8 standards  │
└─────────────────┬───────────────┘
                  ↓ pass
┌─────────────────────────────────┐
│  Stage 2 — Unit Tests (pytest)  │  ← ALL pushes
│  Transformation logic validated │
└─────────────────┬───────────────┘
                  ↓ pass + merge to main
┌─────────────────────────────────┐
│  Stage 3 — Deploy               │  ← main only
│  Databricks CLI auto-deployment │
└─────────────────────────────────┘
```

---

## 🌿 Git Branch Strategy

```
main          ← Production (protected)
develop       ← Integration
feature/*     ← Individual features

Rules:
  → No direct pushes to main
  → All CI checks must pass
  → PR review required
  → Auto-deploy on merge to main
```

---

## ⚙️ ADF Orchestration

```
pl_silver_gold_orchestration (hourly trigger)
  │
  ├── act_run_silver    ← Silver transformation
  │         ↓ on success only
  └── act_run_gold      ← Gold aggregations

Retry policy: 2 retries × 60 second intervals
Bronze: Databricks Workflows (continuous)
```

---

## 📈 Project Results

```
✅ 162,000+  events processed
✅ 3 days    continuous streaming
✅ 15        duplicates auto-removed
✅ 8         Gold tables serving insights
✅ 6         data quality checks automated
✅ 100%      CI/CD automated deployment
✅ 0         hardcoded credentials
✅ 11,543    small files compacted via OPTIMIZE
```

---

## 🚀 How to Run

### Prerequisites
```
- Azure Subscription
- Databricks Workspace (DBR 13.3 LTS+)
- Azure Event Hubs (Standard tier)
- ADLS Gen2 Storage Account
- Azure Key Vault
- Azure Data Factory
```

### Setup Steps

**1. Configure Key Vault Secrets**
```
eventhub-connection-string → Event Hubs connection string
adls-account-key           → ADLS access key
adls-account-name          → Storage account name
```

**2. Create Databricks Secret Scope**
```
https://<databricks-url>#secrets/createScope
Scope Name : property-pulse-scope
DNS Name   : https://<keyvault-name>.vault.azure.net/
```

**3. Start Event Producer**
```bash
pip install azure-eventhub azure-keyvault-secrets azure-identity
python event_producer.py
```

**4. Run Bronze Streaming**
```
Databricks → bronze/01_stream_to_bronze.py → Run All
```

**5. Trigger ADF Pipeline**
```
ADF Studio → pl_silver_gold_orchestration → Trigger Now
```

**6. Verify Unity Catalog**
```
Databricks → Catalog → property_pulse_dbx → property_db
```

---

## 🧪 Running Tests

```bash
pip install pytest pyspark
pytest tests/ -v
```

---

## 👤 Author

**Tharun KC**
- 📧 kctharunuk@gmail.com
- 🔗 [LinkedIn](https://linkedin.com/in/tharunkc300)
- 📍 Bengaluru, Karnataka, India

---

## 📄 License

This project is for portfolio and demonstration purposes.

---

*Built with ❤️ using Azure Cloud, Databricks and Delta Lake*
