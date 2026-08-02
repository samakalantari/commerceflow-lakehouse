# CommerceFlow Lakehouse

An end-to-end data engineering project that ingests transactional and behavioral
e-commerce events from Kafka and refines them through a Bronze → Silver → Gold
lakehouse into BI-ready analytical models.

## Overview

Raw Avro events are streamed from Kafka into an object-store lakehouse, cleaned and
conformed into ACID tables, then published as denormalized models for reporting.

Design goals (`pasted-text.txt`):
- A replayable data flow from Kafka through to analytical models
- Preserved Kafka lineage and metadata for traceability and recovery
- Cleansed, typed, validated tables as the trusted source of truth
- Quality control: deduplication, referential validation, quarantine of bad records
- Query-oriented models for dashboards and reporting
- Repeatable execution with audit, monitoring, and recovery

## Architecture

Kafka (Avro + Schema Registry)
        │
        ▼
  Bronze  ── Spark Structured Streaming → Parquet on MinIO (S3A)
        │      one streaming query + checkpoint per topic
        │      partitioned by year / month / day
        ▼
  Silver  ── Spark batch → Apache Iceberg on MinIO (PostgreSQL JDBC catalog)
        │      normalization, typing, dedup, surrogate keys, SCD2, quarantine
        ▼
   Gold   ── Spark → ClickHouse (staging → validate → atomic exchange)
        │
        ▼
     Metabase dashboards

### Bronze
Decodes Avro messages via Schema Registry and persists them as Parquet with minimal
structural change. Kafka `topic`, `partition`, `offset`, `timestamp`, `key`, and
ingestion time are retained as lineage. Bronze deliberately performs **no** business
validation, deduplication, or quarantine — those belong to Silver
(`pasted-text.txt:14`, `pasted-text.txt:34`).

### Silver
Canonical layer. Trimming/normalization, type casting, deduplication and
latest-version selection, deterministic surrogate keys (`xxhash64`), and fact ↔
dimension relationship resolution. Invalid records are routed to quarantine tables.

Models (`pasted-text.txt:450`, `pasted-text.txt:466`):
- `dim_date`
- `dim_user`
- `dim_product` (SCD Type 2)
- `fact_order`
- `fact_order_item`
- `fact_return_refund`

### Gold
Denormalized, consumer-facing projections. The transactional model is a
**One Big Table (OBT)** joining orders, order items, date, user, and product. Spark
writes to a ClickHouse staging table, validates it, then promotes it to production
via atomic exchange (`pasted-text.txt:1254`, `pasted-text.txt:1268`).

## Tech Stack

| Concern | Technology |
|---|---|
| Event transport | Apache Kafka |
| Message format / schema | Apache Avro, Schema Registry |
| Ingestion & transformation | Apache Spark, Spark Structured Streaming |
| Object storage | MinIO (via Hadoop S3A) |
| Raw file format | Parquet |
| Table format | Apache Iceberg |
| Iceberg catalog | PostgreSQL (JDBC) |
| Serving / OLAP | ClickHouse |
| Orchestration | Apache Airflow |
| BI | Metabase |
| Monitoring | Prometheus, Grafana |
| Local environment | Docker Compose |

## Source Topics

Transactional:
- `transactional.categories`
- `transactional.products`
- `transactional.users`
- `transactional.orders`
- `transactional.order_items`
- `transactional.returns_refunds`
- `transactional.product_price_history`

Behavioral:
- `behavioral.events`

(`pasted-text.txt:47`, `pasted-text.txt:70`)

## Orchestration

- `behavioral_silver_etl` — behavioral Silver DAG, runs every 3 hours
- `gold_transactional_etl` — transactional Gold DAG
- `audit_silver` — uniqueness, null-key, referential integrity, and SCD2 checks
- `audit_gold` — reconciles Gold against Silver

## Getting Started

bash
# bring up Kafka, Schema Registry, MinIO, PostgreSQL, ClickHouse,
# Airflow, Metabase, Prometheus, Grafana
docker compose up -d

Then enable the Bronze streaming queries and trigger the Silver and Gold DAGs from
the Airflow UI. Grafana ships with provisioned dashboards for Airflow and pipeline
health.

## Known Limitations

- Transactional Gold is **not** automatically triggered on Silver success. Ordering
  must be enforced by an operator or an external orchestrator
  (`pasted-text.txt:1316`).
- `fact_return_refund` is not yet included in the transactional OBT
  (`pasted-text.txt:1333`).

## Data Contract

Silver is the canonical, trusted layer. Gold contains consumer-oriented projections
that are fully rebuildable from Silver (`pasted-text.txt:1294`).
