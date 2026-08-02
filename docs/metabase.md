```markdown
# Metabase

## Overview

Metabase serves as the Business Intelligence (BI) layer of the `commerceflow-lakehouse` repository. It is deployed as a dedicated service in the infrastructure stack to expose analytical datasets stored in ClickHouse to end users through dashboard collections, KPI cards, and saved questions.

This document details:
- Deployment configurations and environment variables
- Internal metadata persistence
- ClickHouse analytical source integration
- Spark write paths and target Gold tables
- Behavioral metrics available for analysis
- UI dashboard structure based on live instances

---

## Deployment Configuration

Metabase is defined as a standalone container service in `docker-compose.yml`.

### Verified Service Definition

- **Service Name:** `metabase`
- **Image:** `${METABASE_IMAGE}`
- **Internal Port:** `3000`
- **Network Interfaces:**
  - Internal network: `${COMPOSE_PROJECT_NAME}_internal`
  - External router network: `${TRAEFIK_NETWORK}`
- **Dependency:** Waits for the `postgres` database service health check to pass.

### Traefik Routing Configuration

The repository configures reverse-proxy routing through Traefik using a symbolic routing rule:
- **Compose Route:** `mb.${DOMAIN_SUFFIX}`

Based on live environments, this resolves to:
- **Observed Live Domain:** `mb.group4.querabootcamp-de.ir`

---

## Internal Metadata Database

Metabase stores user roles, session configurations, collection structures, question schemas, and dashboard layouts in an internal PostgreSQL database.

### Database Credentials configuration

The metadata database configuration is supplied to the `metabase` container via the following environment variables:

| Environment Variable | Value |
|---|---|
| `MB_DB_TYPE` | `postgres` |
| `MB_DB_DBNAME` | `${POSTGRES_DB}` |
| `MB_DB_PORT` | `5432` |
| `MB_DB_USER` | `${POSTGRES_USER}` |
| `MB_DB_PASS` | `${POSTGRES_PASSWORD}` |
| `MB_DB_HOST` | `postgres` |

*Note: This PostgreSQL instance is solely dedicated to Metabase's application metadata and does not serve as the warehouse for client analytics queries.*

---

## Connection to ClickHouse (Analytical Source)

To visualize analytics in Metabase, the analytical ClickHouse instance must be connected via JDBC in the Metabase Admin interface.

### Default Connection Parameters

| Field | Repository Value |
|---|---|
| **Database Type** | ClickHouse |
| **Host** | `clickhouse` |
| **HTTP Port** | `8123` |
| **Database** | `gold` |
| **User** | `default` (or `${CLICKHOUSE_USER}`) |
| **Password** | `${CLICKHOUSE_PASSWORD}` |

### Spark JDBC URL Construction

The Spark applications populating the Gold layer build the JDBC URL matching the configuration above:

```text
jdbc:clickhouse://clickhouse:8123/gold
```

---

## Data Model and Gold Tables

Analytical tables are populated from the Silver layer Iceberg tables using Spark jobs inside `spark_apps/gold/`. 

### Target Gold Tables

#### 1. `gold.gold_behavioral_daily`
This table aggregates behavioral events on a daily basis.
- **Source Tables (Silver Layer):**
  - `lakehouse.silver.fact_behavioral_event`
  - `lakehouse.silver.fact_order`
  - `lakehouse.silver.fact_order_item`
- **Output Target:** `gold.gold_behavioral_daily` (configured via `CH_TARGET_TABLE`)

#### 2. `gold.transactional_obt`
A flat, denormalized One Big Table (OBT) compiled for transactional analysis.
- **Target Table:** `gold.transactional_obt`
- **Staging Table:** `gold.transactional_obt_staging`

---

## Behavioral Metrics

The following processed behavioral metrics are written to `gold.gold_behavioral_daily` and are available for Metabase queries and visualizations:

- `unique_users` - Daily unique user count.
- `sessions` - Distinct session identifiers.
- `total_events` - Raw event count aggregated.
- `page_views` - Total count of page view interactions.
- `searches` - Total search events executed by users.
- `add_to_cart_events` - Count of items added to shopping carts.
- `checkout_start_events` - Sessions transitioning into checkout.

---

## Data Refresh and Validation Workflow

Spark applications ensure data integrity before publishing records to the production tables exposed in Metabase:

1. **Write to Staging:** Spark writes the generated Gold dataset into a staging table (e.g., `gold.gold_behavioral_daily_staging`).
2. **Row-Count Validation:** The application compares the row counts in Spark against the records successfully loaded into ClickHouse staging.
3. **Table Exchange:** If validation passes, a native ClickHouse atomic exchange operation swaps the tables:
   ```sql
   EXCHANGE TABLES gold.gold_behavioral_daily AND gold.gold_behavioral_daily_staging;
   ```
4. **Publish Verification:** The final production table row count is confirmed, ensuring no interruption to active Metabase queries.

---

## Dashboard and Collection Structure

Dashboards in the live Metabase interface are organized into two key collections:

### 1. Behavioral Collection
Accessible at `mb.group4.querabootcamp-de.ir/collection/6-behavioral`. It contains behavioral analyses and user funnel items:
- `abandonment rate`
- `active user, buyer frequency`
- `cart and checkout abandonment rate`
- `converted sessions to reach payment`
- `device performance`
- `Device performance matrix`
- `gold_behavioral_daily`
- `Main Funnel`
- `new vs returning users`
- `non-converting sessions remarketing`
- `Segmented distribution:`
- `session-level Funnel`

### 2. Transactional Collection
Accessible at `mb.group4.querabootcamp-de.ir/collection/7-transactional`. It focuses on transactional KPIs and financial summaries:
- `Basket Size Performance`
- `KPI - Average Order Value`
- `KPI - Average Units per Order`
- `KPI - Gross Order Value`
- `KPI - Total Orders`
- `KPI - Total Units Ordered`
- `KPI - Unique Customers`
- `Loyalty Tier Performance`
- `Order Status Performance`
- `Order Value by Payment Method`
- `Orders by Current Status`
- `Orders by Payment Method`
- `Top 10 Locations by Order Value`
- `Transactional Overview`

---

## Operational Guidelines

### Schema Synchronization
Metabase caches table schemas. If you modify Spark jobs to add new metric fields or create new tables:
1. Log in to the Metabase Admin Panel.
2. Navigate to **Databases** -> ClickHouse.
3. Click **Sync database schema now** to refresh metadata.

### Staging Tables Exclusion
To avoid confusing analysts, tables with `_staging` suffixes (such as `transactional_obt_staging`) should be hidden from user search visibility in the Metabase **Data Model** settings.
```
