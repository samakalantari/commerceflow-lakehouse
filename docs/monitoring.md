
The data pipeline consists of:
1. **Metrics Emission:** Airflow emits raw StatsD counter, gauge, and timer metrics over UDP to `statsd-exporter`.
2. **Metrics Translation:** The `statsd-exporter` processes incoming UDP packets, translates them into Prometheus-compatible format, and exposes them at `http://statsd-exporter:9102/metrics`.
3. **Metrics Scraping:** Prometheus scrapes target endpoints every 15 seconds.
4. **Visualization:** Grafana uses Prometheus as its primary database source to load pre-configured Dashboards on startup.

---

## 2. Infrastructure Services (Docker Compose)

The monitoring stack is defined within `docker-compose.yml` under the following service blocks:

### 2.1 StatsD Exporter (`statsd-exporter`)
Acts as the bridge between Airflow's native metric system and Prometheus.
* **Image:** `${STATSD_EXPORTER_IMAGE}`
* **Container Name:** `statsd-exporter`
* **Ports Exposed:**
  * `9102/TCP`: Prometheus scrapable port.
  * `9125/UDP`: StatsD intake port.
* **Network:** `internal`

### 2.2 Prometheus (`prometheus`)
The time-series database storing all performance metrics.
* **Image:** `${PROMETHEUS_IMAGE}`
* **Dependencies:** `statsd-exporter`
* **Ports Exposed:** `${BIND_IP}:${PROMETHEUS_PORT}:9090` (Internal port `9090`)
* **Volumes:**
  * `./prometheus.yml:/etc/prometheus/prometheus.yml:ro` (Configuration mapping)
  * `prometheus_data:/prometheus` (Persistent storage)
* **Command Arguments:**
  * `--config.file=/etc/prometheus/prometheus.yml`
  * `--storage.tsdb.path=/prometheus`
  * `--storage.tsdb.retention.time=${PROMETHEUS_RETENTION}`
* **Network:** `internal`, `traefik_network`
* **Routing (Traefik):** `Host(\`prometheus.${DOMAIN_SUFFIX}\`)`

### 2.3 Grafana (`grafana`)
The visualization and alerting dashboard platform.
* **Image:** `${GRAFANA_IMAGE}`
* **Dependencies:** `prometheus`
* **Environment Variables:**
  * `GF_SECURITY_ADMIN_USER`: `${GRAFANA_ADMIN_USER}`
  * `GF_SECURITY_ADMIN_PASSWORD`: `${GRAFANA_ADMIN_PASSWORD}`
* **Volumes:**
  * `grafana_data:/var/lib/grafana` (Persistent state)
  * `./grafana/provisioning:/etc/grafana/provisioning:ro` (Automatic datasource/dashboard bootstrap)
  * `./grafana/dashboards:/var/lib/grafana/dashboards:ro` (Dashboard JSON storage)
* **Network:** `internal`, `traefik_network`
* **Routing (Traefik):** `Host(\`grafana.${DOMAIN_SUFFIX}\`)`

---

## 3. Scraping Configuration (`prometheus.yml`)

Prometheus is configured to scrape targets at a **15-second interval**. The scraping configuration targets the local Prometheus instance and the StatsD Exporter.
```yaml
global:
  scrape_interval: 15s

scrape_configs:
  - job_name: "prometheus"
static_configs:
- targets: ["prometheus:9090"]

  - job_name: "airflow-statsd"
static_configs:
- targets: ["statsd-exporter:9102"]
