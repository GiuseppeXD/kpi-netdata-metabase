# ClickHouse Monitoring Stack Test Environment

A monitoring stack that integrates **Netdata** with **ClickHouse** for time-series metrics storage and **Metabase** for data visualization. This project provides both synthetic data generation for testing and real production data ingestion from Netdata parent-child setups.

## 🏗️ Architecture

```
Netdata Parent/Children → Custom Proxy → ClickHouse → Metabase
                ↓
         SSH Tunnel (optional)
                ↓
        Python Forwarder → Custom Proxy
```

### Data Flow
1. **Netdata Parent** aggregates metrics from child nodes via streaming
2. **Netdata Forwarder** (Python script) pulls metrics via API and forwards to proxy
3. **Custom Proxy** (Node.js) converts Netdata JSON to ClickHouse format
4. **ClickHouse** stores time-series data with automatic partitioning and TTL
5. **Metabase** provides dashboards and visualization

## 🔧 Components

### Core Services
- **ClickHouse**: Time-series database with optimized storage
- **Netdata Proxy**: Express.js service for format conversion
- **Netdata Forwarder**: Python script for API data extraction
- **Metabase**: Dashboard and visualization platform
- **PostgreSQL**: Metabase metadata storage

### Optional Test Components
- **Data Generator**: Python script for synthetic metrics
- **Local Netdata**: Containerized Netdata for testing

## 🚀 Quick Start

### Prerequisites
- Docker and Docker Compose
- Python 3.8+
- SSH access to Netdata parent server (for production data)

### 1. Start Core Infrastructure

```bash
# Clone or navigate to project directory
cd clickhouse-test

# Start ClickHouse and Proxy
docker-compose up -d clickhouse netdata-proxy

# Verify services
docker-compose ps
curl http://localhost:8081/health  # Proxy health check
```

### 2. Initialize Database Schema

```bash
# Run the database setup script
./setup_clickhouse_init.sh

# Verify tables were created
docker exec clickhouse-test clickhouse-client --query "SHOW TABLES FROM netdata_metrics"
```

### 3. Choose Data Source

#### Option A: Real Production Data (Recommended)

```bash
# 1. Setup SSH tunnel to your Netdata parent server
ssh -L 19999:localhost:19999 user@your-netdata-server

# 2. Install Python dependencies (if needed)
pip3 install requests

# 3. Run the forwarder (pulls from real Netdata via tunnel)
python3 netdata-forwarder.py

# 4. Check data is flowing
./check-data.sh
```

#### Option B: Synthetic Test Data

```bash
# Start data generator
docker-compose up -d data-generator

# Monitor logs
docker-compose logs -f data-generator
```

### 4. Setup Metabase (Optional)

```bash
# Start Metabase
docker-compose up -d metabase postgres

# Access Metabase
open http://localhost:3000

# Configure ClickHouse connection:
# Host: clickhouse
# Port: 8123
# Database: netdata_metrics
# Username: netdata
# Password: netdata123
```

## 📊 Database Schema

### Main Metrics Table
```sql
CREATE TABLE metrics (
    timestamp DateTime64(3),
    hostname LowCardinality(String),
    chart_id LowCardinality(String),
    chart_name String,
    dimension LowCardinality(String),
    value Float64,
    units String,
    family String,
    context String,
    chart_type String
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(timestamp)
ORDER BY (hostname, chart_id, dimension, timestamp)
TTL timestamp + INTERVAL 30 DAY;
```

### Hourly Aggregation
- Automatic rollups via materialized views
- 1-year retention for aggregated data
- Optimized for dashboard queries

## 🔍 Data Verification

### Check Data Ingestion
```bash
# View summary
./check-data.sh

# Manual queries
docker exec clickhouse-test clickhouse-client --query "
    SELECT hostname, count() as metrics, uniq(chart_id) as charts 
    FROM netdata_metrics.metrics 
    GROUP BY hostname 
    ORDER BY metrics DESC
"
```

### Common Queries
```sql
-- Recent disk usage by host
SELECT 
    hostname, 
    chart_id, 
    dimension, 
    value, 
    units
FROM netdata_metrics.metrics 
WHERE chart_id LIKE '%disk_space%' 
AND timestamp > now() - INTERVAL 1 HOUR
ORDER BY timestamp DESC;

-- CPU usage trends
SELECT 
    toStartOfHour(timestamp) as hour,
    hostname,
    avg(value) as avg_cpu
FROM netdata_metrics.metrics 
WHERE chart_id = 'system.cpu' 
AND dimension = 'user'
GROUP BY hour, hostname
ORDER BY hour DESC;
```

## 🛠️ Configuration

### Environment Variables
Set in `docker-compose.yml`:

```yaml
# ClickHouse Connection
CLICKHOUSE_HOST: clickhouse
CLICKHOUSE_PORT: 8123
CLICKHOUSE_DATABASE: netdata_metrics
CLICKHOUSE_USER: netdata
CLICKHOUSE_PASSWORD: netdata123

# Forwarder Settings
NETDATA_URL: http://localhost:19999
PROXY_URL: http://localhost:8081
INTERVAL_SECONDS: 30
```

### Netdata Forwarder Configuration
Edit `netdata-forwarder.py`:
```python
NETDATA_URL = "http://localhost:19999"  # SSH tunneled endpoint
PROXY_URL = "http://localhost:8081"     # Local proxy
INTERVAL_SECONDS = 30                   # Collection frequency
```

## 📋 Production Setup

### 1. Netdata Parent-Child Configuration
On child nodes (`stream.conf`):
```ini
[stream]
enabled = yes
destination = PARENT_IP:19999
api key = YOUR_API_KEY
```

On parent node (`stream.conf`):
```ini
[YOUR_API_KEY]
enabled = yes
default history = 3600
default memory mode = save
health enabled = yes
```

### 2. SSH Tunnel for Secure Access
```bash
# Setup persistent tunnel
ssh -f -N -L 19999:localhost:19999 user@netdata-parent-server

# Or use autossh for reliability
autossh -f -N -L 19999:localhost:19999 user@netdata-parent-server
```

### 3. Systemd Service (Optional)
Create `/etc/systemd/system/netdata-forwarder.service`:
```ini
[Unit]
Description=Netdata to ClickHouse Forwarder
After=network.target

[Service]
Type=simple
User=netdata
WorkingDirectory=/path/to/clickhouse-test
ExecStart=/usr/bin/python3 netdata-forwarder.py
Restart=always
RestartSec=30

[Install]
WantedBy=multi-user.target
```

## 🐛 Troubleshooting

### Common Issues

**Forwarder not collecting data:**
```bash
# Check Netdata API accessibility
curl http://localhost:19999/api/v1/info

# Test individual host endpoint
curl "http://localhost:19999/host/HOSTNAME/api/v1/allmetrics?format=json"

# Check proxy health
curl http://localhost:8081/health
```

**Database connection errors:**
```bash
# Verify ClickHouse is running
docker exec clickhouse-test clickhouse-client --query "SELECT 1"

# Check table exists
docker exec clickhouse-test clickhouse-client --query "SHOW TABLES FROM netdata_metrics"
```

**Dimension name issues:**
- Ensure forwarder sends `id` field, not `dimension`
- Check proxy processes metrics correctly
- Verify chart IDs match expected format

### Logs and Monitoring
```bash
# Proxy logs
docker-compose logs -f netdata-proxy

# ClickHouse logs  
docker-compose logs -f clickhouse

# Forwarder logs (when run manually)
python3 netdata-forwarder.py --once  # Single run for testing
```

## 🎯 Use Cases

- **Production Monitoring**: Real-time metrics from distributed Netdata setup
- **Performance Testing**: Synthetic load generation for ClickHouse optimization
- **Dashboard Development**: Metabase dashboard creation and testing
- **Data Engineering**: Time-series data pipeline development
- **Alerting**: Custom alert rule development and testing

## 📁 Project Structure

```
├── README.md                    # This file
├── CLAUDE.md                   # Claude Code project instructions
├── docker-compose.yml          # Container orchestration
├── setup_clickhouse_init.sh    # Database initialization
├── netdata-proxy/              # Node.js format converter
│   ├── app.js
│   └── package.json
├── data-generator/             # Python synthetic data generator
│   ├── generator.py
│   └── requirements.txt
├── netdata-forwarder.py        # Real data collector
├── netdata-forwarder.sh        # Bash alternative
├── check-data.sh              # Data verification script
└── diagnose-disk-issue.py     # Diagnostic utilities
```

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Test with both synthetic and real data
4. Submit a pull request

## 📜 License

This project is provided as-is for monitoring and testing purposes.