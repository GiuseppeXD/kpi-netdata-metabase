# Netdata to GraphQL Exporter

This repository contains a minimal toolchain to pull metrics from a Netdata API that is exposed locally (for example via an SSH tunnel on `localhost:19999`) and forward the data to a GraphQL API. The stack is intentionally small: a Node.js proxy converts Netdata JSON payloads into GraphQL mutations and a Python forwarder polls Netdata on a schedule.

## Components

- **GraphQL proxy** (`netdata-proxy-graphql/`)
  - Receives Netdata JSON payloads (HTTP or JSONL over TCP)
  - Converts metrics into GraphQL `createRecord` mutations and batches them per request
  - Requires a GraphQL endpoint, sheet ID, and API key
- **Forwarder** (`netdata-forwarder.py`)
  - Polls the Netdata API for a selectable list of charts and aggregations
  - Sends batches to the proxy (`/avg`, `/max`, `/median` endpoints)
  - Designed to talk to a Netdata instance on `localhost:19999`
- **Docker Compose** (`docker-compose.yml`)
  - Builds and runs the proxy
  - Launches the Python forwarder with configurable environment variables

## Prerequisites

- Docker and Docker Compose
- A Netdata instance reachable at `http://localhost:19999` (commonly exposed via `ssh -L 19999:localhost:19999 user@host`)
- A GraphQL API key with permission to insert records

## Getting Started

1. Copy the environment template and fill in your credentials:
   ```bash
   cp .env.example .env
   # edit .env to set API_KEY and optional overrides
   ```

2. Establish the SSH tunnel (if needed):
   ```bash
   ssh -L 19999:localhost:19999 user@your-netdata-host
   ```

3. Start the stack:
   ```bash
   docker-compose up -d
   ```

4. Check service health:
   ```bash
   docker-compose ps
   curl http://localhost:8090/health       # GraphQL proxy status
   docker-compose logs -f netdata-forwarder
   ```

The forwarder polls Netdata every 60 seconds by default and pushes metrics to the proxy, which then writes them to the configured GraphQL endpoint.

## Configuration

All runtime settings can be supplied through `.env` or the shell. Key variables:

| Variable | Description | Default |
|----------|-------------|---------|
| `API_KEY` | GraphQL API authentication token (required) | — |
| `GRAPHQL_ENDPOINT` | GraphQL endpoint URL | `https://digitalize.oxeanbits.com/graphql` |
| `SHEET_ID` | Target sheet ID for record creation | `68bf175e85d08f78bf97b15f` |
| `NETDATA_URL` | Netdata base URL | `http://host.docker.internal:19999` |
| `GRAPHQL_PROXY_URL` | Proxy URL used by the forwarder | `http://netdata-graphql-proxy:8090` |
| `CHARTS` | Comma-separated Netdata chart IDs to query | `system.cpu,disk_space./` |
| `CHART_FILTER` | Substring filter applied to `CHARTS` (`*` disables filtering) | `disk_space` |
| `AGGREGATION_TYPES` | Comma-separated aggregations (`average`, `median`, `max`) | `average,median,max` |
| `NETDATA_HOSTS` | Optional list of mirrored hostnames to poll | _(auto-detected)_ |
| `INTERVAL_SECONDS` | Polling interval | `60` |
| `REQUEST_TIMEOUT` | HTTP timeout for Netdata/proxy calls | `120` secs |
| `FORWARDER_LOG_LEVEL` | Forwarder log level | `INFO` |
| `GRAPHQL_BATCH_SIZE` | Metrics per GraphQL mutation batch | `20` |

When running in Docker the forwarder resolves the host machine via `host.docker.internal`. If your Docker version does not support this alias, override `NETDATA_URL` with the appropriate address (for example `http://172.17.0.1:19999`).

## Running the Forwarder Manually

You can execute the forwarder outside Docker for testing:

```bash
python3 netdata-forwarder.py --once   # single poll
python3 netdata-forwarder.py          # continuous mode
```

Export the same environment variables used in `.env` so the script can reach Netdata and the proxy.

## Observability

- Proxy logs: `docker-compose logs -f netdata-graphql-proxy`
- Forwarder metrics: `docker-compose logs -f netdata-forwarder`
- Health endpoint: `curl http://localhost:8090/health`
- Test GraphQL connectivity: `curl http://localhost:8090/test`

## File Layout

```
netdata-proxy-graphql/   # Node.js proxy source and Dockerfile
netdata-forwarder.py     # Netdata polling script
docker-compose.yml      # Service definitions
.env.example             # Configuration template
```

This repository deliberately excludes legacy ClickHouse/Metabase assets to focus solely on exporting Netdata metrics to the GraphQL API.
