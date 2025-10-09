# CLAUDE.md

Guidance for coding agents working in this repository.

## Overview

This project exports metrics from Netdata to a GraphQL API. The stack now consists of:

- **GraphQL proxy** (`netdata-proxy-graphql/app.js`): Express server that accepts Netdata JSON (HTTP or TCP) and issues GraphQL mutations.
  It batches metrics per request using GraphQL aliases to minimize API calls.
- **Forwarder** (`netdata-forwarder.py`): Python script that polls a Netdata endpoint (typically tunneled to `localhost:19999`) and posts metrics to the proxy.
- **Docker Compose**: Builds the proxy container and runs the forwarder on a schedule.

## Architecture

```
Netdata (localhost:19999) ──> Forwarder ──> GraphQL Proxy ──> GraphQL API
```

## Key Paths

- Proxy sources: `netdata-proxy-graphql/`
- Python forwarder: `netdata-forwarder.py`
- Compose definition: `docker-compose.yml`
- Environment template: `.env.example`

## Development Commands

### Docker Stack
```bash
docker-compose up -d              # start proxy + forwarder
docker-compose logs -f netdata-graphql-proxy
docker-compose logs -f netdata-forwarder
docker-compose down               # stop everything
```

### Proxy Development
```bash
cd netdata-proxy-graphql
npm install
npm run dev  # nodemon
npm start
```

### Forwarder Development
```bash
python3 netdata-forwarder.py --once  # run single poll
python3 netdata-forwarder.py         # continuous
```

## Configuration

Set environment variables in `.env` (see `.env.example`):

- `API_KEY` – GraphQL API authentication token (required)
- `GRAPHQL_ENDPOINT`, `SHEET_ID` – GraphQL destination details
- `NETDATA_URL` – Netdata base URL (default `http://host.docker.internal:19999`)
- `CHARTS`, `AGGREGATION_TYPES` – Metrics collected by the forwarder
- `INTERVAL_SECONDS`, `REQUEST_TIMEOUT`, `FORWARDER_LOG_LEVEL` – Forwarder runtime behaviour
- `GRAPHQL_BATCH_SIZE` – Number of metrics sent per GraphQL mutation batch (default 20)

The forwarder resolves the host machine via `host.docker.internal`; override `NETDATA_URL` if this alias is not available on your platform.

## Health Checks

- Proxy: `curl http://localhost:8090/health`
- Proxy test mutation: `curl http://localhost:8090/test`
- Forwarder logs show successful batch transfers and errors.

## Gotchas

- Ensure the SSH tunnel to Netdata (`localhost:19999`) is active before running the forwarder.
- The proxy requires `API_KEY` to be present; missing keys cause HTTP 500 responses when sending metrics.
- Aggregation names must match Netdata `group` values (`average`, `max`, `median`).
