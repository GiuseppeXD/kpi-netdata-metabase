#!/usr/bin/env python3
"""Netdata to GraphQL forwarder.

This script polls a Netdata instance exposed locally (for example through
an SSH tunnel) and forwards selected metrics to the GraphQL proxy service.
"""

from __future__ import annotations

import logging
import os
import sys
import time
from typing import Any, Dict, Iterable, List, Optional
from urllib.parse import quote

import requests

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=LOG_LEVEL,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

AGGREGATION_SETTINGS = {
    "average": {"group": "average", "endpoint": "avg", "label": "AVG"},
    "avg": {"group": "average", "endpoint": "avg", "label": "AVG"},
    "max": {"group": "max", "endpoint": "max", "label": "MAX"},
    "maximum": {"group": "max", "endpoint": "max", "label": "MAX"},
    "median": {"group": "median", "endpoint": "median", "label": "MEDIAN"},
}


class Config:
    """Runtime configuration read from environment variables."""

    def __init__(self) -> None:
        self.netdata_url = os.getenv("NETDATA_URL", "http://localhost:19999").rstrip("/")
        self.graphql_proxy_url = os.getenv("GRAPHQL_PROXY_URL", "http://localhost:8090").rstrip("/")
        self.interval_seconds = int(os.getenv("INTERVAL_SECONDS", str(60 * 60)))
        self.request_timeout = int(os.getenv("REQUEST_TIMEOUT", "120"))
        self.skip_tls_verify = os.getenv("NETDATA_SKIP_TLS_VERIFY", "false").lower() == "true"
        self.netdata_hosts = self._split_list(os.getenv("NETDATA_HOSTS", ""))
        self.chart_filter = os.getenv("CHART_FILTER", "disk_space").strip()
        raw_chart_ids = self._split_list(os.getenv("CHARTS", "system.cpu,disk_space./"))
        if raw_chart_ids:
            self.chart_ids = raw_chart_ids
        else:
            logger.warning("No chart IDs configured; defaulting to 'disk_space./'")
            self.chart_ids = ["disk_space./"]
        self.aggregations = self._parse_aggregations(os.getenv("AGGREGATION_TYPES", "average,median,max"))

    @staticmethod
    def _split_list(raw: str) -> List[str]:
        return [item.strip() for item in raw.split(",") if item.strip()]

    def filter_chart_ids(self, chart_ids: Iterable[str]) -> List[str]:
        if self.chart_filter and self.chart_filter != "*":
            return [chart_id for chart_id in chart_ids if self.chart_filter in chart_id]
        return list(chart_ids)

    def _parse_aggregations(self, raw: str) -> List[str]:
        parsed: List[str] = []
        for item in self._split_list(raw):
            key = item.lower()
            if key not in AGGREGATION_SETTINGS:
                logger.warning("Unsupported aggregation '%s' - skipping", item)
                continue
            canonical = AGGREGATION_SETTINGS[key]["group"]
            if canonical not in parsed:
                parsed.append(canonical)
        if not parsed:
            parsed.append("average")
        return parsed


class NetdataForwarder:
    def __init__(self, config: Config) -> None:
        self.config = config
        self.session = requests.Session()
        self.session.verify = not config.skip_tls_verify
        self.chart_catalog_cache: Dict[str, Dict[str, Any]] = {}

    def get_netdata_info(self) -> Optional[Dict[str, Any]]:
        try:
            response = self.session.get(
                f"{self.config.netdata_url}/api/v1/info",
                timeout=self.config.request_timeout,
            )
            response.raise_for_status()
            return response.json()
        except requests.RequestException as exc:
            logger.warning("Unable to fetch Netdata info: %s", exc)
            return None

    def get_chart_data(self, hostname: str, chart_id: str, aggregation: str) -> Optional[Dict[str, Any]]:
        params = {
            "chart": chart_id,
            "group": aggregation,
            "after": -self.config.interval_seconds,
            "points": 1,
            "format": "json",
        }
        try:
            fallback_attempted = False
            urls_to_try: List[str] = []
            if hostname:
                host_slug = quote(hostname.strip())
                urls_to_try.append(f"{self.config.netdata_url}/host/{host_slug}/api/v1/data")
            urls_to_try.append(f"{self.config.netdata_url}/api/v1/data")

            for idx, url in enumerate(urls_to_try):
                effective_params = dict(params)
                if hostname and idx > 0:
                    effective_params["host"] = hostname
                response = self.session.get(url, params=effective_params, timeout=self.config.request_timeout)
                if response.status_code == 404 and hostname:
                    if idx == 0:
                        fallback_attempted = True
                        continue
                    logger.warning("Host '%s' not found for chart '%s'", hostname, chart_id)
                    return None
                response.raise_for_status()
                if fallback_attempted:
                    logger.debug(
                        "Successfully fetched chart '%s' for host '%s' via fallback endpoint",
                        chart_id,
                        hostname,
                    )
                return response.json()
            return None
        except requests.RequestException as exc:
            logger.warning(
                "Failed to get %s aggregation for %s (host=%s): %s",
                aggregation,
                chart_id,
                hostname or "self",
                exc,
            )
            return None

    def parse_chart_response(
        self,
        payload: Dict[str, Any],
        hostname: str,
        chart_id: str,
        aggregation: str,
    ) -> List[Dict[str, Any]]:
        metrics: List[Dict[str, Any]] = []
        labels = payload.get("labels") or []
        data_points = payload.get("data") or []
        if len(labels) < 2 or not data_points:
            return metrics

        timestamp = int(data_points[0][0]) if data_points[0] else int(time.time())
        values = data_points[0][1:]
        dimensions = labels[1:]

        for dimension, value in zip(dimensions, values):
            if value is None:
                continue
            try:
                numeric = float(value)
            except (TypeError, ValueError):
                continue
            metric = {
                "timestamp": timestamp,
                "hostname": hostname,
                "chart_id": chart_id,
                "chart_name": chart_id.replace("_", " ").title(),
                "id": dimension,
                "value": numeric,
                "units": self._guess_units(chart_id),
                "context": chart_id,
                "chart_type": "line",
                "aggregation": aggregation,
            }
            metrics.append(metric)
        return metrics

    @staticmethod
    def _guess_units(chart_id: str) -> str:
        lowered = chart_id.lower()
        if "cpu" in lowered:
            return "percentage"
        if "disk" in lowered:
            return "GB"
        if "memory" in lowered:
            return "MB"
        return ""

    def collect_metrics(self) -> List[Dict[str, Any]]:
        info = self.get_netdata_info()
        default_hostname = info.get("hostname") if isinstance(info, dict) else "netdata"
        mirrored_hosts = info.get("mirrored_hosts", []) if isinstance(info, dict) else []

        host_targets: Iterable[str]
        if self.config.netdata_hosts:
            host_targets = self._dedupe(self.config.netdata_hosts)
        elif mirrored_hosts:
            host_targets = self._normalize_hosts(mirrored_hosts)
        else:
            host_targets = []

        collected: List[Dict[str, Any]] = []
        per_host_counts: Dict[str, int] = {}

        if host_targets:
            logger.info("Collecting metrics for hosts: %s", ", ".join(host_targets))
            for host in host_targets:
                host_metrics = self._collect_for_host(host)
                collected.extend(host_metrics)
                per_host_counts[host] = len(host_metrics)
                if host_metrics:
                    logger.info("Collected %d metrics for host '%s'", len(host_metrics), host)
                else:
                    logger.warning("No metrics collected for host '%s'", host)
        else:
            logger.info("Collecting metrics directly from %s", self.config.netdata_url)
            host_label = default_hostname or "netdata"
            host_metrics = self._collect_for_host(None, override_hostname=default_hostname)
            collected.extend(host_metrics)
            per_host_counts[host_label] = len(host_metrics)
            if host_metrics:
                logger.info("Collected %d metrics for host '%s'", len(host_metrics), host_label)
            else:
                logger.warning("No metrics collected for host '%s'", host_label)

        missing_hosts = [host for host, count in per_host_counts.items() if count == 0]
        if missing_hosts:
            logger.warning("Unable to collect metrics for %d host(s): %s", len(missing_hosts), ", ".join(missing_hosts))
        elif per_host_counts:
            logger.info("Successfully collected metrics for all %d host(s)", len(per_host_counts))

        logger.info("Collected %d metrics in total", len(collected))
        return collected

    def _collect_for_host(self, host: Optional[str], override_hostname: Optional[str] = None) -> List[Dict[str, Any]]:
        host_label = host or override_hostname or "netdata"
        catalog = self._fetch_chart_catalog(host)
        chart_ids = self._resolve_chart_ids_for_host(host, catalog)
        volumes = self._extract_volume_labels(chart_ids, catalog)
        if volumes:
            logger.info(
                "Discovered %d volume chart(s) for host '%s': %s",
                len(volumes),
                host_label,
                ", ".join(sorted(volumes.values())),
            )
        elif any(chart_id.startswith("disk_space") for chart_id in chart_ids):
            logger.warning("No volume metadata found for host '%s'", host_label)

        metrics: List[Dict[str, Any]] = []
        for chart_id in chart_ids:
            for aggregation in self.config.aggregations:
                payload = self.get_chart_data(host or "", chart_id, aggregation)
                if not payload:
                    continue
                display_host = self._format_hostname_for_chart(host_label, chart_id, volumes)
                metrics.extend(self.parse_chart_response(payload, display_host, chart_id, aggregation))
        return metrics

    @staticmethod
    def _dedupe(items: Iterable[str]) -> List[str]:
        seen: dict[str, None] = {}
        for item in items:
            if not item:
                continue
            normalized = item.strip()
            if not normalized:
                continue
            if normalized not in seen:
                seen[normalized] = None
        return list(seen.keys())

    def _normalize_hosts(self, hosts: Iterable[Any]) -> List[str]:
        normalized: List[str] = []
        for host in hosts:
            candidate: Optional[str] = None
            if isinstance(host, str):
                candidate = host
            elif isinstance(host, dict):
                candidate = (
                    host.get("hostname")
                    or host.get("name")
                    or host.get("machine_guid")
                    or host.get("id")
                )
            if candidate:
                normalized.append(candidate)
        return self._dedupe(normalized)

    def _fetch_chart_catalog(self, host: Optional[str]) -> Dict[str, Any]:
        cache_key = host or "__self__"
        if cache_key in self.chart_catalog_cache:
            return self.chart_catalog_cache[cache_key]

        urls_to_try: List[str] = []
        fallback_attempted = False
        if host:
            host_slug = quote(host.strip())
            urls_to_try.append(f"{self.config.netdata_url}/host/{host_slug}/api/v1/charts")
        urls_to_try.append(f"{self.config.netdata_url}/api/v1/charts")

        for idx, url in enumerate(urls_to_try):
            params: Dict[str, Any] = {}
            if host and idx > 0:
                params["host"] = host
            try:
                response = self.session.get(url, params=params or None, timeout=self.config.request_timeout)
                if response.status_code == 404 and host:
                    if idx == 0:
                        fallback_attempted = True
                        continue
                    logger.debug("Charts for host '%s' not found (HTTP 404)", host)
                    break
                response.raise_for_status()
                data = response.json()
                chart_map = self._extract_chart_map(data)
                if chart_map:
                    if fallback_attempted:
                        logger.debug("Discovered charts for host '%s' via fallback endpoint", host)
                    self.chart_catalog_cache[cache_key] = chart_map
                    return chart_map
                logger.debug(
                    "Unexpected chart catalog format for host '%s': %s",
                    host or "self",
                    type(data).__name__,
                )
            except requests.RequestException as exc:
                logger.debug(
                    "Failed to fetch chart catalog for host '%s' from %s: %s",
                    host or "self",
                    url,
                    exc,
                )
        self.chart_catalog_cache[cache_key] = {}
        return {}

    @staticmethod
    def _extract_chart_map(payload: Any) -> Dict[str, Any]:
        if not isinstance(payload, dict):
            return {}
        if "charts" in payload and isinstance(payload["charts"], dict):
            return payload["charts"]
        # Some Netdata versions use "data" wrapper
        if "data" in payload and isinstance(payload["data"], dict):
            return payload["data"]
        # Heuristic: if values look like chart definitions, accept payload as-is
        sample = next(iter(payload.values()), None)
        if isinstance(sample, dict) and any(key in sample for key in ("chart", "id", "name", "context")):
            return payload
        return {}

    def _resolve_chart_ids_for_host(self, host: Optional[str], catalog: Dict[str, Any]) -> List[str]:
        discovered_ids = list(catalog.keys()) if catalog else []
        filtered_discovered = self.config.filter_chart_ids(discovered_ids)
        combined = list(self.config.chart_ids)
        for chart_id in filtered_discovered:
            if chart_id not in combined:
                combined.append(chart_id)
        if not combined:
            combined = ["disk_space./"]
        return combined

    def _format_hostname_for_chart(
        self,
        base_host_label: str,
        chart_id: str,
        volumes: Optional[Dict[str, str]],
    ) -> str:
        if volumes and chart_id in volumes:
            volume_label = volumes[chart_id]
            if volume_label and volume_label != "root":
                return f"{base_host_label}/{volume_label}"
        return base_host_label

    def _extract_volume_labels(
        self,
        chart_ids: Iterable[str],
        catalog: Optional[Dict[str, Any]],
    ) -> Dict[str, str]:
        volumes: Dict[str, str] = {}
        if not isinstance(catalog, dict):
            catalog = {}
        for chart_id in chart_ids:
            if not chart_id.startswith("disk_space"):
                continue
            chart_info = catalog.get(chart_id, {})
            volume_name = self._derive_volume_name(chart_id, chart_info)
            sanitized = self._sanitize_volume_label(volume_name)
            if sanitized:
                volumes[chart_id] = sanitized
        return volumes

    def _derive_volume_name(self, chart_id: str, chart_info: Any) -> str:
        candidates: List[str] = []
        if isinstance(chart_info, dict):
            for key in ("mount_point", "path", "name", "chart", "id", "context", "title", "family"):
                value = chart_info.get(key)
                if isinstance(value, str) and value.strip():
                    candidates.append(value.strip())
            labels = chart_info.get("chart_labels")
            if isinstance(labels, dict):
                for key in ("mount_point", "mountpoint", "mount", "path", "volume", "fs", "filesystem", "family"):
                    value = labels.get(key)
                    if isinstance(value, str) and value.strip():
                        candidates.append(value.strip())
        candidates.append(chart_id)

        for candidate in candidates:
            normalized = self._normalize_volume_identifier(candidate)
            if normalized:
                return normalized
        return chart_id

    @staticmethod
    def _normalize_volume_identifier(value: str) -> str:
        if not value:
            return ""
        raw = value.strip()
        if not raw:
            return ""
        if raw.startswith("disk_space."):
            raw = raw.split("disk_space.", 1)[1]
        normalized = NetdataForwarder._normalize_volume_suffix(raw)
        if normalized:
            return normalized
        return NetdataForwarder._normalize_volume_suffix(NetdataForwarder._chart_root_path_from_metadata(value))

    @staticmethod
    def _chart_root_path_from_metadata(raw: str) -> str:
        # Attempt to recover path hints embedded in raw metadata strings.
        if not raw:
            return ""
        if "mount_point" in raw:
            return raw.split("mount_point", 1)[-1]
        return raw

    @staticmethod
    def _normalize_volume_suffix(suffix: str) -> str:
        cleaned = suffix.strip()
        if not cleaned:
            return "/"
        if cleaned in {"/", "./"}:
            return "/"
        if cleaned in {"_", "._"}:
            return "/"
        if cleaned.startswith("./"):
            remainder = cleaned[1:].strip()
            return remainder or "/"
        if cleaned.startswith("/"):
            return cleaned
        if cleaned.startswith("_"):
            body = cleaned[1:]
            path = body.replace("_", "/").strip()
            return f"/{path}" if path else "/"
        if cleaned.startswith("."):
            body = cleaned[1:]
            return f"/{body}" if body else "/"
        return cleaned

    @staticmethod
    def _sanitize_volume_label(volume_name: str) -> str:
        if not volume_name:
            return ""
        trimmed = volume_name.strip()
        if not trimmed or trimmed == "/":
            return "root"
        if trimmed.startswith("mount_point="):
            trimmed = trimmed.split("=", 1)[1]
        if trimmed.startswith("/" ) and "mount_point" in volume_name:
            trimmed = trimmed.split("mount_point", 1)[0].strip()
        without_slash = trimmed.lstrip("/")
        return without_slash or "root"

    def send_to_graphql(self, metrics: List[Dict[str, Any]], aggregation: str) -> bool:
        settings = next(
            (details for details in AGGREGATION_SETTINGS.values() if details["group"] == aggregation),
            None,
        )
        if not settings:
            logger.error("Unknown aggregation '%s'", aggregation)
            return False

        cleaned = []
        for metric in metrics:
            metric_copy = dict(metric)
            metric_copy.pop("aggregation", None)
            cleaned.append(metric_copy)

        if not cleaned:
            logger.warning("No metrics to send for aggregation '%s'", aggregation)
            return False

        endpoint_path = settings["endpoint"]
        url = f"{self.config.graphql_proxy_url}/{endpoint_path}" if endpoint_path else self.config.graphql_proxy_url

        try:
            response = self.session.post(
                url,
                json=cleaned,
                timeout=self.config.request_timeout,
            )
            response.raise_for_status()
            logger.info(
                "Forwarded %d metrics to %s (%s)",
                len(cleaned),
                url,
                response.json().get("status", "unknown"),
            )
            return True
        except requests.RequestException as exc:
            logger.error(
                "Failed to forward %d metrics to %s: %s",
                len(cleaned),
                url,
                exc,
            )
            if exc.response is not None:
                logger.debug("GraphQL proxy response: %s", exc.response.text)
            return False

    def run_once(self) -> bool:
        metrics = self.collect_metrics()
        if not metrics:
            logger.warning("No metrics collected during this cycle")
            return False

        grouped: Dict[str, List[Dict[str, Any]]] = {}
        for metric in metrics:
            key = metric.get("aggregation", "average")
            grouped.setdefault(key, []).append(metric)

        success = False
        for aggregation, items in grouped.items():
            if self.send_to_graphql(items, aggregation):
                success = True
        return success

    def run_continuous(self) -> None:
        logger.info("Starting Netdata forwarder (interval: %ss)", self.config.interval_seconds)
        while True:
            try:
                self.run_once()
            except KeyboardInterrupt:
                logger.info("Stopping forwarder")
                break
            except Exception as exc:  # noqa: BLE001
                logger.exception("Unexpected error: %s", exc)
            time.sleep(self.config.interval_seconds)


def main() -> int:
    config = Config()
    forwarder = NetdataForwarder(config)
    if len(sys.argv) > 1 and sys.argv[1] in {"--once", "once"}:
        return 0 if forwarder.run_once() else 1
    forwarder.run_continuous()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
