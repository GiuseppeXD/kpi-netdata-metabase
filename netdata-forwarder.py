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
        self.interval_seconds = int(os.getenv("INTERVAL_SECONDS", "60"))
        self.request_timeout = int(os.getenv("REQUEST_TIMEOUT", "120"))
        self.skip_tls_verify = os.getenv("NETDATA_SKIP_TLS_VERIFY", "false").lower() == "true"
        self.netdata_hosts = self._split_list(os.getenv("NETDATA_HOSTS", ""))
        self.chart_ids = [
            chart_id
            for chart_id in self._split_list(os.getenv("CHARTS", "system.cpu,disk_space./"))
            if "disk_space" in chart_id
        ]
        if not self.chart_ids:
            # Ensure we always collect at least the root disk when filtering.
            self.chart_ids = ["disk_space./"]
        self.aggregations = self._parse_aggregations(os.getenv("AGGREGATION_TYPES", "average,median,max"))

    @staticmethod
    def _split_list(raw: str) -> List[str]:
        return [item.strip() for item in raw.split(",") if item.strip()]

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
            if hostname:
                url = f"{self.config.netdata_url}/host/{hostname}/api/v1/data"
            else:
                url = f"{self.config.netdata_url}/api/v1/data"
            response = self.session.get(url, params=params, timeout=self.config.request_timeout)
            if response.status_code == 404 and hostname:
                logger.warning("Host '%s' not found for chart '%s'", hostname, chart_id)
                return None
            response.raise_for_status()
            return response.json()
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
            host_targets = self.config.netdata_hosts
        elif mirrored_hosts:
            host_targets = mirrored_hosts
        else:
            host_targets = []

        collected: List[Dict[str, Any]] = []

        if host_targets:
            logger.info("Collecting metrics for hosts: %s", ", ".join(host_targets))
            for host in host_targets:
                collected.extend(self._collect_for_host(host))
        else:
            logger.info("Collecting metrics directly from %s", self.config.netdata_url)
            collected.extend(self._collect_for_host(None, override_hostname=default_hostname))

        logger.info("Collected %d metrics", len(collected))
        return collected

    def _collect_for_host(self, host: Optional[str], override_hostname: Optional[str] = None) -> List[Dict[str, Any]]:
        host_label = host or override_hostname or "netdata"
        metrics: List[Dict[str, Any]] = []
        for chart_id in self.config.chart_ids:
            for aggregation in self.config.aggregations:
                payload = self.get_chart_data(host or "", chart_id, aggregation)
                if not payload:
                    continue
                metrics.extend(self.parse_chart_response(payload, host_label, chart_id, aggregation))
        return metrics

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
