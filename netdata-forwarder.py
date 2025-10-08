#!/usr/bin/env python3
"""
Netdata Metrics Forwarder
Pulls metrics from SSH-tunneled Netdata API and forwards to netdata-proxy
"""

import requests
import json
import time
import sys
from datetime import datetime
import logging

# Configuration
NETDATA_URL = "http://localhost:19999"  # SSH tunneled Netdata API on host
NETDATA_LOCAL_URL = "http://localhost:19998"  # Local netdata-parent container
PROXY_URL = "http://localhost:8080"     # netdata-proxy HTTP endpoint (host network)
GRAPHQL_PROXY_URL = "http://localhost:8090"  # GraphQL proxy HTTP endpoint (host network)
INTERVAL_SECONDS = 60                   # How often to pull metrics (must match aggregation window)
MAX_RETRIES = 3

# Specific charts to collect
CHARTS_TO_COLLECT = [
    'system.cpu',      # CPU usage
    'disk_space./'     # Root disk usage
]

# Aggregation types to collect
AGGREGATION_TYPES = ['max', 'average', 'median']

# Time window for aggregations (in seconds, negative for "last X seconds")
TIME_WINDOW = -60

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class NetdataForwarder:
    def __init__(self):
        self.session = requests.Session()
        self.session.timeout = 10

    def get_netdata_info(self):
        """Get Netdata node information"""
        try:
            response = self.session.get(f"{NETDATA_URL}/api/v1/info")
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            logger.warning(f"Failed to get Netdata info: {e}")
            return None

    def get_netdata_metrics(self):
        """Pull all metrics from Netdata API"""
        try:
            url = f"{NETDATA_URL}/api/v1/allmetrics?format=json"
            response = self.session.get(url)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to fetch Netdata metrics: {e}")
            return None

    def get_chart_data(self, hostname, chart_id, aggregation_type, base_netdata_url):
        """Get aggregated data for a specific chart from a specific mirrored host"""
        try:
            # Build URL for host-specific chart data with aggregation
            base_url = f"{base_netdata_url}/host/{hostname}/api/v1/data"
                
            params = {
                'chart': chart_id,
                'group': aggregation_type,
                'after': TIME_WINDOW,
                'points': 1,  # Get single aggregated point
                'format': 'json'
            }
            
            response = self.session.get(base_url, params=params)
            if response.status_code == 200:
                return response.json()
            else:
                logger.warning(f"Failed to get {aggregation_type} data for {chart_id} from {hostname}: {response.status_code}")
                return None
                
        except requests.exceptions.RequestException as e:
            logger.warning(f"Error fetching {aggregation_type} data for {chart_id} from {hostname}: {e}")
            return None

    def get_chart_data_direct(self, chart_id, aggregation_type, netdata_url):
        """Get aggregated data directly from a Netdata instance (not host-specific)"""
        try:
            # Build URL for direct chart data with aggregation
            base_url = f"{netdata_url}/api/v1/data"
                
            params = {
                'chart': chart_id,
                'group': aggregation_type,
                'after': TIME_WINDOW,
                'points': 1,  # Get single aggregated point
                'format': 'json'
            }
            
            response = self.session.get(base_url, params=params)
            if response.status_code == 200:
                return response.json()
            else:
                logger.warning(f"Failed to get {aggregation_type} data for {chart_id} from {netdata_url}: {response.status_code}")
                return None
                
        except requests.exceptions.RequestException as e:
            logger.warning(f"Error fetching {aggregation_type} data for {chart_id} from {netdata_url}: {e}")
            return None

    def get_metrics_for_all_hosts(self):
        """Get aggregated metrics from both mirrored hosts and local container"""
        all_metrics = []
        
        # 1. Collect from mirrored hosts via SSH tunnel (remote)
        logger.info("Collecting from mirrored hosts via SSH tunnel...")
        mirrored_metrics = self.get_mirrored_host_metrics()
        all_metrics.extend(mirrored_metrics)
        
        # 2. Collect from local netdata-parent container (DISABLED)
        # logger.info("Collecting from local netdata-parent container...")
        # local_metrics = self.get_local_host_metrics()
        # all_metrics.extend(local_metrics)
        
        logger.info(f"Collected {len(all_metrics)} total metrics from all sources")
        return all_metrics

    def get_mirrored_host_metrics(self):
        """Get metrics from mirrored hosts via SSH tunnel"""
        all_metrics = []
        
        # Get info about available mirrored hosts
        info = self.get_netdata_info()
        if not info:
            logger.warning("Could not get Netdata info from SSH tunnel - skipping mirrored hosts")
            return []
            
        # Get list of mirrored hosts (children streaming to this parent)
        mirrored_hosts = info.get('mirrored_hosts', [])
        if not mirrored_hosts:
            logger.warning("No mirrored hosts found - no child nodes are streaming to this parent")
            return []
            
        logger.info(f"Found {len(mirrored_hosts)} mirrored hosts: {', '.join(mirrored_hosts)}")
        
        # Collect metrics for each mirrored host
        for hostname in mirrored_hosts:
            host_metrics_collected = 0
            for chart_id in CHARTS_TO_COLLECT:
                for aggregation_type in AGGREGATION_TYPES:
                    try:
                        chart_data = self.get_chart_data(hostname, chart_id, aggregation_type, NETDATA_URL)
                        if chart_data:
                            metrics = self.parse_chart_response(chart_data, hostname, chart_id, aggregation_type)
                            all_metrics.extend(metrics)
                            host_metrics_collected += len(metrics)
                        else:
                            logger.debug(f"No data for {chart_id} ({aggregation_type}) from {hostname}")
                    except Exception as e:
                        logger.warning(f"Failed to collect {chart_id} ({aggregation_type}) from {hostname}: {e}")
                        continue
                        
            if host_metrics_collected > 0:
                logger.info(f"Collected {host_metrics_collected} metrics from mirrored host {hostname}")
            else:
                logger.warning(f"No metrics collected from mirrored host {hostname}")
                
        return all_metrics

    def get_local_host_metrics(self):
        """Get metrics from local netdata-parent container"""
        all_metrics = []
        local_hostname = "netdata-parent-test"  # Container hostname
        
        host_metrics_collected = 0
        for chart_id in CHARTS_TO_COLLECT:
            for aggregation_type in AGGREGATION_TYPES:
                try:
                    chart_data = self.get_chart_data_direct(chart_id, aggregation_type, NETDATA_LOCAL_URL)
                    if chart_data:
                        metrics = self.parse_chart_response(chart_data, local_hostname, chart_id, aggregation_type)
                        all_metrics.extend(metrics)
                        host_metrics_collected += len(metrics)
                    else:
                        logger.debug(f"No data for {chart_id} ({aggregation_type}) from local container")
                except Exception as e:
                    logger.warning(f"Failed to collect {chart_id} ({aggregation_type}) from local container: {e}")
                    continue
                    
        if host_metrics_collected > 0:
            logger.info(f"Collected {host_metrics_collected} metrics from local container")
        else:
            logger.warning(f"No metrics collected from local container")
            
        return all_metrics

    def parse_chart_response(self, chart_data, hostname, chart_id, aggregation_type):
        """Parse Netdata /api/v1/data response into individual metrics"""
        metrics = []
        
        if not chart_data or not isinstance(chart_data, dict):
            return metrics
            
        # Get labels and data from actual API response format
        labels = chart_data.get('labels', [])
        data_points = chart_data.get('data', [])
        
        if not labels or not data_points:
            logger.warning(f"No data found for {chart_id} from {hostname} ({aggregation_type})")
            return metrics
            
        # First label is "time", rest are dimension names
        if len(labels) < 2:
            logger.warning(f"Invalid labels format for {chart_id} from {hostname} ({aggregation_type})")
            return metrics
            
        dimensions = labels[1:]  # Skip "time" label
        
        # Use the first (and should be only) data point since we requested points=1
        if len(data_points) > 0 and len(data_points[0]) > 1:
            timestamp = data_points[0][0] if len(data_points[0]) > 0 else int(time.time())
            values = data_points[0][1:] if len(data_points[0]) > 1 else []
            
            # Create metric for each dimension
            for i, dimension in enumerate(dimensions):
                if i < len(values) and values[i] is not None:
                    try:
                        value = float(values[i])
                        
                        metric = {
                            'timestamp': timestamp,
                            'hostname': hostname,
                            'chart_id': chart_id,
                            'chart_name': chart_id.replace('_', ' ').title(),  # Generate readable name
                            'id': dimension,  # proxy expects 'id' not 'dimension'
                            'value': value,
                            'units': self._get_chart_units(chart_id),  # Helper method for units
                            'family': chart_id.split('.')[0] if '.' in chart_id else '',
                            'context': chart_id,
                            'chart_type': 'line',
                            '_aggregation_type': aggregation_type  # Track aggregation type
                        }
                        metrics.append(metric)
                        
                    except (ValueError, TypeError) as e:
                        logger.warning(f"Invalid value for {dimension} in {chart_id}: {values[i]}")
                        
        return metrics

    def _get_chart_units(self, chart_id):
        """Get appropriate units for common chart types"""
        if 'cpu' in chart_id.lower():
            return 'percentage'
        elif 'disk_space' in chart_id.lower():
            return 'GB'
        elif 'memory' in chart_id.lower():
            return 'MB'
        else:
            return ''

    def transform_metrics(self, all_host_data):
        """Transform Netdata API format to netdata-proxy format"""
        metrics = []
        
        # Handle different possible formats from Netdata API
        if not all_host_data:
            return metrics
        
        # Process data for each host
        for hostname, host_data in all_host_data.items():
            if not isinstance(host_data, dict):
                continue
                
            # Process charts for this host
            for chart_id, chart_data in host_data.items():
                if not isinstance(chart_data, dict):
                    continue
                    
                # Extract chart metadata
                chart_name = chart_data.get('name', chart_id)
                chart_type = chart_data.get('chart_type', 'line')  
                units = chart_data.get('units', '')
                family = chart_data.get('family', '')
                context = chart_data.get('context', chart_id)
                
                # Use Netdata's actual timestamp, not current time
                chart_timestamp = chart_data.get('last_updated')
                if chart_timestamp:
                    # Netdata provides Unix timestamp, convert to seconds
                    metric_timestamp = int(chart_timestamp)
                else:
                    # Fallback to current time if no timestamp available
                    metric_timestamp = int(time.time())
                
                # Use hostname from structure, or override if chart has _hostname tag
                actual_hostname = chart_data.get('_hostname', hostname)
                    
                # Extract dimensions (actual metric values)
                dimensions = chart_data.get('dimensions', {})
                if not dimensions:
                    continue
                    
                for dimension_id, dimension_data in dimensions.items():
                    if not isinstance(dimension_data, dict):
                        continue
                        
                    value = dimension_data.get('value')
                    if value is None:
                        continue
                        
                    try:
                        value = float(value)
                    except (ValueError, TypeError):
                        continue
                    
                    # Create metric in format expected by netdata-proxy
                    # The proxy expects 'id' field for dimension name (see processNetdataMetric)
                    metric = {
                        'timestamp': metric_timestamp,
                        'hostname': actual_hostname,
                        'chart_id': chart_id,
                        'chart_name': chart_name,
                        'id': dimension_id,  # proxy expects 'id' not 'dimension' 
                        'value': value,
                        'units': units,
                        'family': family,
                        'context': context,
                        'chart_type': chart_type
                    }
                    metrics.append(metric)
        
        return metrics

    def send_to_proxy(self, metrics, aggregation_type='average'):
        """Send transformed metrics to netdata-proxy with specified aggregation type"""
        if not metrics:
            logger.warning("No metrics to send")
            return False
            
        try:
            # Choose endpoint based on aggregation type
            if aggregation_type == 'max':
                endpoint = f"{PROXY_URL}/max"
            elif aggregation_type == 'average':
                endpoint = f"{PROXY_URL}/avg"  
            elif aggregation_type == 'median':
                endpoint = f"{PROXY_URL}/median"  # Will add this endpoint to proxy
            else:
                endpoint = PROXY_URL  # Default endpoint (defaults to average)
            
            # Send as JSON array (format supported by netdata-proxy)
            response = self.session.post(
                endpoint,
                json=metrics,
                headers={'Content-Type': 'application/json'}
            )
            response.raise_for_status()
            
            result = response.json()
            logger.info(f"Successfully sent {len(metrics)} {aggregation_type} metrics to ClickHouse proxy. "
                       f"Proxy response: {result.get('status')} - "
                       f"{result.get('rows_inserted', 0)} rows inserted in "
                       f"{result.get('processing_time', 0)}ms")
            return True
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to send {aggregation_type} metrics to ClickHouse proxy: {e}")
            return False

    def send_to_graphql_proxy(self, metrics, aggregation_type='average'):
        """Send transformed metrics to GraphQL proxy with specified aggregation type"""
        if not metrics:
            logger.warning("No metrics to send to GraphQL proxy")
            return False
            
        try:
            # Choose endpoint based on aggregation type
            if aggregation_type == 'max':
                endpoint = f"{GRAPHQL_PROXY_URL}/max"
            elif aggregation_type == 'average':
                endpoint = f"{GRAPHQL_PROXY_URL}/avg"  
            elif aggregation_type == 'median':
                endpoint = f"{GRAPHQL_PROXY_URL}/median"
            else:
                endpoint = GRAPHQL_PROXY_URL  # Default endpoint (defaults to average)
            
            # Send as JSON array (format supported by GraphQL proxy)
            response = self.session.post(
                endpoint,
                json=metrics,
                headers={'Content-Type': 'application/json'}
            )
            response.raise_for_status()
            
            result = response.json()
            logger.info(f"Successfully sent {len(metrics)} {aggregation_type} metrics to GraphQL proxy. "
                       f"Proxy response: {result.get('status')} - "
                       f"{result.get('records_sent', 0)} records sent in "
                       f"{result.get('processing_time', 0)}ms")
            return True
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to send {aggregation_type} metrics to GraphQL proxy: {e}")
            return False

    def health_check(self):
        """Check if Netdata and both proxies are accessible"""
        try:
            # Check Netdata API
            netdata_response = self.session.get(f"{NETDATA_URL}/api/v1/info")
            netdata_response.raise_for_status()
            logger.info(f"Netdata API accessible - Version: {netdata_response.json().get('version', 'unknown')}")
            
            # Check ClickHouse proxy health
            proxy_response = self.session.get(f"{PROXY_URL}/health")
            proxy_response.raise_for_status()
            logger.info(f"ClickHouse proxy accessible - Status: {proxy_response.json().get('status')}")
            
            # Check GraphQL proxy health
            graphql_proxy_response = self.session.get(f"{GRAPHQL_PROXY_URL}/health")
            graphql_proxy_response.raise_for_status()
            graphql_status = graphql_proxy_response.json()
            logger.info(f"GraphQL proxy accessible - Status: {graphql_status.get('status')}, Auth: {graphql_status.get('auth_configured')}")
            
            return True
        except requests.exceptions.RequestException as e:
            logger.error(f"Health check failed: {e}")
            return False

    def run_once(self):
        """Pull metrics once and forward to proxy"""
        logger.info("Pulling aggregated metrics from mirrored hosts...")
        
        # Get metrics directly as list (already aggregated by type)
        metrics_by_aggregation = {}
        all_host_data = self.get_metrics_for_all_hosts()
        
        if not all_host_data:
            logger.error("No data received from mirrored hosts")
            return False
            
        # Group metrics by aggregation type for separate sending
        for metric in all_host_data:
            # We need to extract aggregation type from our collection process
            # Since we collected each metric with specific aggregation, we need to group them
            pass
            
        # Since we're now collecting pre-aggregated data, send directly
        logger.info(f"Collected {len(all_host_data)} pre-aggregated metrics")
        
        if not all_host_data:
            logger.warning("No valid metrics collected")
            return False
            
        # Group metrics by aggregation type and send to both proxies
        # Send available data even if some aggregation types are missing
        total_sent_clickhouse = 0
        total_sent_graphql = 0
        failed_sends = 0
        
        for aggregation_type in AGGREGATION_TYPES:
            # Filter metrics for this aggregation type
            type_metrics = [m for m in all_host_data if m.get('_aggregation_type') == aggregation_type]
            if type_metrics:
                # Remove the internal aggregation marker before sending
                clean_metrics = []
                for metric in type_metrics:
                    clean_metric = {k: v for k, v in metric.items() if k != '_aggregation_type'}
                    clean_metrics.append(clean_metric)
                
                # Send to ClickHouse proxy
                try:
                    clickhouse_success = self.send_to_proxy(clean_metrics, aggregation_type)
                    if clickhouse_success:
                        total_sent_clickhouse += len(clean_metrics)
                        logger.info(f"Successfully sent {len(clean_metrics)} metrics with {aggregation_type} aggregation to ClickHouse")
                    else:
                        failed_sends += 1
                        logger.error(f"Failed to send {len(clean_metrics)} metrics with {aggregation_type} aggregation to ClickHouse")
                except Exception as e:
                    failed_sends += 1
                    logger.error(f"Exception sending {aggregation_type} metrics to ClickHouse: {e}")
                
                # Send to GraphQL proxy
                try:
                    graphql_success = self.send_to_graphql_proxy(clean_metrics, aggregation_type)
                    if graphql_success:
                        total_sent_graphql += len(clean_metrics)
                        logger.info(f"Successfully sent {len(clean_metrics)} metrics with {aggregation_type} aggregation to GraphQL")
                    else:
                        failed_sends += 1
                        logger.error(f"Failed to send {len(clean_metrics)} metrics with {aggregation_type} aggregation to GraphQL")
                except Exception as e:
                    failed_sends += 1
                    logger.error(f"Exception sending {aggregation_type} metrics to GraphQL: {e}")
            else:
                logger.warning(f"No metrics found for {aggregation_type} aggregation - no mirrored hosts provided this data")
        
        # Consider success if we sent any data to either proxy, even if some failed
        total_sent = total_sent_clickhouse + total_sent_graphql
        if total_sent > 0:
            logger.info(f"Overall success: sent {total_sent_clickhouse} metrics to ClickHouse, "
                       f"{total_sent_graphql} metrics to GraphQL ({failed_sends} sends failed)")
            return True
        else:
            logger.error("No metrics were successfully sent to any proxy")
            return False

    def run_continuous(self):
        """Run continuously with specified interval"""
        logger.info(f"Starting continuous mode - pulling every {INTERVAL_SECONDS} seconds")
        
        while True:
            try:
                success = self.run_once()
                if not success:
                    logger.warning("Failed to process metrics this cycle")
                    
            except KeyboardInterrupt:
                logger.info("Shutting down...")
                break
            except Exception as e:
                logger.error(f"Unexpected error: {e}")
                
            time.sleep(INTERVAL_SECONDS)

def main():
    forwarder = NetdataForwarder()
    
    if len(sys.argv) > 1 and sys.argv[1] == '--once':
        # Run once and exit
        success = forwarder.run_once()
        sys.exit(0 if success else 1)
    else:
        # Run continuously
        forwarder.run_continuous()

if __name__ == "__main__":
    main()
