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
NETDATA_URL = "http://localhost:19999"  # SSH tunneled Netdata API
PROXY_URL = "http://localhost:8081"     # netdata-proxy HTTP endpoint
INTERVAL_SECONDS = 30                   # How often to pull metrics
MAX_RETRIES = 3

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

    def get_metrics_for_all_hosts(self):
        """Get metrics from all available hosts (parent + children)"""
        all_metrics = {}
        
        # Get info about available hosts
        info = self.get_netdata_info()
        if not info:
            logger.warning("Could not get Netdata info, trying default endpoint")
            return self.get_netdata_metrics()
            
        # Get list of mirrored hosts (children)
        mirrored_hosts = info.get('mirrored_hosts', [])
        logger.info(f"Found {len(mirrored_hosts)} hosts: {', '.join(mirrored_hosts)}")
        
        # Get metrics for each host using the correct API pattern
        for hostname in mirrored_hosts:
            try:
                # Use the correct host-specific endpoint pattern:
                # http://localhost:19999/host/HOSTNAME/api/v1/allmetrics?format=json
                url = f"{NETDATA_URL}/host/{hostname}/api/v1/allmetrics?format=json"
                    
                response = self.session.get(url)
                if response.status_code == 200:
                    host_data = response.json()
                    # Tag each metric with the correct hostname
                    for chart_id, chart_data in host_data.items():
                        if isinstance(chart_data, dict):
                            chart_data['_hostname'] = hostname  # Add hostname tag
                    all_metrics[hostname] = host_data
                    logger.info(f"Got {len(host_data)} charts from {hostname}")
                else:
                    logger.warning(f"Failed to get metrics from {hostname}: {response.status_code}")
                    
            except requests.exceptions.RequestException as e:
                logger.warning(f"Error fetching metrics from {hostname}: {e}")
                
        # If we got no host-specific data, fall back to default
        if not all_metrics:
            logger.info("No host-specific data found, using default endpoint")
            default_data = self.get_netdata_metrics()
            if default_data:
                all_metrics['netdata-parent'] = default_data
                
        return all_metrics

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

    def send_to_proxy(self, metrics):
        """Send transformed metrics to netdata-proxy"""
        if not metrics:
            logger.warning("No metrics to send")
            return False
            
        try:
            # Send as JSON array (format supported by netdata-proxy)
            response = self.session.post(
                PROXY_URL,
                json=metrics,
                headers={'Content-Type': 'application/json'}
            )
            response.raise_for_status()
            
            result = response.json()
            logger.info(f"Successfully sent {len(metrics)} metrics. "
                       f"Proxy response: {result.get('status')} - "
                       f"{result.get('rows_inserted', 0)} rows inserted in "
                       f"{result.get('processing_time', 0)}ms")
            return True
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to send metrics to proxy: {e}")
            return False

    def health_check(self):
        """Check if both Netdata and proxy are accessible"""
        try:
            # Check Netdata API
            netdata_response = self.session.get(f"{NETDATA_URL}/api/v1/info")
            netdata_response.raise_for_status()
            logger.info(f"Netdata API accessible - Version: {netdata_response.json().get('version', 'unknown')}")
            
            # Check proxy health
            proxy_response = self.session.get(f"{PROXY_URL}/health")
            proxy_response.raise_for_status()
            logger.info(f"Proxy accessible - Status: {proxy_response.json().get('status')}")
            
            return True
        except requests.exceptions.RequestException as e:
            logger.error(f"Health check failed: {e}")
            return False

    def run_once(self):
        """Pull metrics once and forward to proxy"""
        logger.info("Pulling metrics from Netdata...")
        all_host_data = self.get_metrics_for_all_hosts()
        
        if not all_host_data:
            logger.error("No data received from Netdata")
            return False
            
        total_charts = sum(len(host_data) for host_data in all_host_data.values() if isinstance(host_data, dict))
        logger.info(f"Received data from {len(all_host_data)} hosts with {total_charts} total charts")
        
        # Transform metrics
        metrics = self.transform_metrics(all_host_data)
        logger.info(f"Transformed {len(metrics)} individual metrics")
        
        if not metrics:
            logger.warning("No valid metrics after transformation")
            return False
            
        # Send to proxy
        return self.send_to_proxy(metrics)

    def run_continuous(self):
        """Run continuously with specified interval"""
        logger.info(f"Starting continuous mode - pulling every {INTERVAL_SECONDS} seconds")
        
        # Initial health check
        if not self.health_check():
            logger.error("Health check failed - exiting")
            return
            
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