// netdata-proxy/app.js
// Proxy to convert Netdata JSON format to ClickHouse insertions

const express = require('express');
const { createClient } = require('@clickhouse/client');

const app = express();
const port = process.env.PORT || 8080;

// Debug middleware to log all requests
app.use((req, res, next) => {
  console.log(`[${new Date().toISOString()}] ${req.method} ${req.url} from ${req.ip} - Content-Type: ${req.headers['content-type']} - Length: ${req.headers['content-length']}`);
  next();
});

// Middleware
app.use(express.json({ limit: '10mb' }));
app.use(express.text({ limit: '10mb' }));

// ClickHouse client configuration
const clickhouseClient = createClient({
  url: `http://${process.env.CLICKHOUSE_HOST}:${process.env.CLICKHOUSE_PORT}`,
  username: process.env.CLICKHOUSE_USER || 'default',
  password: process.env.CLICKHOUSE_PASSWORD || '',
  database: process.env.CLICKHOUSE_DATABASE || 'netdata_metrics',
});

// Health check endpoint
app.get('/health', (req, res) => {
  res.json({ status: 'healthy', timestamp: new Date().toISOString() });
});

// Main endpoint for Netdata data (handles JSONL format)
app.post('/', async (req, res) => {
  try {
    const startTime = Date.now();
    let rawData = req.body;

    console.log(`[${new Date().toISOString()}] Raw request:`, {
      type: typeof rawData,
      contentType: req.headers['content-type'],
      size: rawData ? rawData.length || JSON.stringify(rawData).length : 0,
      sample: typeof rawData === 'string' ? rawData.substring(0, 200) + '...' : JSON.stringify(rawData).substring(0, 200) + '...'
    });

    // Handle Netdata's line-delimited JSON format
    if (typeof rawData === 'string') {
      // Parse JSONL format (each line is a separate JSON object)
      const lines = rawData.trim().split('\n').filter(line => line.trim());
      rawData = [];

      for (const line of lines) {
        try {
          const metric = JSON.parse(line.trim());
          rawData.push(metric);
        } catch (e) {
          console.warn('Failed to parse JSON line:', line.substring(0, 100), e.message);
        }
      }

      console.log(`[${new Date().toISOString()}] Parsed ${rawData.length} metrics from JSONL`);
    }

    // Convert to ClickHouse format
    const clickhouseRows = convertNetdataToClickHouse(rawData);

    if (clickhouseRows.length === 0) {
      console.warn('No valid data to insert');
      return res.status(200).json({
        status: 'no_data',
        message: 'No valid metrics found',
        processing_time: Date.now() - startTime
      });
    }

    // Insert into ClickHouse
    await insertToClickHouse(clickhouseRows);

    const processingTime = Date.now() - startTime;
    console.log(`[${new Date().toISOString()}] Successfully inserted ${clickhouseRows.length} rows in ${processingTime}ms`);

    res.status(200).json({
      status: 'success',
      rows_inserted: clickhouseRows.length,
      processing_time: processingTime
    });

  } catch (error) {
    console.error('Error processing request:', error);
    res.status(500).json({
      status: 'error',
      message: error.message,
      timestamp: new Date().toISOString()
    });
  }
});

// Convert Netdata JSON to ClickHouse rows
function convertNetdataToClickHouse(data) {
  const rows = [];

  // Handle array of Netdata metrics (JSONL format)
  if (Array.isArray(data)) {
    data.forEach(metric => {
      const row = processNetdataMetric(metric);
      if (row) rows.push(row);
    });
  } else if (data.hostname && data.charts) {
    // Legacy single host format (check hostname first!)
    Object.entries(data.charts).forEach(([chartId, chartData]) => {
      rows.push(...processChart(chartId, chartData, new Date(), data.hostname));
    });
  } else if (data.charts) {
    // Legacy charts format without hostname
    Object.entries(data.charts).forEach(([chartId, chartData]) => {
      rows.push(...processChart(chartId, chartData, new Date()));
    });
  } else {
    // Single metric format (legacy)
    rows.push(...processMetric(data, new Date()));
  }

  return rows.filter(row => row && typeof row.value === 'number' && !isNaN(row.value));
}

// Process individual Netdata metric (from JSONL format)
function processNetdataMetric(metric) {
  if (!metric || typeof metric !== 'object') return null;

  // Handle hostname replacement from %H template
  let hostname = metric.hostname || 'unknown';
  if (hostname === '%H') {
    hostname = 'netdata-parent-test'; // Use container hostname
  }

  // Convert timestamp from Unix seconds to ClickHouse format
  let timestamp;
  if (metric.timestamp) {
    timestamp = new Date(metric.timestamp * 1000).toISOString().replace('T', ' ').slice(0, 23);
  } else {
    timestamp = new Date().toISOString().replace('T', ' ').slice(0, 23);
  }

  return {
    timestamp: timestamp,
    hostname: hostname,
    chart_id: metric.chart_id || 'unknown',
    chart_name: metric.chart_name || metric.chart_id || 'Unknown Chart',
    dimension: metric.id || metric.name || 'value',
    value: parseFloat(metric.value) || 0,
    units: metric.units || '',
    family: metric.chart_family || '',
    context: metric.chart_context || metric.chart_id || '',
    chart_type: metric.chart_type || 'line'
  };
}

function processMetric(metric, timestamp) {
  const rows = [];

  if (!metric || typeof metric !== 'object') return rows;

  const baseRow = {
    timestamp: metric.timestamp ?
      new Date(metric.timestamp * 1000).toISOString().replace('T', ' ').slice(0, 23) :
      timestamp.toISOString().replace('T', ' ').slice(0, 23),
    hostname: metric.hostname || metric.host || 'unknown',
    chart_id: metric.chart || metric.chart_id || 'unknown',
    chart_name: metric.chart_name || metric.title || metric.chart || 'Unknown Chart',
    units: metric.units || '',
    family: metric.family || '',
    context: metric.context || metric.chart || '',
    chart_type: metric.chart_type || metric.type || 'line'
  };

  // Handle dimensions
  if (metric.dimensions) {
    Object.entries(metric.dimensions).forEach(([dimName, dimValue]) => {
      if (typeof dimValue === 'number' && !isNaN(dimValue)) {
        rows.push({
          ...baseRow,
          dimension: dimName,
          value: dimValue
        });
      }
    });
  } else if (metric.value !== undefined) {
    rows.push({
      ...baseRow,
      dimension: metric.dimension || 'value',
      value: parseFloat(metric.value)
    });
  }

  return rows;
}

function processChart(chartId, chartData, timestamp, hostname = 'unknown') {
  const rows = [];

  if (!chartData || typeof chartData !== 'object') return rows;

  const baseRow = {
    timestamp: chartData.timestamp ?
      new Date(chartData.timestamp * 1000).toISOString().replace('T', ' ').slice(0, 23) :
      timestamp.toISOString().replace('T', ' ').slice(0, 23),
    hostname: hostname,
    chart_id: chartId,
    chart_name: chartData.name || chartData.title || chartId,
    units: chartData.units || '',
    family: chartData.family || '',
    context: chartData.context || chartId,
    chart_type: chartData.chart_type || chartData.type || 'line'
  };

  // Handle data points
  if (chartData.data) {
    Object.entries(chartData.data).forEach(([dimName, dimValue]) => {
      if (typeof dimValue === 'number' && !isNaN(dimValue)) {
        rows.push({
          ...baseRow,
          dimension: dimName,
          value: dimValue
        });
      }
    });
  }

  return rows;
}

// Insert data into ClickHouse
async function insertToClickHouse(rows) {
  if (rows.length === 0) return;

  const query = `
    INSERT INTO metrics (
      timestamp, hostname, chart_id, chart_name, dimension,
      value, units, family, context, chart_type
    ) VALUES
  `;

  try {
    await clickhouseClient.insert({
      table: 'metrics',
      values: rows,
      format: 'JSONEachRow',
    });
  } catch (error) {
    console.error('ClickHouse insertion failed:', error);
    throw error;
  }
}

// Error handling middleware
app.use((error, req, res, next) => {
  console.error('Unhandled error:', error);
  res.status(500).json({
    status: 'error',
    message: 'Internal server error',
    timestamp: new Date().toISOString()
  });
});

// TCP Server for raw Netdata connections (JSONL format)
const net = require('net');

const tcpServer = net.createServer((socket) => {
  console.log(`[${new Date().toISOString()}] Raw TCP connection from ${socket.remoteAddress}:${socket.remotePort}`);

  let buffer = '';

  socket.on('data', async (data) => {
    buffer += data.toString();

    // Process complete lines (JSONL format)
    const lines = buffer.split('\n');
    buffer = lines.pop(); // Keep incomplete line in buffer

    if (lines.length > 0) {
      console.log(`[${new Date().toISOString()}] Received ${lines.length} JSONL lines from Netdata`);

      try {
        // Parse JSONL lines
        const metrics = [];
        for (const line of lines) {
          if (line.trim()) {
            try {
              const metric = JSON.parse(line.trim());
              metrics.push(metric);
            } catch (e) {
              console.warn('Failed to parse JSONL line:', line.substring(0, 100));
            }
          }
        }

        if (metrics.length > 0) {
          // Convert to ClickHouse format
          const clickhouseRows = convertNetdataToClickHouse(metrics);

          if (clickhouseRows.length > 0) {
            // Insert into ClickHouse
            await insertToClickHouse(clickhouseRows);
            console.log(`[${new Date().toISOString()}] Successfully inserted ${clickhouseRows.length} rows from TCP connection`);
          }
        }

      } catch (error) {
        console.error('Error processing TCP data:', error);
      }
    }
  });

  socket.on('end', () => {
    console.log(`[${new Date().toISOString()}] TCP connection ended`);
  });

  socket.on('error', (err) => {
    console.error('TCP socket error:', err.message);
  });
});

// Start TCP server for Netdata (port 8080) and HTTP server for other clients (port 8081) (data-generator and helth check)
const httpPort = 8081;

tcpServer.listen(port, '0.0.0.0', () => {
  console.log(`[${new Date().toISOString()}] TCP server listening on port ${port} for raw Netdata connections`);
});

app.listen(httpPort, '0.0.0.0', () => {
  console.log(`[${new Date().toISOString()}] HTTP server listening on port ${httpPort} for data generator and health checks`);
  console.log('Environment:', {
    CLICKHOUSE_HOST: process.env.CLICKHOUSE_HOST,
    CLICKHOUSE_PORT: process.env.CLICKHOUSE_PORT,
    CLICKHOUSE_USER: process.env.CLICKHOUSE_USER,
    CLICKHOUSE_DATABASE: process.env.CLICKHOUSE_DATABASE
  });
});

// Graceful shutdown
process.on('SIGINT', async () => {
  console.log('Shutting down gracefully...');
  await clickhouseClient.close();
  process.exit(0);
});