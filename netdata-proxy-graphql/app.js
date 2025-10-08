// netdata-proxy-graphql/app.js
// Proxy to convert Netdata JSON format to GraphQL API calls

const express = require('express');
const axios = require('axios');
const { v4: uuidv4 } = require('uuid');

const app = express();
const port = process.env.PORT || 8090;
const parsedBatchSize = parseInt(process.env.GRAPHQL_BATCH_SIZE || '20', 10);
const GRAPHQL_BATCH_SIZE = Number.isFinite(parsedBatchSize) && parsedBatchSize > 0 ? parsedBatchSize : 20;

// GraphQL API configuration
const GRAPHQL_ENDPOINT = process.env.GRAPHQL_ENDPOINT || 'https://digitalize.oxeanbits.com/graphql';
const SHEET_ID = process.env.SHEET_ID || '68bf175e85d08f78bf97b15f'; // Default sheet ID from example
const API_KEY = process.env.API_KEY; // API key for GraphQL API authentication

// Debug middleware to log all requests
app.use((req, res, next) => {
  console.log(`[${new Date().toISOString()}] ${req.method} ${req.url} from ${req.ip} - Content-Type: ${req.headers['content-type']} - Length: ${req.headers['content-length']}`);
  next();
});

// Middleware
app.use(express.json({ limit: '10mb' }));
app.use(express.text({ limit: '10mb' }));

// Health check endpoint
app.get('/health', (req, res) => {
  res.json({ 
    status: 'healthy', 
    timestamp: new Date().toISOString(),
    graphql_endpoint: GRAPHQL_ENDPOINT,
    sheet_id: SHEET_ID,
    auth_configured: !!API_KEY
  });
});

// Test endpoint to verify GraphQL connection
app.get('/test', async (req, res) => {
  try {
    // Test GraphQL endpoint by sending a simple record
    const testRecord = {
      timestamp: new Date().toISOString().slice(0, 16),
      hostname: 'test',
      chart_id: 'test.connection',
      chart_name: 'Test Connection',
      dimension: 'status',
      value: 1,
      unit: null,
      chart_context: 'test',
      chart_type: 'line',
      data_source_operation: 'AVG'
    };

    const result = await sendRecordsToGraphQL([testRecord]);

    res.json({
      status: result.recordsFailed === 0 ? 'success' : 'partial_success',
      message: 'GraphQL connection test executed',
      records_sent: result.recordsSent,
      records_failed: result.recordsFailed,
      errors: result.errors,
      timestamp: new Date().toISOString()
    });
  } catch (error) {
    res.status(500).json({
      status: 'error',
      message: error.message,
      timestamp: new Date().toISOString()
    });
  }
});

// Generic function to process metrics and send to GraphQL
async function processMetrics(req, res, aggregationType) {
  try {
    const startTime = Date.now();
    let rawData = req.body;

    console.log(`[${new Date().toISOString()}] ${aggregationType.toUpperCase()} request:`, {
      type: typeof rawData,
      contentType: req.headers['content-type'],
      size: rawData ? rawData.length || JSON.stringify(rawData).length : 0,
      sample: typeof rawData === 'string' ? rawData.substring(0, 200) + '...' : JSON.stringify(rawData).substring(0, 200) + '...'
    });

    // Handle Netdata's line-delimited JSON format
    if (typeof rawData === 'string') {
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

      console.log(`[${new Date().toISOString()}] Parsed ${rawData.length} ${aggregationType} metrics from JSONL`);
    }

    // Convert to GraphQL format
    const graphqlRecords = convertNetdataToGraphQL(rawData, aggregationType);

    if (graphqlRecords.length === 0) {
      console.warn('No valid data to send');
      return res.status(200).json({
        status: 'no_data',
        message: 'No valid metrics found',
        processing_time: Date.now() - startTime
      });
    }

    const sendStarted = Date.now();
    const sendSummary = await sendRecordsToGraphQL(graphqlRecords);
    const processingTime = Date.now() - startTime;

    console.log(
      `[${new Date().toISOString()}] Processed ${graphqlRecords.length} ${aggregationType} records: ` +
      `${sendSummary.recordsSent} success, ${sendSummary.recordsFailed} errors in ${processingTime}ms`
    );

    res.status(200).json({
      status: sendSummary.recordsFailed === 0 ? 'success' : 'partial_success',
      aggregation_type: aggregationType,
      records_processed: graphqlRecords.length,
      records_sent: sendSummary.recordsSent,
      records_failed: sendSummary.recordsFailed,
      errors: sendSummary.errors.slice(0, 5),
      processing_time: processingTime,
      send_time: Date.now() - sendStarted
    });

  } catch (error) {
    console.error(`Error processing ${aggregationType} request:`, error);
    res.status(500).json({
      status: 'error',
      message: error.message,
      timestamp: new Date().toISOString()
    });
  }
}

// Main endpoint for Netdata data (legacy - defaults to average)
app.post('/', async (req, res) => {
  console.log(`[${new Date().toISOString()}] === MAIN ENDPOINT HIT (defaulting to average) ===`);
  await processMetrics(req, res, 'AVG');
});

// Average metrics endpoint
app.post('/avg', async (req, res) => {
  console.log(`[${new Date().toISOString()}] === AVERAGE ENDPOINT HIT ===`);
  await processMetrics(req, res, 'AVG');
});

// Maximum metrics endpoint
app.post('/max', async (req, res) => {
  console.log(`[${new Date().toISOString()}] === MAXIMUM ENDPOINT HIT ===`);
  await processMetrics(req, res, 'MAX');
});

// Median metrics endpoint
app.post('/median', async (req, res) => {
  console.log(`[${new Date().toISOString()}] === MEDIAN ENDPOINT HIT ===`);
  await processMetrics(req, res, 'MEDIAN');
});

// Convert Netdata JSON to GraphQL record format
function convertNetdataToGraphQL(data, aggregationType = 'AVG') {
  const records = [];

  // Handle array of Netdata metrics (JSONL format)
  if (Array.isArray(data)) {
    data.forEach(metric => {
      const record = processNetdataMetric(metric, aggregationType);
      if (record) records.push(record);
    });
  } else if (data.hostname && data.charts) {
    // Legacy single host format (check hostname first!)
    Object.entries(data.charts).forEach(([chartId, chartData]) => {
      records.push(...processChart(chartId, chartData, new Date(), data.hostname, aggregationType));
    });
  } else if (data.charts) {
    // Legacy charts format without hostname
    Object.entries(data.charts).forEach(([chartId, chartData]) => {
      records.push(...processChart(chartId, chartData, new Date(), 'unknown', aggregationType));
    });
  } else {
    // Single metric format (legacy)
    records.push(...processMetric(data, new Date(), aggregationType));
  }

  return records.filter(record => record && typeof record.value === 'number' && !isNaN(record.value));
}

// Process individual Netdata metric (from JSONL format)
function processNetdataMetric(metric, aggregationType = 'AVG') {
  if (!metric || typeof metric !== 'object') return null;

  // Handle hostname replacement from %H template
  let hostname = metric.hostname || 'unknown';
  if (hostname === '%H') {
    hostname = 'netdata-parent-test'; // Use container hostname
  }

  // Convert timestamp from Unix seconds to ISO format
  let timestamp;
  if (metric.timestamp) {
    timestamp = new Date(metric.timestamp * 1000).toISOString().slice(0, 16); // YYYY-MM-DDTHH:MM format
  } else {
    timestamp = new Date().toISOString().slice(0, 16);
  }

  return {
    timestamp: timestamp,
    hostname: hostname,
    chart_id: metric.chart_id || 'unknown',
    chart_name: metric.chart_name || metric.chart_id || 'Unknown Chart',
    dimension: metric.id || metric.name || 'value',
    value: parseFloat(metric.value) || 0,
    unit: metric.units || null,
    chart_context: metric.chart_context || metric.chart_id || 'unknown',
    chart_type: metric.chart_type || 'line',
    data_source_operation: aggregationType
  };
}

function processMetric(metric, timestamp, aggregationType = 'AVG') {
  const records = [];

  if (!metric || typeof metric !== 'object') return records;

  const baseRecord = {
    timestamp: metric.timestamp ?
      new Date(metric.timestamp * 1000).toISOString().slice(0, 16) :
      timestamp.toISOString().slice(0, 16),
    hostname: metric.hostname || metric.host || 'unknown',
    chart_id: metric.chart || metric.chart_id || 'unknown',
    chart_name: metric.chart_name || metric.title || metric.chart || 'Unknown Chart',
    unit: metric.units || null,
    chart_context: metric.context || metric.chart || 'unknown',
    chart_type: metric.chart_type || metric.type || 'line',
    data_source_operation: aggregationType
  };

  // Handle dimensions
  if (metric.dimensions) {
    Object.entries(metric.dimensions).forEach(([dimName, dimValue]) => {
      if (typeof dimValue === 'number' && !isNaN(dimValue)) {
        records.push({
          ...baseRecord,
          dimension: dimName,
          value: dimValue
        });
      }
    });
  } else if (metric.value !== undefined) {
    records.push({
      ...baseRecord,
      dimension: metric.dimension || 'value',
      value: parseFloat(metric.value)
    });
  }

  return records;
}

function processChart(chartId, chartData, timestamp, hostname = 'unknown', aggregationType = 'AVG') {
  const records = [];

  if (!chartData || typeof chartData !== 'object') return records;

  const baseRecord = {
    timestamp: chartData.timestamp ?
      new Date(chartData.timestamp * 1000).toISOString().slice(0, 16) :
      timestamp.toISOString().slice(0, 16),
    hostname: hostname,
    chart_id: chartId,
    chart_name: chartData.name || chartData.title || chartId,
    unit: chartData.units || null,
    chart_context: chartData.context || chartId,
    chart_type: chartData.chart_type || chartData.type || 'line',
    data_source_operation: aggregationType
  };

  // Handle data points
  if (chartData.data) {
    Object.entries(chartData.data).forEach(([dimName, dimValue]) => {
      if (typeof dimValue === 'number' && !isNaN(dimValue)) {
        records.push({
          ...baseRecord,
          dimension: dimName,
          value: dimValue
        });
      }
    });
  }

  return records;
}

async function sendRecordsToGraphQL(records) {
  if (!API_KEY) {
    throw new Error('API key not configured. Set API_KEY environment variable.');
  }

  const summary = {
    recordsSent: 0,
    recordsFailed: 0,
    errors: []
  };

  const batches = chunkArray(records, GRAPHQL_BATCH_SIZE);

  for (const batch of batches) {
    try {
      const batchResult = await sendGraphQLBatch(batch);
      summary.recordsSent += batchResult.sent;
      summary.recordsFailed += batchResult.failed.length;
      summary.errors.push(...batchResult.failed);
    } catch (error) {
      summary.recordsFailed += batch.length;
      summary.errors.push({
        error: error.message,
        records: batch
      });
      console.error(`[${new Date().toISOString()}] Failed to send batch to GraphQL:`, error.message);
    }
  }

  return summary;
}

function chunkArray(items, size) {
  const chunkSize = Math.max(size || 1, 1);
  const chunks = [];
  for (let i = 0; i < items.length; i += chunkSize) {
    chunks.push(items.slice(i, i + chunkSize));
  }
  return chunks;
}

function buildBatchMutation(records) {
  const variableDefinitions = ['$sheetId: ID'];
  const variables = { sheetId: SHEET_ID };
  const aliasBlocks = [];

  records.forEach((record, index) => {
    const alias = `metric${index}`;
    const idVar = `id${index}`;
    const dynamicFieldsVar = `dynamicFields${index}`;

    variableDefinitions.push(`$${idVar}: ID`);
    variableDefinitions.push(`$${dynamicFieldsVar}: Hash`);

    variables[idVar] = uuidv4();
    variables[dynamicFieldsVar] = {
      timestamp: record.timestamp,
      hostname: record.hostname,
      chart_id: record.chart_id,
      chart_name: record.chart_name,
      dimension: record.dimension,
      value: record.value,
      unit: record.unit,
      chart_context: record.chart_context,
      chart_type: record.chart_type,
      data_source_operation: record.data_source_operation
    };

    aliasBlocks.push(`
      ${alias}: createRecord(data: {id: $${idVar}, sheetId: $sheetId, dynamicFields: $${dynamicFieldsVar}}) {
        id
        sheetId
        projectId
        dynamicFields
        dynamicAssociations
        createdAt
        updatedAt
        createdById
        updatedById
        aiExtraction
        attachments {
          id
          recordId
          columnKey
          name
          size
          comments
          sheetId
          type
          mode
          extension
          createdAt
          updatedAt
          createdBy {
            id
            name
            email
            profilePicture {
              id
              mode
            }
          }
          updatedBy {
            id
            name
            email
          }
          aiExtraction
        }
      }
    `);
  });

  const query = `mutation createRecords(${variableDefinitions.join(', ')}) {${aliasBlocks.join('\n')}}`;

  return {
    operationName: 'createRecords',
    query,
    variables
  };
}

async function sendGraphQLBatch(records) {
  if (!records.length) {
    return { sent: 0, failed: [] };
  }

  const payload = buildBatchMutation(records);

  const headers = {
    'Content-Type': 'application/json',
    'Authorization': API_KEY
  };

  console.log(`[${new Date().toISOString()}] === GRAPHQL BATCH REQUEST ===`);
  console.log('URL:', GRAPHQL_ENDPOINT);
  console.log('Headers:', {
    'Content-Type': headers['Content-Type'],
    'Authorization': `${API_KEY.substring(0, 20)}...`
  });
  console.log('Batch size:', records.length);

  try {
    const response = await axios.post(GRAPHQL_ENDPOINT, payload, {
      headers,
      timeout: 120000
    });

    const errors = response.data.errors || [];
    if (errors.length > 0) {
      console.log(`[${new Date().toISOString()}] GraphQL batch returned errors:`, JSON.stringify(errors, null, 2));
    }

    const failedAliases = new Set(
      errors.map(err => (Array.isArray(err.path) && err.path.length ? err.path[0] : null)).filter(Boolean)
    );

    const data = response.data.data || {};

    const failed = [];
    records.forEach((record, index) => {
      const alias = `metric${index}`;
      if (failedAliases.has(alias) || !(alias in data)) {
        failed.push({
          record,
          error: `GraphQL alias ${alias} failed`
        });
      }
    });

    const successfulCount = records.length - failed.length;
    console.log(`[${new Date().toISOString()}] GraphQL batch success: ${successfulCount}/${records.length}`);

    return {
      sent: successfulCount,
      failed
    };
  } catch (error) {
    console.log(`[${new Date().toISOString()}] === GRAPHQL BATCH ERROR ===`);
    if (error.response) {
      console.log('Response Status:', error.response.status);
      console.log('Response Data:', JSON.stringify(error.response.data, null, 2));
      console.log('Response Headers:', error.response.headers);
      throw new Error(`GraphQL API error: ${error.response.status} - ${JSON.stringify(error.response.data)}`);
    } else if (error.request) {
      console.log('Network Error - No Response Received');
      console.log('Request Config:', error.config);
      throw new Error('Network error: Could not reach GraphQL endpoint');
    } else {
      console.log('Request Setup Error:', error.message);
      throw new Error(`Request error: ${error.message}`);
    }
  }
}

// TCP Server for raw Netdata connections (JSONL format)
const net = require('net');

// Create TCP server for average data
const tcpServerAvg = net.createServer((socket) => {
  console.log(`[${new Date().toISOString()}] AVERAGE TCP connection from ${socket.remoteAddress}:${socket.remotePort}`);
  handleTcpConnection(socket, 'AVG');
});

// Create TCP server for maximum data  
const tcpServerMax = net.createServer((socket) => {
  console.log(`[${new Date().toISOString()}] MAXIMUM TCP connection from ${socket.remoteAddress}:${socket.remotePort}`);
  handleTcpConnection(socket, 'MAX');
});

// Handle TCP connection data processing
function handleTcpConnection(socket, aggregationType) {
  let buffer = '';

  socket.on('data', async (data) => {
    buffer += data.toString();

    // Process complete lines (JSONL format)
    const lines = buffer.split('\n');
    buffer = lines.pop(); // Keep incomplete line in buffer

    if (lines.length > 0) {
      console.log(`[${new Date().toISOString()}] Received ${lines.length} JSONL lines from Netdata (${aggregationType})`);

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
          // Convert to GraphQL format with specific aggregation type
          const graphqlRecords = convertNetdataToGraphQL(metrics, aggregationType);

          if (graphqlRecords.length > 0) {
            const summary = await sendRecordsToGraphQL(graphqlRecords);
            console.log(
              `[${new Date().toISOString()}] Successfully sent ${summary.recordsSent}/${graphqlRecords.length} ${aggregationType} records from TCP connection`
            );
          }
        }

      } catch (error) {
        console.error(`Error processing TCP ${aggregationType} data:`, error);
      }
    }
  });

  socket.on('end', () => {
    console.log(`[${new Date().toISOString()}] ${aggregationType} TCP connection ended`);
  });

  socket.on('error', (err) => {
    console.error(`TCP ${aggregationType} socket error:`, err.message);
  });
}

// Start HTTP server on port 8090 for HTTP endpoints and health checks
app.listen(port, '0.0.0.0', () => {
  console.log(`[${new Date().toISOString()}] HTTP server listening on port ${port} for GraphQL proxy`);
  console.log('Environment:', {
    GRAPHQL_ENDPOINT: GRAPHQL_ENDPOINT,
    SHEET_ID: SHEET_ID,
    PORT: port,
    API_KEY_CONFIGURED: !!API_KEY
  });
  
  if (!API_KEY) {
    console.warn('⚠️  WARNING: API_KEY not configured. GraphQL requests will fail. Set API_KEY environment variable.');
  }
});

// Start TCP server for average data on port 8091
const tcpAvgPort = 8091;
tcpServerAvg.listen(tcpAvgPort, '0.0.0.0', () => {
  console.log(`[${new Date().toISOString()}] TCP server listening on port ${tcpAvgPort} for AVERAGE Netdata connections`);
});

// Start TCP server for maximum data on port 8092
const tcpMaxPort = 8092;
tcpServerMax.listen(tcpMaxPort, '0.0.0.0', () => {
  console.log(`[${new Date().toISOString()}] TCP server listening on port ${tcpMaxPort} for MAXIMUM Netdata connections`);
});

// Error handling middleware
app.use((error, req, res, next) => {
  console.error('Unhandled error:', error);
  res.status(500).json({
    status: 'error',
    message: 'Internal server error',
    timestamp: new Date().toISOString()
  });
});

// Graceful shutdown
process.on('SIGINT', () => {
  console.log('Shutting down gracefully...');
  process.exit(0);
});
