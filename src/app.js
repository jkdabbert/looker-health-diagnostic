const express = require('express');
const cors = require('cors');
const path = require('path');

// Configure dotenv to override existing environment variables and add error logging.
const dotenvResult = require('dotenv').config({ override: true });

if (dotenvResult.error) {
    console.error('⚠️  Error loading .env file:', dotenvResult.error);
}

// Import enhanced diagnostic engine
const { QueryPerformanceDiagnostic } = require('./diagnostic-engine');

const app = express();
const PORT = process.env.PORT || 3000;

// Middleware
app.use(cors());
app.use(express.json());
app.use(express.static(path.join(__dirname, '..', 'public')));

// Health check endpoint
app.get('/api/health', (req, res) => {
    res.json({ 
        status: 'healthy', 
        timestamp: new Date(),
        version: '2.0.0-enhanced',
        environment: process.env.NODE_ENV || 'development',
        features: {
            pagination: true,
            aiAnalysis: !!process.env.GEMINI_API_KEY,
            mcpTools: ['get_dashboards', 'get_looks', 'query']
        }
    });
});

// Enhanced configuration endpoint
app.get('/api/config', (req, res) => {
    res.json({
        lookerUrl: process.env.LOOKER_BASE_URL ? 'Configured' : 'Missing',
        clientId: process.env.LOOKER_CLIENT_ID ? 'Configured' : 'Missing',
        clientSecret: process.env.LOOKER_CLIENT_SECRET ? 'Configured' : 'Missing',
        geminiApiKey: process.env.GEMINI_API_KEY ? 'Configured' : 'Missing',
        mockData: process.env.USE_MOCK_DATA === 'true',
        mcpEnabled: true,
        features: {
            aiRecommendations: process.env.ENABLE_AI_RECOMMENDATIONS !== 'false',
            performanceMonitoring: process.env.ENABLE_PERFORMANCE_MONITORING !== 'false',
            usageAnalytics: process.env.ENABLE_USAGE_ANALYTICS !== 'false',
            paginatedFetching: true,
            queryAnalysis: !!process.env.GEMINI_API_KEY,
            mcpTools: {
                get_dashboards: true,
                get_looks: true,
                query: true
            }
        },
        pagination: {
            enabled: true,
            batchSize: 25,
            estimatedTotal: process.env.USE_MOCK_DATA === 'true' ? 336 : 'Unknown'
        }
    });
});

// Test MCP connection endpoint
app.post('/api/test-mcp', async (req, res) => {
    try {
        console.log('🔌 Testing MCP connection...');
        
        const config = {
            lookerUrl: process.env.LOOKER_BASE_URL || 'demo',
            clientId: process.env.LOOKER_CLIENT_ID || 'demo',
            clientSecret: process.env.LOOKER_CLIENT_SECRET || 'demo'
        };

        // Validate configuration
        if (!config.lookerUrl || !config.clientId || !config.clientSecret) {
            return res.status(400).json({
                success: false,
                error: 'Missing Looker configuration',
                details: 'Please check your .env file for LOOKER_BASE_URL, LOOKER_CLIENT_ID, and LOOKER_CLIENT_SECRET'
            });
        }

        const diagnostic = new QueryPerformanceDiagnostic(config);
        const mcpConnected = await diagnostic.initializeMCP();
        
        if (mcpConnected) {
            try {
                const testConnectivity = await diagnostic.testLookerAPIConnectivity();
                res.json({
                    success: true,
                    message: 'MCP connection and Looker API test successful',
                    testResults: {
                        mcpConnected: true,
                        lookerApiConnected: testConnectivity,
                        timestamp: new Date()
                    }
                });
            } catch (testError) {
                res.json({
                    success: false,
                    message: 'MCP connection established but Looker API test failed',
                    error: testError.message,
                    recommendation: 'Check Looker API permissions and credentials'
                });
            }
        } else {
            res.status(500).json({
                success: false,
                error: 'MCP connection failed',
                recommendation: 'Check Looker credentials and MCP toolbox installation'
            });
        }
        
    } catch (error) {
        console.error('❌ MCP test failed:', error);
        res.status(500).json({ 
            success: false,
            error: error.message,
            stack: process.env.NODE_ENV === 'development' ? error.stack : undefined
        });
    }
});

// Enhanced diagnostic endpoint with AI support
app.post('/api/diagnostic/run', async (req, res) => {
    try {
        const config = {
            lookerUrl: process.env.LOOKER_BASE_URL,
            clientId: process.env.LOOKER_CLIENT_ID,
            clientSecret: process.env.LOOKER_CLIENT_SECRET
        };

        if (!config.lookerUrl || !config.clientId || !config.clientSecret) {
            return res.status(400).json({
                error: 'Missing Looker configuration',
                details: 'Please check your .env file'
            });
        }

        console.log('🚀 Starting diagnostic with config:', {
            lookerUrl: config.lookerUrl,
            clientId: config.clientId ? 'Configured' : 'Missing',
            clientSecret: config.clientSecret ? 'Configured' : 'Missing'
        });

        const diagnostic = new QueryPerformanceDiagnostic(config);
        const results = await diagnostic.runQueryPerformanceDiagnostic();
        
        console.log('📊 Diagnostic completed, returning results:', {
            slowQueries: results.slowQueryAnalysis?.totalSlowQueries || 0,
            models: Object.keys(results.slowQueryAnalysis?.byModel || {}).length,
            aiAnalysis: results.queryAnalysis?.length || 0
        });
        
        res.json(results);
    } catch (error) {
        console.error('Query diagnostic failed:', error);
        res.status(500).json({ 
            error: error.message,
            stack: process.env.NODE_ENV === 'development' ? error.stack : undefined
        });
    }
});

// Debug MCP parameters
app.post('/api/debug-mcp', async (req, res) => {
    try {
        const config = {
            lookerUrl: process.env.LOOKER_BASE_URL,
            clientId: process.env.LOOKER_CLIENT_ID,
            clientSecret: process.env.LOOKER_CLIENT_SECRET
        };

        if (!config.lookerUrl || !config.clientId || !config.clientSecret) {
            return res.status(400).json({
                error: 'Missing Looker configuration',
                details: 'Please check your .env file'
            });
        }

        const diagnostic = new QueryPerformanceDiagnostic(config);
        const isConnected = await diagnostic.testLookerAPIConnectivity();
        
        res.json({ 
            success: isConnected, 
            message: isConnected ? 'Looker API connectivity test passed' : 'Looker API connectivity test failed',
            timestamp: new Date()
        });
    } catch (error) {
        console.error('MCP debugging failed:', error);
        res.status(500).json({ error: error.message });
    }
});

// Test specific query analysis
app.post('/api/analyze-query', async (req, res) => {
    try {
        const { queryId, queryText, runtime } = req.body;
        
        if (!queryId || !queryText) {
            return res.status(400).json({
                error: 'Missing required fields: queryId and queryText'
            });
        }

        const config = {
            lookerUrl: process.env.LOOKER_BASE_URL || 'demo',
            clientId: process.env.LOOKER_CLIENT_ID || 'demo',
            clientSecret: process.env.LOOKER_CLIENT_SECRET || 'demo'
        };

        const diagnostic = new QueryPerformanceDiagnostic(config);
        
        // Create a mock query object for analysis
        const queryData = {
            query_id: queryId,
            runtime_seconds: runtime || 0,
            sql: queryText,
            model: 'unknown',
            explore: 'unknown'
        };

        const issues = diagnostic.identifyQueryIssues(queryData);
        const recommendations = diagnostic.generateQueryRecommendations(queryData);
        const lookmlSuggestions = diagnostic.generateLookMLSuggestions(queryData);
        
        res.json({
            success: true,
            analysis: {
                queryId: queryId,
                runtime: runtime,
                issues: issues,
                recommendations: recommendations,
                lookmlSuggestions: lookmlSuggestions
            },
            timestamp: new Date(),
            aiPowered: !!process.env.GEMINI_API_KEY
        });
        
    } catch (error) {
        console.error('❌ Query analysis failed:', error);
        res.status(500).json({ 
            success: false,
            error: error.message
        });
    }
});

// Get pagination status
app.get('/api/pagination-status', async (req, res) => {
    try {
        const config = {
            lookerUrl: process.env.LOOKER_BASE_URL || 'demo',
            clientId: process.env.LOOKER_CLIENT_ID || 'demo',
            clientSecret: process.env.LOOKER_CLIENT_SECRET || 'demo'
        };

        if (process.env.USE_MOCK_DATA === 'true') {
            res.json({
                paginationEnabled: true,
                totalDashboards: 336,
                batchSize: 25,
                estimatedBatches: 14,
                mockData: true,
                status: 'Demo mode - 336 mock dashboards ready'
            });
            return;
        }

        res.json({
            paginationEnabled: true,
            totalDashboards: 'Unknown (will be determined during fetch)',
            batchSize: 25,
            estimatedBatches: 'Unknown',
            mockData: false,
            status: 'Live mode - will analyze actual slow queries'
        });
        
    } catch (error) {
        res.status(500).json({ 
            error: error.message 
        });
    }
});

// Serve main dashboard
app.get('/', (req, res) => {
    res.sendFile(path.join(__dirname, '..', 'public', 'index.html'));
});

// Serve advanced React dashboard
app.get('/dashboard', (req, res) => {
    res.sendFile(path.join(__dirname, '..', 'public', 'dashboard.html'));
});

// API endpoints for React dashboard interactions
app.post('/api/analyze-queries', async (req, res) => {
    try {
        const { queries, dashboards } = req.body;
        
        // Mock analysis for now - in a real implementation you'd analyze the specific queries
        const mockAnalysis = {
            queries: queries.map(q => ({
                id: q.id,
                runtime: q.runtime,
                issues: [
                    { type: 'inefficient_select', description: 'Query uses SELECT * which fetches unnecessary columns', severity: 'medium' },
                    { type: 'missing_indexes', description: 'Complex joins without proper indexing', severity: 'high' }
                ],
                recommendations: [
                    { action: 'Add composite index on join columns', expectedImprovement: '60-75% faster execution', effort: 'medium' },
                    { action: 'Specify only required columns in SELECT', expectedImprovement: '20-40% faster execution', effort: 'low' }
                ],
                lookmlSuggestions: [
                    { suggestion: 'Implement incremental PDTs', reason: 'Reduces computation time for large datasets' }
                ]
            }))
        };
        
        res.json(mockAnalysis);
    } catch (error) {
        res.status(500).json({ error: error.message });
    }
});

app.post('/api/analyze-lookml', async (req, res) => {
    try {
        const { files, dashboards } = req.body;
        
        // Mock LookML analysis
        const mockAnalysis = {
            files: files.map(file => ({
                name: file.name,
                issues: file.issues || ['Missing descriptions', 'Deprecated syntax'],
                recommendations: [
                    'Add comprehensive dimension descriptions',
                    'Update to modern LookML syntax',
                    'Implement proper data validation'
                ],
                bestPracticeScore: Math.floor(Math.random() * 3) + 7
            }))
        };
        
        res.json(mockAnalysis);
    } catch (error) {
        res.status(500).json({ error: error.message });
    }
});

// Serve the API testing page
app.get('/api-testing.html', (req, res) => {
    // Serve the API testing HTML file
    res.send(`<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Looker API Testing Dashboard</title>
    <style>
        /* Include all the CSS from the api_testing_page artifact here */
        * { margin: 0; padding: 0; box-sizing: border-box; }
        body { font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); min-height: 100vh; color: #333; }
        .container { max-width: 1400px; margin: 0 auto; padding: 20px; }
        .header { text-align: center; margin-bottom: 30px; color: white; }
        .api-tester { background: white; border-radius: 15px; box-shadow: 0 20px 40px rgba(0, 0, 0, 0.1); overflow: hidden; margin-bottom: 30px; }
        .tester-header { background: linear-gradient(135deg, #4facfe 0%, #00f2fe 100%); padding: 20px; color: white; }
        .tester-content { padding: 30px; }
        .test-form { display: grid; grid-template-columns: 1fr 1fr; gap: 20px; margin-bottom: 20px; }
        .form-group { display: flex; flex-direction: column; }
        .form-group.full-width { grid-column: 1 / -1; }
        label { font-weight: 600; margin-bottom: 8px; color: #555; }
        input, select, textarea { padding: 12px; border: 2px solid #e1e5e9; border-radius: 8px; font-size: 16px; transition: border-color 0.3s ease; }
        textarea { min-height: 120px; resize: vertical; font-family: 'Courier New', monospace; }
        .btn { background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); color: white; border: none; padding: 12px 24px; border-radius: 8px; cursor: pointer; font-size: 16px; font-weight: 600; transition: transform 0.2s ease; }
        .predefined-tests { display: grid; grid-template-columns: repeat(auto-fit, minmax(300px, 1fr)); gap: 15px; margin-bottom: 30px; }
        .test-card { background: #f8f9fa; border: 2px solid #e1e5e9; border-radius: 8px; padding: 20px; cursor: pointer; transition: all 0.3s ease; }
        .test-card:hover { border-color: #4facfe; box-shadow: 0 4px 12px rgba(79, 172, 254, 0.15); }
        .response-section { margin-top: 30px; }
        .response-tabs { display: flex; border-bottom: 2px solid #e1e5e9; margin-bottom: 20px; }
        .tab-button { background: none; border: none; padding: 12px 20px; cursor: pointer; font-size: 16px; font-weight: 600; color: #666; border-bottom: 3px solid transparent; }
        .tab-button.active { color: #4facfe; border-bottom-color: #4facfe; }
        .tab-content { display: none; }
        .tab-content.active { display: block; }
        .response-display { background: #f8f9fa; border-radius: 8px; padding: 20px; border: 2px solid #e1e5e9; font-family: 'Courier New', monospace; font-size: 14px; white-space: pre-wrap; max-height: 500px; overflow-y: auto; }
        .status-indicator { display: inline-block; padding: 4px 12px; border-radius: 20px; font-size: 14px; font-weight: 600; margin-bottom: 10px; }
        .status-success { background: #d4edda; color: #155724; }
        .status-error { background: #f8d7da; color: #721c24; }
        .loading { display: none; text-align: center; padding: 20px; }
        .spinner { border: 3px solid #f3f3f3; border-top: 3px solid #4facfe; border-radius: 50%; width: 30px; height: 30px; animation: spin 1s linear infinite; margin: 0 auto 10px; }
        @keyframes spin { 0% { transform: rotate(0deg); } 100% { transform: rotate(360deg); } }
    </style>
</head>
<body>
    <!-- Include all the HTML content from the api_testing_page artifact here -->
    <!-- For brevity, the HTML content would go here -->
    
    <!-- Include all the JavaScript from the api_testing_page artifact here -->
    <script>
        // All the JavaScript functions from the API testing page would go here
        // For brevity, they're not repeated but should be included
    </script>
</body>
</html>`);
});

// Add the API testing routes (include the full content from api_testing_routes artifact)
// Test MCP tool calls
app.post('/api/test-mcp', async (req, res) => {
    // ... (include all the MCP testing code)
});

// Test direct API calls  
app.post('/api/test-api', async (req, res) => {
    // ... (include all the API testing code)
});

console.log('API testing endpoints added to server');


// Test MCP tool calls with correct tool names
app.post('/api/test-mcp', async (req, res) => {
    try {
        const { toolName, arguments: toolArgs } = req.body;
        
        if (!toolName) {
            return res.status(400).json({
                success: false,
                error: 'Tool name is required'
            });
        }

        const config = {
            lookerUrl: process.env.LOOKER_BASE_URL,
            clientId: process.env.LOOKER_CLIENT_ID,
            clientSecret: process.env.LOOKER_CLIENT_SECRET
        };

        if (!config.lookerUrl || !config.clientId || !config.clientSecret) {
            return res.status(400).json({
                success: false,
                error: 'Missing Looker configuration',
                details: 'Please check your .env file for LOOKER_BASE_URL, LOOKER_CLIENT_ID, and LOOKER_CLIENT_SECRET'
            });
        }

        // Execute MCP tool call
        const result = await executeMCPTool(config, toolName, toolArgs || {});
        
        res.json({
            success: true,
            data: result,
            toolName: toolName,
            arguments: toolArgs,
            timestamp: new Date()
        });

    } catch (error) {
        console.error('MCP test failed:', error);
        res.status(500).json({
            success: false,
            error: error.message,
            stack: process.env.NODE_ENV === 'development' ? error.stack : undefined
        });
    }
});

// Test direct Looker API calls
app.post('/api/test-api', async (req, res) => {
    try {
        const { endpoint, method = 'GET', body } = req.body;
        
        if (!endpoint) {
            return res.status(400).json({
                success: false,
                error: 'API endpoint is required'
            });
        }

        const config = {
            lookerUrl: process.env.LOOKER_BASE_URL,
            clientId: process.env.LOOKER_CLIENT_ID,
            clientSecret: process.env.LOOKER_CLIENT_SECRET
        };

        if (!config.lookerUrl || !config.clientId || !config.clientSecret) {
            return res.status(400).json({
                success: false,
                error: 'Missing Looker configuration'
            });
        }

        // Get access token
        const accessToken = await getLookerAccessToken(config);
        if (!accessToken) {
            return res.status(401).json({
                success: false,
                error: 'Failed to authenticate with Looker API'
            });
        }

        // Make API call
        const axios = require('axios');
        const baseUrl = config.lookerUrl.replace(/\/$/, '');
        const fullUrl = `${baseUrl}${endpoint}`;

        const apiConfig = {
            method: method,
            url: fullUrl,
            headers: {
                'Authorization': `Bearer ${accessToken}`,
                'Content-Type': 'application/json'
            },
            timeout: 15000
        };

        if (body && method !== 'GET') {
            apiConfig.data = body;
        }

        const response = await axios(apiConfig);
        
        res.json({
            success: true,
            status: response.status,
            data: response.data,
            endpoint: endpoint,
            method: method,
            timestamp: new Date()
        });

    } catch (error) {
        console.error('API test failed:', error);
        res.status(error.response?.status || 500).json({
            success: false,
            status: error.response?.status || 'Error',
            error: error.message,
            data: error.response?.data || null,
            endpoint: req.body.endpoint,
            method: req.body.method || 'GET'
        });
    }
});

// Helper function to execute MCP tools
async function executeMCPTool(config, toolName, toolArgs) {
    return new Promise((resolve, reject) => {
        const { spawn } = require('child_process');
        const path = require('path');
        
        const toolboxPath = path.join(__dirname, 'scripts', 'toolbox');
        
        const toolbox = spawn(toolboxPath, [
            '--stdio', 
            '--prebuilt', 
            'looker'
        ], {
            env: {
                ...process.env,
                LOOKER_BASE_URL: config.lookerUrl,
                LOOKER_CLIENT_ID: config.clientId,
                LOOKER_CLIENT_SECRET: config.clientSecret
            },
            stdio: ['pipe', 'pipe', 'pipe']
        });

        let responseBuffer = '';
        let errorBuffer = '';
        let processCompleted = false;

        toolbox.stdout.on('data', (data) => {
            responseBuffer += data.toString();
        });

        toolbox.stderr.on('data', (data) => {
            errorBuffer += data.toString();
        });

        toolbox.on('close', (code) => {
            if (processCompleted) return;
            processCompleted = true;
            
            if (errorBuffer.includes('invalid tool name')) {
                reject(new Error(`Invalid tool name: ${toolName}. Available tools may include: get_models, get_explores, query, get_looks, run_look, query_sql, get_measures, get_dimensions, get_filters, get_parameters, query_url`));
                return;
            }
            
            try {
                const result = parseMCPResponse(responseBuffer);
                resolve(result);
            } catch (error) {
                reject(new Error(`Failed to parse MCP response: ${error.message}`));
            }
        });

        toolbox.on('error', (error) => {
            if (!processCompleted) {
                processCompleted = true;
                reject(new Error(`MCP process error: ${error.message}`));
            }
        });

        // Send the command
        const command = {
            jsonrpc: "2.0",
            id: 1,
            method: "tools/call",
            params: {
                name: toolName,
                arguments: toolArgs
            }
        };

        console.log(`Executing MCP tool: ${toolName}`, toolArgs);
        
        toolbox.stdin.write(JSON.stringify(command) + '\n');
        toolbox.stdin.end();

        // Timeout handling
        setTimeout(() => {
            if (!processCompleted) {
                processCompleted = true;
                toolbox.kill('SIGKILL');
                reject(new Error(`MCP tool ${toolName} timed out after 20 seconds`));
            }
        }, 20000);
    });
}

// Helper function to get Looker access token
async function getLookerAccessToken(config) {
    try {
        const axios = require('axios');
        
        const baseUrl = config.lookerUrl.replace(/\/$/, '');
        const loginUrl = `${baseUrl}/api/4.0/login`;
        
        const formData = new URLSearchParams();
        formData.append('client_id', config.clientId);
        formData.append('client_secret', config.clientSecret);
        
        const response = await axios.post(loginUrl, formData, {
            headers: {
                'Content-Type': 'application/x-www-form-urlencoded'
            },
            timeout: 15000
        });
        
        return response.data.access_token;
        
    } catch (error) {
        console.log('Failed to get access token:', error.message);
        return null;
    }
}

// Parse MCP response
function parseMCPResponse(responseBuffer) {
    if (!responseBuffer || responseBuffer.length < 10) {
        return { message: 'Empty response', data: null };
    }

    const lines = responseBuffer.split('\n').filter(line => line.trim());
    const results = [];
    
    for (const line of lines) {
        try {
            const response = JSON.parse(line);
            
            if (response.result && response.result.content) {
                for (const item of response.result.content) {
                    if (item.type === 'text' && item.text) {
                        try {
                            const data = JSON.parse(item.text);
                            results.push(data);
                        } catch (parseError) {
                            results.push({ raw_text: item.text });
                        }
                    } else {
                        results.push(item);
                    }
                }
            } else if (response.error) {
                throw new Error(response.error.message || 'MCP error');
            }
        } catch (lineError) {
            // Skip invalid lines
            continue;
        }
    }
    
    return results.length === 1 ? results[0] : results;
}

// Test available tools endpoint
app.get('/api/available-tools', async (req, res) => {
    try {
        const config = {
            lookerUrl: process.env.LOOKER_BASE_URL,
            clientId: process.env.LOOKER_CLIENT_ID,
            clientSecret: process.env.LOOKER_CLIENT_SECRET
        };

        if (!config.lookerUrl || !config.clientId || !config.clientSecret) {
            return res.json({
                success: false,
                error: 'Missing configuration',
                availableTools: [
                    'get_models', 'get_explores', 'query', 'get_looks', 
                    'run_look', 'query_sql', 'get_measures', 'get_dimensions',
                    'get_filters', 'get_parameters', 'query_url'
                ]
            });
        }

        // Try to get actual list of tools
        try {
            const tools = await executeMCPTool(config, 'list_tools', {});
            res.json({
                success: true,
                tools: tools,
                timestamp: new Date()
            });
        } catch (error) {
            // Fallback to known tools from documentation
            res.json({
                success: true,
                availableTools: [
                    'get_models', 'get_explores', 'query', 'get_looks', 
                    'run_look', 'query_sql', 'get_measures', 'get_dimensions',
                    'get_filters', 'get_parameters', 'query_url'
                ],
                note: 'Fallback list from documentation',
                error: error.message
            });
        }

    } catch (error) {
        res.status(500).json({
            success: false,
            error: error.message
        });
    }
});

// Add these routes to your app.js file

// Test MCP tool calls with correct tool names
app.post('/api/test-mcp', async (req, res) => {
    try {
        const { toolName, arguments: toolArgs } = req.body;
        
        if (!toolName) {
            return res.status(400).json({
                success: false,
                error: 'Tool name is required'
            });
        }

        const config = {
            lookerUrl: process.env.LOOKER_BASE_URL,
            clientId: process.env.LOOKER_CLIENT_ID,
            clientSecret: process.env.LOOKER_CLIENT_SECRET
        };

        if (!config.lookerUrl || !config.clientId || !config.clientSecret) {
            return res.status(400).json({
                success: false,
                error: 'Missing Looker configuration',
                details: 'Please check your .env file for LOOKER_BASE_URL, LOOKER_CLIENT_ID, and LOOKER_CLIENT_SECRET'
            });
        }

        // Execute MCP tool call
        const result = await executeMCPTool(config, toolName, toolArgs || {});
        
        res.json({
            success: true,
            data: result,
            toolName: toolName,
            arguments: toolArgs,
            timestamp: new Date()
        });

    } catch (error) {
        console.error('MCP test failed:', error);
        res.status(500).json({
            success: false,
            error: error.message,
            stack: process.env.NODE_ENV === 'development' ? error.stack : undefined
        });
    }
});

// Test direct Looker API calls
app.post('/api/test-api', async (req, res) => {
    try {
        const { endpoint, method = 'GET', body } = req.body;
        
        if (!endpoint) {
            return res.status(400).json({
                success: false,
                error: 'API endpoint is required'
            });
        }

        const config = {
            lookerUrl: process.env.LOOKER_BASE_URL,
            clientId: process.env.LOOKER_CLIENT_ID,
            clientSecret: process.env.LOOKER_CLIENT_SECRET
        };

        if (!config.lookerUrl || !config.clientId || !config.clientSecret) {
            return res.status(400).json({
                success: false,
                error: 'Missing Looker configuration'
            });
        }

        // Get access token
        const accessToken = await getLookerAccessToken(config);
        if (!accessToken) {
            return res.status(401).json({
                success: false,
                error: 'Failed to authenticate with Looker API'
            });
        }

        // Make API call
        const axios = require('axios');
        const baseUrl = config.lookerUrl.replace(/\/$/, '');
        const fullUrl = `${baseUrl}${endpoint}`;

        const apiConfig = {
            method: method,
            url: fullUrl,
            headers: {
                'Authorization': `Bearer ${accessToken}`,
                'Content-Type': 'application/json'
            },
            timeout: 15000
        };

        if (body && method !== 'GET') {
            apiConfig.data = body;
        }

        const response = await axios(apiConfig);
        
        res.json({
            success: true,
            status: response.status,
            data: response.data,
            endpoint: endpoint,
            method: method,
            timestamp: new Date()
        });

    } catch (error) {
        console.error('API test failed:', error);
        res.status(error.response?.status || 500).json({
            success: false,
            status: error.response?.status || 'Error',
            error: error.message,
            data: error.response?.data || null,
            endpoint: req.body.endpoint,
            method: req.body.method || 'GET'
        });
    }
});

// Helper function to execute MCP tools
async function executeMCPTool(config, toolName, toolArgs) {
    return new Promise((resolve, reject) => {
        const { spawn } = require('child_process');
        const path = require('path');
        
        const toolboxPath = path.join(__dirname, 'scripts', 'toolbox');
        
        const toolbox = spawn(toolboxPath, [
            '--stdio', 
            '--prebuilt', 
            'looker'
        ], {
            env: {
                ...process.env,
                LOOKER_BASE_URL: config.lookerUrl,
                LOOKER_CLIENT_ID: config.clientId,
                LOOKER_CLIENT_SECRET: config.clientSecret
            },
            stdio: ['pipe', 'pipe', 'pipe']
        });

        let responseBuffer = '';
        let errorBuffer = '';
        let processCompleted = false;

        toolbox.stdout.on('data', (data) => {
            responseBuffer += data.toString();
        });

        toolbox.stderr.on('data', (data) => {
            errorBuffer += data.toString();
        });

        toolbox.on('close', (code) => {
            if (processCompleted) return;
            processCompleted = true;
            
            if (errorBuffer.includes('invalid tool name')) {
                reject(new Error(`Invalid tool name: ${toolName}. Available tools may include: get_models, get_explores, query, get_looks, run_look, query_sql, get_measures, get_dimensions, get_filters, get_parameters, query_url`));
                return;
            }
            
            try {
                const result = parseMCPResponse(responseBuffer);
                resolve(result);
            } catch (error) {
                reject(new Error(`Failed to parse MCP response: ${error.message}`));
            }
        });

        toolbox.on('error', (error) => {
            if (!processCompleted) {
                processCompleted = true;
                reject(new Error(`MCP process error: ${error.message}`));
            }
        });

        // Send the command
        const command = {
            jsonrpc: "2.0",
            id: 1,
            method: "tools/call",
            params: {
                name: toolName,
                arguments: toolArgs
            }
        };

        console.log(`Executing MCP tool: ${toolName}`, toolArgs);
        
        toolbox.stdin.write(JSON.stringify(command) + '\n');
        toolbox.stdin.end();

        // Timeout handling
        setTimeout(() => {
            if (!processCompleted) {
                processCompleted = true;
                toolbox.kill('SIGKILL');
                reject(new Error(`MCP tool ${toolName} timed out after 20 seconds`));
            }
        }, 20000);
    });
}

// Helper function to get Looker access token
async function getLookerAccessToken(config) {
    try {
        const axios = require('axios');
        
        const baseUrl = config.lookerUrl.replace(/\/$/, '');
        const loginUrl = `${baseUrl}/api/4.0/login`;
        
        const formData = new URLSearchParams();
        formData.append('client_id', config.clientId);
        formData.append('client_secret', config.clientSecret);
        
        const response = await axios.post(loginUrl, formData, {
            headers: {
                'Content-Type': 'application/x-www-form-urlencoded'
            },
            timeout: 15000
        });
        
        return response.data.access_token;
        
    } catch (error) {
        console.log('Failed to get access token:', error.message);
        return null;
    }
}

// Parse MCP response
function parseMCPResponse(responseBuffer) {
    if (!responseBuffer || responseBuffer.length < 10) {
        return { message: 'Empty response', data: null };
    }

    const lines = responseBuffer.split('\n').filter(line => line.trim());
    const results = [];
    
    for (const line of lines) {
        try {
            const response = JSON.parse(line);
            
            if (response.result && response.result.content) {
                for (const item of response.result.content) {
                    if (item.type === 'text' && item.text) {
                        try {
                            const data = JSON.parse(item.text);
                            results.push(data);
                        } catch (parseError) {
                            results.push({ raw_text: item.text });
                        }
                    } else {
                        results.push(item);
                    }
                }
            } else if (response.error) {
                throw new Error(response.error.message || 'MCP error');
            }
        } catch (lineError) {
            // Skip invalid lines
            continue;
        }
    }
    
    return results.length === 1 ? results[0] : results;
}

// Test available tools endpoint
app.get('/api/available-tools', async (req, res) => {
    try {
        const config = {
            lookerUrl: process.env.LOOKER_BASE_URL,
            clientId: process.env.LOOKER_CLIENT_ID,
            clientSecret: process.env.LOOKER_CLIENT_SECRET
        };

        if (!config.lookerUrl || !config.clientId || !config.clientSecret) {
            return res.json({
                success: false,
                error: 'Missing configuration',
                availableTools: [
                    'get_models', 'get_explores', 'query', 'get_looks', 
                    'run_look', 'query_sql', 'get_measures', 'get_dimensions',
                    'get_filters', 'get_parameters', 'query_url'
                ]
            });
        }

        // Try to get actual list of tools
        try {
            const tools = await executeMCPTool(config, 'list_tools', {});
            res.json({
                success: true,
                tools: tools,
                timestamp: new Date()
            });
        } catch (error) {
            // Fallback to known tools from documentation
            res.json({
                success: true,
                availableTools: [
                    'get_models', 'get_explores', 'query', 'get_looks', 
                    'run_look', 'query_sql', 'get_measures', 'get_dimensions',
                    'get_filters', 'get_parameters', 'query_url'
                ],
                note: 'Fallback list from documentation',
                error: error.message
            });
        }

    } catch (error) {
        res.status(500).json({
            success: false,
            error: error.message
        });
    }
});

console.log('API testing endpoints added to server');

console.log('API testing endpoints added to server');

// Start server with enhanced logging
app.listen(PORT, () => {
    console.log('');
    console.log('🚀 Enhanced Looker Health Diagnostic Assistant v2.0');
    console.log('=====================================================');
    console.log(`🏠 Server: http://localhost:${PORT}`);
    console.log(`📊 Basic Dashboard: http://localhost:${PORT}`);
    console.log(`⚡ Advanced Dashboard: http://localhost:${PORT}/dashboard`);
    console.log(`🔧 Health Check: http://localhost:${PORT}/api/health`);
    console.log(`⚙️ Configuration: http://localhost:${PORT}/api/config`);
    console.log('');
    
    // Configuration status
    console.log('📋 Configuration Status:');
    console.log(`   Looker URL: ${process.env.LOOKER_BASE_URL ? '✅ Configured' : '❌ Missing'}`);
    console.log(`   Client ID: ${process.env.LOOKER_CLIENT_ID ? '✅ Configured' : '❌ Missing'}`);
    console.log(`   Client Secret: ${process.env.LOOKER_CLIENT_SECRET ? '✅ Configured' : '❌ Missing'}`);
    console.log(`   Gemini API Key: ${process.env.GEMINI_API_KEY ? '✅ Configured (AI Enabled)' : '⚠️ Missing (AI Disabled)'}`);
    
    console.log('--- ENV DEBUG ---');
    console.log('Current working directory:', process.cwd());
    const geminiKey = process.env.GEMINI_API_KEY;
    console.log('GEMINI_API_KEY loaded:', !!geminiKey);
    console.log('GEMINI_API_KEY length:', geminiKey?.length || 0);
    console.log('GEMINI_API_KEY starts with:', geminiKey?.substring(0, 8) || 'NONE');
    console.log('GEMINI_API_KEY ends with:', geminiKey?.slice(-4) || 'NONE');
    console.log('==================');
    console.log('');
    
    // Feature status
    console.log('🚀 Enhanced Features:');
    console.log('   ✅ Query performance analysis');
    console.log('   ✅ LookML file fetching and optimization');
    console.log(`   ${process.env.GEMINI_API_KEY ? '✅' : '❌'} AI-powered query analysis`);
    console.log('   ✅ Real-time slow query monitoring');
    console.log('   ✅ Interactive dashboard with detailed breakdowns');
    console.log('');
    
    // Mode status
    if (process.env.USE_MOCK_DATA === 'true') {
        console.log('🎭 Demo Mode Active:');
        console.log('   • Using simulated data for demonstration');
        console.log('   • Set USE_MOCK_DATA=false for real Looker data');
    } else {
        console.log('🔗 Live Data Mode:');
        console.log('   • Will connect to real Looker instance');
        console.log('   • Real query performance analysis');
        if (!process.env.LOOKER_BASE_URL || !process.env.LOOKER_CLIENT_ID || !process.env.LOOKER_CLIENT_SECRET) {
            console.log('   ⚠️  Missing credentials - configure .env or enable demo mode');
        }
    }
    
    console.log('');
    console.log('🎯 Next Steps:');
    console.log('   1. Open http://localhost:' + PORT + ' for basic interface');
    console.log('   2. Open http://localhost:' + PORT + '/dashboard for advanced React UI');
    console.log('   3. Test connection with POST /api/test-mcp');
    console.log('   4. Run full diagnostic with POST /api/diagnostic/run');
    console.log('');
    
    if (!process.env.GEMINI_API_KEY) {
        console.log('💡 Pro Tip: Add GEMINI_API_KEY to .env for AI-powered query analysis');
        console.log('   Get your API key from: https://makersuite.google.com/app/apikey');
        console.log('');
    }
    
    console.log('🏆 Built for Looker Hackathon 2025 | MCP + AI Powered');
    console.log('📚 Documentation: Check README.md for troubleshooting');
});