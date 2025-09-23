// src/app.js - COMPLETE UPDATED FILE with loading screen fixes
const express = require('express');
const cors = require('cors');
const path = require('path');

// Configure dotenv to override existing environment variables and add error logging
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
        
        const connectorResults = await diagnostic.initializeConnectors();
        
        if (connectorResults.mcp) {
            try {
                const testConnectivity = await diagnostic.testLookerAPIConnectivity();
                res.json({
                    success: true,
                    message: 'MCP connection and Looker API test successful',
                    testResults: {
                        mcpConnected: connectorResults.mcp,
                        lookerApiConnected: connectorResults.lookerApi,
                        bigqueryConnected: connectorResults.bigquery?.success || false,
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
                details: connectorResults,
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

// UPDATED: Enhanced diagnostic endpoint with proper loading state management
app.post('/api/diagnostic/run', async (req, res) => {
    try {
        // Set up Server-Sent Events for real-time progress updates
        res.writeHead(200, {
            'Content-Type': 'text/event-stream',
            'Cache-Control': 'no-cache',
            'Connection': 'keep-alive',
            'X-Accel-Buffering': 'no' // Disable nginx buffering
        });
        
        // Send initial loading event
        res.write(`data: ${JSON.stringify({
            status: 'loading',
            message: 'Starting diagnostic...',
            progress: 0
        })}\n\n`);
        
        const config = {
            lookerUrl: process.env.LOOKER_BASE_URL,
            clientId: process.env.LOOKER_CLIENT_ID,
            clientSecret: process.env.LOOKER_CLIENT_SECRET
        };

        if (!config.lookerUrl || !config.clientId || !config.clientSecret) {
            res.write(`data: ${JSON.stringify({
                status: 'error',
                error: 'Missing Looker configuration',
                details: 'Please check your .env file'
            })}\n\n`);
            res.end();
            return;
        }

        console.log('🚀 Starting diagnostic with config:', {
            lookerUrl: config.lookerUrl,
            clientId: config.clientId ? 'Configured' : 'Missing',
            clientSecret: config.clientSecret ? 'Configured' : 'Missing'
        });

        const diagnostic = new QueryPerformanceDiagnostic(config);
        
        // Set up progress callback to send updates to client
        diagnostic.onProgress = (message, progress) => {
            if (!res.finished) {
                res.write(`data: ${JSON.stringify({
                    status: 'loading',
                    message: message,
                    progress: progress
                })}\n\n`);
            }
        };
        
        // Run the diagnostic
        const results = await diagnostic.runQueryPerformanceDiagnostic();
        
        console.log('📊 Diagnostic completed, returning results:', {
            slowQueries: results.slowQueryAnalysis?.totalSlowQueries || 0,
            models: Object.keys(results.slowQueryAnalysis?.byModel || {}).length,
            aiAnalysis: results.queryAnalysis?.length || 0
        });
        
        // Send final results
        res.write(`data: ${JSON.stringify({
            status: 'complete',
            results: results
        })}\n\n`);
        res.end();
        
    } catch (error) {
        console.error('Query diagnostic failed:', error);
        
        if (!res.finished) {
            res.write(`data: ${JSON.stringify({
                status: 'error',
                error: error.message,
                stack: process.env.NODE_ENV === 'development' ? error.stack : undefined
            })}\n\n`);
            res.end();
        }
    }
});

// Alternative endpoint for clients that don't support SSE
app.post('/api/diagnostic/run-simple', async (req, res) => {
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
        const results = await diagnostic.runQueryPerformanceDiagnostic();
        
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
        
        // Test connectivity through the diagnostic class
        const connectorResults = await diagnostic.initializeConnectors();
        
        res.json({ 
            success: connectorResults.mcp || connectorResults.lookerApi, 
            message: connectorResults.mcp ? 
                'MCP connectivity test passed' : 
                connectorResults.lookerApi ? 
                'Looker API connectivity test passed' : 
                'Both MCP and API connectivity tests failed',
            details: connectorResults,
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

        // Use the SQL analyzer directly for query analysis
        await diagnostic.initializeConnectors();
        const analysis = await diagnostic.sqlAnalyzer.analyzeRealQuery(queryData, diagnostic.mcpConnector);
        
        res.json({
            success: true,
            analysis: analysis,
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

// Helper function to execute MCP tools
async function executeMCPTool(config, toolName, toolArgs) {
    return new Promise((resolve, reject) => {
        const { spawn } = require('child_process');
        const path = require('path');
        
        // Try multiple possible toolbox locations
        const possiblePaths = [
            path.join(__dirname, 'scripts', 'toolbox'),
            path.join(__dirname, '..', 'scripts', 'toolbox'),
            path.join(process.cwd(), 'scripts', 'toolbox'),
            'toolbox'
        ];
        
        let toolboxPath = null;
        for (const testPath of possiblePaths) {
            try {
                const fs = require('fs');
                if (fs.existsSync(testPath)) {
                    toolboxPath = testPath;
                    console.log(`Found toolbox at: ${toolboxPath}`);
                    break;
                }
            } catch (e) {
                // Continue to next path
            }
        }
        
        if (!toolboxPath) {
            reject(new Error(`MCP toolbox not found. Searched paths: ${possiblePaths.join(', ')}`));
            return;
        }
        
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
                reject(new Error(`Invalid tool name: ${toolName}`));
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

        setTimeout(() => {
            if (!processCompleted) {
                processCompleted = true;
                toolbox.kill('SIGKILL');
                reject(new Error(`MCP tool ${toolName} timed out after 20 seconds`));
            }
        }, 20000);
    });
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

// Test MCP tool calls
app.post('/api/test-mcp-tool', async (req, res) => {
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
                details: 'Please check your .env file'
            });
        }

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

        res.json({
            success: true,
            availableTools: [
                'get_models', 'get_explores', 'query', 'get_looks', 
                'run_look', 'query_sql', 'get_measures', 'get_dimensions',
                'get_filters', 'get_parameters', 'query_url'
            ],
            note: 'Standard MCP Looker tools'
        });

    } catch (error) {
        res.status(500).json({
            success: false,
            error: error.message
        });
    }
});

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
    console.log('==================');
    console.log('');
    
    // Feature status
    console.log('🚀 Enhanced Features:');
    console.log('   ✅ Query performance analysis');
    console.log('   ✅ LookML file fetching and optimization');
    console.log(`   ${process.env.GEMINI_API_KEY ? '✅' : '❌'} AI-powered query analysis (Gemini 2.0)`);
    console.log('   ✅ Real-time slow query monitoring');
    console.log('   ✅ Server-Sent Events for progress tracking');
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
        console.log('   Get your API key from: https://aistudio.google.com/app/apikey');
        console.log('');
    }
    
    console.log('🏆 Built for Looker Hackathon 2025 | MCP + AI Powered');
    console.log('📚 Documentation: Check README.md for troubleshooting');
});

module.exports = app;