// src/app.js - Complete file with two-phase diagnostic support
const express = require('express');
const cors = require('cors');
const path = require('path');

// Configure dotenv to override existing environment variables
const dotenvResult = require('dotenv').config({ override: true });

if (dotenvResult.error) {
    console.error('⚠️  Error loading .env file:', dotenvResult.error);
}

// Import enhanced diagnostic engine - ONLY IMPORT, DO NOT REDECLARE
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
            mcpTools: ['get_dashboards', 'get_looks', 'query'],
            twoPhaseAnalysis: true
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
            twoPhaseAnalysis: true,
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

// NEW: Phase 1 - Fast diagnostic scan (no AI)
app.post('/api/diagnostic/scan', async (req, res) => {
    try {
        console.log('🔍 Starting fast diagnostic scan...');
        
        const config = {
            lookerUrl: process.env.LOOKER_BASE_URL,
            clientId: process.env.LOOKER_CLIENT_ID,
            clientSecret: process.env.LOOKER_CLIENT_SECRET
        };

        // Validate configuration
        if (!config.lookerUrl || !config.clientId || !config.clientSecret) {
            return res.status(400).json({
                error: 'Missing Looker configuration',
                details: 'Please check your .env file for LOOKER_BASE_URL, LOOKER_CLIENT_ID, and LOOKER_CLIENT_SECRET'
            });
        }

        const diagnostic = new QueryPerformanceDiagnostic(config);
        const results = await diagnostic.runFastScan();
        
        console.log('✅ Fast scan completed successfully');
        console.log(`   Found ${results.slowQuerySummary?.totalSlowQueries || 0} slow queries`);
        console.log(`   Scan duration: ${Math.round(results.scanDuration / 1000)}s`);
        
        res.json(results);
        
    } catch (error) {
        console.error('❌ Fast scan failed:', error);
        res.status(500).json({ 
            error: error.message,
            stack: process.env.NODE_ENV === 'development' ? error.stack : undefined
        });
    }
});

// NEW: Phase 2 - AI analysis for selected queries
app.post('/api/diagnostic/analyze-batch', async (req, res) => {
    try {
        const { queries, maxQueries = 5 } = req.body;
        
        if (!queries || !Array.isArray(queries)) {
            return res.status(400).json({
                error: 'queries must be an array'
            });
        }

        const config = {
            lookerUrl: process.env.LOOKER_BASE_URL,
            clientId: process.env.LOOKER_CLIENT_ID,
            clientSecret: process.env.LOOKER_CLIENT_SECRET
        };

        console.log(`🤖 Starting batch AI analysis for ${Math.min(queries.length, maxQueries)} queries...`);
        
        const diagnostic = new QueryPerformanceDiagnostic(config);
        await diagnostic.initializeConnectors();
        
        // Set up SQL analyzer with Looker API connector for AI analysis
        diagnostic.sqlAnalyzer.setLookerApiConnector(diagnostic.lookerApiConnector);
        
        // Limit to prevent timeouts and excessive API usage
        const queriesToAnalyze = queries.slice(0, maxQueries);
        const analyses = [];
        
        for (const [index, query] of queriesToAnalyze.entries()) {
            try {
                console.log(`   Analyzing query ${index + 1}/${queriesToAnalyze.length}: ${query.query_id}`);
                
                const analysis = await diagnostic.sqlAnalyzer.analyzeRealQuery(
                    query, 
                    diagnostic.mcpConnector
                );
                analyses.push(analysis);
                
            } catch (queryError) {
                console.error(`   Failed to analyze query ${query.query_id}:`, queryError.message);
                analyses.push({
                    queryId: query.query_id,
                    slug: query.slug,
                    runtime: query.runtime_seconds,
                    model: query.model,
                    explore: query.explore,
                    error: queryError.message,
                    fallback: true,
                    analysis: {
                        issues: [{
                            type: 'analysis_error',
                            severity: 'medium',
                            description: 'Could not perform detailed analysis',
                            recommendation: 'Manual investigation required'
                        }],
                        recommendations: [{
                            type: 'manual_review',
                            priority: 'medium',
                            title: 'Manual Query Review',
                            description: 'Review this query manually in Looker',
                            expectedImprovement: 'Unknown',
                            effort: 'medium'
                        }]
                    }
                });
            }
        }
        
        console.log(`✅ Batch AI analysis completed: ${analyses.length} queries processed`);
        res.json({
            success: true,
            totalQueries: queries.length,
            analyzedQueries: analyses.length,
            analyses: analyses,
            timestamp: new Date(),
            aiPowered: !!process.env.GEMINI_API_KEY
        });
        
    } catch (error) {
        console.error('❌ Batch analysis failed:', error);
        res.status(500).json({ 
            success: false,
            error: error.message
        });
    }
});

// NEW: Single query AI analysis
app.post('/api/diagnostic/analyze-query', async (req, res) => {
    try {
        const { queryId, query } = req.body;
        
        if (!queryId || !query) {
            return res.status(400).json({
                error: 'Missing queryId or query data'
            });
        }

        const config = {
            lookerUrl: process.env.LOOKER_BASE_URL,
            clientId: process.env.LOOKER_CLIENT_ID,
            clientSecret: process.env.LOOKER_CLIENT_SECRET
        };

        console.log(`🤖 Starting AI analysis for single query ${queryId}...`);
        
        const diagnostic = new QueryPerformanceDiagnostic(config);
        await diagnostic.initializeConnectors();
        
        // Set up SQL analyzer with Looker API connector
        diagnostic.sqlAnalyzer.setLookerApiConnector(diagnostic.lookerApiConnector);
        
        const analysis = await diagnostic.sqlAnalyzer.analyzeRealQuery(
            query, 
            diagnostic.mcpConnector
        );
        
        console.log('✅ Single query AI analysis completed');
        res.json({
            success: true,
            queryId: queryId,
            analysis: analysis,
            timestamp: new Date(),
            aiPowered: !!process.env.GEMINI_API_KEY
        });
        
    } catch (error) {
        console.error('❌ Single query AI analysis failed:', error);
        res.status(500).json({ 
            success: false,
            error: error.message
        });
    }
});

// EXISTING: Full diagnostic endpoint (backwards compatibility)
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

        console.log('🚀 Starting full diagnostic with config:', {
            lookerUrl: config.lookerUrl,
            clientId: config.clientId ? 'Configured' : 'Missing',
            clientSecret: config.clientSecret ? 'Configured' : 'Missing'
        });

        const diagnostic = new QueryPerformanceDiagnostic(config);
        const results = await diagnostic.runQueryPerformanceDiagnostic();
        
        console.log('📊 Full diagnostic completed, returning results:', {
            slowQueries: results.slowQueryAnalysis?.totalSlowQueries || 0,
            models: Object.keys(results.slowQueryAnalysis?.byModel || {}).length,
            aiAnalysis: results.queryAnalysis?.length || 0
        });
        
        res.json(results);
    } catch (error) {
        console.error('Full diagnostic failed:', error);
        res.status(500).json({ 
            error: error.message,
            stack: process.env.NODE_ENV === 'development' ? error.stack : undefined
        });
    }
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
        
        if (connectorResults.mcp || connectorResults.lookerApi) {
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

// Debug MCP parameters endpoint
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

// Test MCP tool calls with correct tool names
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
                details: 'Please check your .env file for LOOKER_BASE_URL, LOOKER_CLIENT_ID, and LOOKER_CLIENT_SECRET'
            });
        }

        // Initialize diagnostic and connectors
        const diagnostic = new QueryPerformanceDiagnostic(config);
        await diagnostic.initializeConnectors();
        
        // Execute MCP tool call
        const result = await diagnostic.mcpConnector.executeTool(toolName, toolArgs || {});
        
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

        const diagnostic = new QueryPerformanceDiagnostic(config);
        await diagnostic.initializeConnectors();
        
        try {
            const tools = await diagnostic.mcpConnector.listAvailableTools();
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
    
    console.log('');
    
    // Feature status
    console.log('🚀 Enhanced Features:');
    console.log('   ✅ Two-Phase Analysis (Fast Scan + Selective AI)');
    console.log('   ✅ Query performance analysis');
    console.log('   ✅ LookML file fetching and optimization');
    console.log(`   ${process.env.GEMINI_API_KEY ? '✅' : '❌'} AI-powered query analysis`);
    console.log('   ✅ Real-time slow query monitoring');
    console.log('   ✅ Interactive dashboard with detailed breakdowns');
    console.log('   ✅ MCP integration with fallback to direct API');
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
    console.log('   1. Open http://localhost:' + PORT + '/dashboard for React UI');
    console.log('   2. Use Two-Phase Diagnostic tab for optimal performance');
    console.log('   3. Test connection with POST /api/test-mcp');
    console.log('   4. Run fast scan first, then selective AI analysis');
    console.log('');
    
    if (!process.env.GEMINI_API_KEY) {
        console.log('💡 Pro Tip: Add GEMINI_API_KEY to .env for AI-powered query analysis');
        console.log('   Get your API key from: https://makersuite.google.com/app/apikey');
        console.log('');
    }
    
    console.log('🏆 Built for Looker Hackathon 2025 | Two-Phase + AI Powered');
    console.log('📚 Documentation: Check README.md for troubleshooting');
});