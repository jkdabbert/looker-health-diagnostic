const express = require('express');
const cors = require('cors');
const path = require('path');

// Configure dotenv to override existing environment variables and add error logging.
// This ensures that the values in your .env file are always used during local development.
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

        const diagnostic = new LookerHealthDiagnostic(config);
        const mcpConnected = await diagnostic.initializeMCP();
        
        if (mcpConnected) {
            // Try a simple MCP call
            try {
                const testData = await diagnostic.fetchDashboardBatch(0, 5);
                res.json({
                    success: true,
                    message: 'MCP connection successful',
                    testResults: {
                        connected: true,
                        sampleDataCount: testData ? testData.length : 0,
                        timestamp: new Date()
                    }
                });
            } catch (testError) {
                res.json({
                    success: false,
                    message: 'MCP connection established but test call failed',
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
// In your /api/diagnostic/run endpoint, make sure you're passing config correctly:
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
        await diagnostic.debugMCPParameters();
        
        res.json({ 
            success: true, 
            message: 'MCP parameter debugging completed - check server console for detailed output' 
        });
    } catch (error) {
        console.error('MCP debugging failed:', error);
        res.status(500).json({ error: error.message });
    }
});
app.post('/api/test-mcp-params', async (req, res) => {
    try {
        const config = {
            lookerUrl: process.env.LOOKER_BASE_URL,
            clientId: process.env.LOOKER_CLIENT_ID,
            clientSecret: process.env.LOOKER_CLIENT_SECRET
        };

        const diagnostic = new QueryPerformanceDiagnostic(config);
        await diagnostic.debugMCPParameters();
        
        res.json({ success: true, message: 'Check console for detailed MCP parameter test results' });
    } catch (error) {
        console.error('MCP parameter testing failed:', error);
        res.status(500).json({ error: error.message });
    }
});

// Get AI analysis for specific query
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

        const diagnostic = new LookerHealthDiagnostic(config);
        const queryData = {
            query_id: queryId,
            sql_query_text: queryText,
            runtime: runtime || 0
        };

        const analysis = await diagnostic.analyzeQueryWithAI(queryData);
        
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

        // For real data, we'd need to do a count query
        res.json({
            paginationEnabled: true,
            totalDashboards: 'Unknown (will be determined during fetch)',
            batchSize: 25,
            estimatedBatches: 'Unknown',
            mockData: false,
            status: 'Live mode - will paginate through all available dashboards'
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

app.post('/api/analyze-queries', async (req, res) => {
    // Analyze specific slow queries with AI
    const { queries, dashboards } = req.body;
    // Call your existing analyzeQueryWithAI method
  });
  
  app.post('/api/analyze-lookml', async (req, res) => {
    // Analyze LookML files with AI
    const { files, dashboards } = req.body;
    // Implement LookML-specific AI analysis
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
    console.log('GEMINI_API_KEY ends with:', geminiKey?.slice(-4) || 'NONE');
    console.log('==================');
    console.log('');
    
    // Feature status
    console.log('🚀 Enhanced Features:');
    console.log('   ✅ Pagination support for 336+ dashboards');
    console.log('   ✅ Multiple MCP tools (get_dashboards, get_looks, query)');
    console.log(`   ${process.env.GEMINI_API_KEY ? '✅' : '❌'} AI-powered query analysis`);
    console.log('   ✅ LookML optimization recommendations');
    console.log('   ✅ Performance metrics with query insights');
    console.log('');
    
    // Mode status
    if (process.env.USE_MOCK_DATA === 'true') {
        console.log('🎭 Demo Mode Active:');
        console.log('   • 336 simulated dashboards ready for pagination testing');
        console.log('   • 50 mock looks for comprehensive analysis');
        console.log('   • 20 sample slow queries for AI analysis');
        console.log('   • Perfect for hackathon demonstrations!');
        console.log('   • Set USE_MOCK_DATA=false for real Looker data');
    } else {
        console.log('🔗 Live Data Mode:');
        console.log('   • Will connect to real Looker instance');
        console.log('   • Paginated fetching of all available dashboards');
        console.log('   • Real query performance analysis');
        if (!process.env.LOOKER_BASE_URL || !process.env.LOOKER_CLIENT_ID || !process.env.LOOKER_CLIENT_SECRET) {
            console.log('   ⚠️  Missing credentials - configure .env or enable demo mode');
        }
    }
    
    console.log('');
    console.log('🎯 Next Steps:');
    console.log('   1. Open http://localhost:' + PORT + ' for basic interface');
    console.log('   2. Open http://localhost:' + PORT + '/dashboard for advanced React UI');
    console.log('   3. Test MCP connection with POST /api/test-mcp');
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