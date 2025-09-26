// src/app.js - Clean Enhanced Version with BigQuery Integration
const express = require('express');
const cors = require('cors');
const path = require('path');

// Configure dotenv to override existing environment variables
const dotenvResult = require('dotenv').config({ override: true });

if (dotenvResult.error) {
    console.error('⚠️  Error loading .env file:', dotenvResult.error);
}

// Import enhanced diagnostic engine
const { QueryPerformanceDiagnostic } = require('./diagnostic-engine');

// Import BigQuery connector
const { BigQueryConnector } = require('./connectors/bigquery-connector');

const app = express();
const PORT = process.env.PORT || 3000;

// Middleware
app.use(cors());
app.use(express.json());
app.use(express.static(path.join(__dirname, '..', 'public')));

// Initialize BigQuery connector
const bigQueryConnector = new BigQueryConnector({
    gcpProjectId: process.env.GCP_PROJECT_ID,
    serviceAccountPath: process.env.GOOGLE_APPLICATION_CREDENTIALS
});

// =============================================================================
// HEALTH AND CONFIG ENDPOINTS
// =============================================================================

// Health check endpoint
app.get('/api/health', (req, res) => {
    res.json({ 
        status: 'healthy', 
        timestamp: new Date(),
        version: '2.1.0-enhanced',
        environment: process.env.NODE_ENV || 'development',
        features: {
            pagination: true,
            aiAnalysis: !!process.env.GEMINI_API_KEY,
            mcpTools: ['get_dashboards', 'get_looks', 'query'],
            twoPhaseAnalysis: true,
            bigQueryIntegration: true,
            bigQueryOptimization: true
        },
        connectors: {
            bigQuery: bigQueryConnector.getStatus(),
            looker: 'Available',
            mcp: 'Available'
        }
    });
});

// Enhanced configuration endpoint with BigQuery info
app.get('/api/config', (req, res) => {
    res.json({
        lookerUrl: process.env.LOOKER_BASE_URL ? 'Configured' : 'Missing',
        clientId: process.env.LOOKER_CLIENT_ID ? 'Configured' : 'Missing',
        clientSecret: process.env.LOOKER_CLIENT_SECRET ? 'Configured' : 'Missing',
        geminiApiKey: process.env.GEMINI_API_KEY ? 'Configured' : 'Missing',
        gcpProjectId: process.env.GCP_PROJECT_ID ? 'Configured' : 'Missing',
        googleCredentials: process.env.GOOGLE_APPLICATION_CREDENTIALS ? 'Configured' : 'Missing',
        mockData: process.env.USE_MOCK_DATA === 'true',
        mcpEnabled: true,
        features: {
            aiRecommendations: process.env.ENABLE_AI_RECOMMENDATIONS !== 'false',
            performanceMonitoring: process.env.ENABLE_PERFORMANCE_MONITORING !== 'false',
            usageAnalytics: process.env.ENABLE_USAGE_ANALYTICS !== 'false',
            paginatedFetching: true,
            queryAnalysis: !!process.env.GEMINI_API_KEY,
            twoPhaseAnalysis: true,
            bigQueryOptimization: true,
            costAnalysis: true,
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
        },
        bigQuery: bigQueryConnector.getStatus()
    });
});

// =============================================================================
// BIGQUERY API ENDPOINTS
// =============================================================================

// Test BigQuery connection
app.post('/api/test-bigquery', async (req, res) => {
    try {
        console.log('🔍 Testing BigQuery connection...');
        
        const testResult = await bigQueryConnector.testConnection();
        
        console.log(`BigQuery test result: ${testResult.success ? '✅ Success' : '❌ Failed'}`);
        
        res.json({
            success: testResult.success,
            message: testResult.message,
            projectId: testResult.projectId,
            recommendation: testResult.recommendation,
            timestamp: new Date(),
            connectorStatus: bigQueryConnector.getStatus()
        });
        
    } catch (error) {
        console.error('❌ BigQuery test failed:', error);
        res.status(500).json({ 
            success: false,
            error: error.message,
            recommendation: 'Check your Google Cloud credentials and project access'
        });
    }
});

// Initialize BigQuery connection
app.post('/api/bigquery/initialize', async (req, res) => {
    try {
        console.log('🔌 Initializing BigQuery connection...');
        
        const initResult = await bigQueryConnector.initialize();
        
        res.json({
            success: initResult.success,
            message: initResult.message,
            projectId: initResult.projectId,
            hasCredentials: initResult.hasCredentials,
            fallbackMode: initResult.fallbackMode,
            recommendation: initResult.recommendation,
            timestamp: new Date()
        });
        
    } catch (error) {
        console.error('❌ BigQuery initialization failed:', error);
        res.status(500).json({ 
            success: false,
            error: error.message
        });
    }
});

// Get BigQuery connection details for a Looker connection
app.post('/api/bigquery/connection-details', async (req, res) => {
    try {
        const { connectionName } = req.body;
        
        if (!connectionName) {
            return res.status(400).json({
                error: 'Missing connectionName in request body'
            });
        }
        
        console.log(`🔍 Getting BigQuery connection details for: ${connectionName}`);
        
        // Initialize Looker API connector for connection details
        const config = {
            lookerUrl: process.env.LOOKER_BASE_URL,
            clientId: process.env.LOOKER_CLIENT_ID,
            clientSecret: process.env.LOOKER_CLIENT_SECRET
        };

        const diagnostic = new QueryPerformanceDiagnostic(config);
        await diagnostic.initializeConnectors();
        
        const connectionDetails = await bigQueryConnector.getConnectionDetails(
            connectionName, 
            diagnostic.lookerApiConnector
        );
        
        res.json({
            success: true,
            connectionName: connectionName,
            details: connectionDetails,
            timestamp: new Date()
        });
        
    } catch (error) {
        console.error('❌ Failed to get BigQuery connection details:', error);
        res.status(500).json({ 
            success: false,
            error: error.message
        });
    }
});

// Run comprehensive BigQuery optimization analysis
app.post('/api/bigquery/optimize', async (req, res) => {
    try {
        const { connectionName, projectId, dataset } = req.body;
        
        console.log('🚀 Starting BigQuery optimization analysis...');
        
        // Get connection details first
        let connectionDetails;
        
        if (connectionName) {
            // Get details from Looker connection
            const config = {
                lookerUrl: process.env.LOOKER_BASE_URL,
                clientId: process.env.LOOKER_CLIENT_ID,
                clientSecret: process.env.LOOKER_CLIENT_SECRET
            };

            const diagnostic = new QueryPerformanceDiagnostic(config);
            await diagnostic.initializeConnectors();
            
            connectionDetails = await bigQueryConnector.getConnectionDetails(
                connectionName, 
                diagnostic.lookerApiConnector
            );
        } else if (projectId) {
            // Use provided project details
            connectionDetails = {
                isBigQuery: true,
                projectId: projectId,
                dataset: dataset,
                connectionName: 'manual'
            };
        } else {
            return res.status(400).json({
                error: 'Must provide either connectionName or projectId'
            });
        }
        
        if (!connectionDetails.isBigQuery) {
            return res.json({
                applicable: false,
                reason: connectionDetails.reason || 'Not a BigQuery connection',
                connectionDetails: connectionDetails
            });
        }
        
        // Run the optimization analysis
        const optimizationResults = await bigQueryConnector.runOptimizationAnalysis(connectionDetails);
        
        console.log('✅ BigQuery optimization analysis completed');
        console.log(`📊 Found ${optimizationResults.recommendations?.length || 0} optimization recommendations`);
        
        res.json({
            success: true,
            applicable: true,
            connectionDetails: connectionDetails,
            results: optimizationResults,
            summary: {
                recommendations: optimizationResults.recommendations?.length || 0,
                costAnalysisQueries: optimizationResults.costAnalysis?.data?.length || 0,
                performanceAnalysisQueries: optimizationResults.performanceAnalysis?.data?.length || 0,
                tablesAnalyzed: optimizationResults.tableOptimization?.data?.length || 0,
                queryPatterns: optimizationResults.queryPatterns?.data?.length || 0
            },
            timestamp: new Date()
        });
        
    } catch (error) {
        console.error('❌ BigQuery optimization analysis failed:', error);
        res.status(500).json({ 
            success: false,
            applicable: false,
            error: error.message,
            stack: process.env.NODE_ENV === 'development' ? error.stack : undefined
        });
    }
});

// Get BigQuery cost analysis
app.post('/api/bigquery/cost-analysis', async (req, res) => {
    try {
        const { projectId, days = 7 } = req.body;
        
        if (!projectId) {
            return res.status(400).json({
                error: 'Missing projectId in request body'
            });
        }
        
        console.log(`💰 Running cost analysis for project ${projectId} (${days} days)...`);
        
        const connectionDetails = {
            isBigQuery: true,
            projectId: projectId
        };
        
        const costAnalysis = await bigQueryConnector.analyzeCosts(connectionDetails);
        
        res.json({
            success: costAnalysis.success,
            projectId: projectId,
            days: days,
            data: costAnalysis.data,
            source: costAnalysis.source,
            summary: costAnalysis.data ? {
                totalQueries: costAnalysis.data.length,
                totalCost: costAnalysis.data.reduce((sum, q) => sum + (q.estimated_cost_usd || 0), 0),
                highestCost: Math.max(...costAnalysis.data.map(q => q.estimated_cost_usd || 0))
            } : null,
            timestamp: new Date()
        });
        
    } catch (error) {
        console.error('❌ Cost analysis failed:', error);
        res.status(500).json({ 
            success: false,
            error: error.message
        });
    }
});

// Get BigQuery performance analysis
app.post('/api/bigquery/performance-analysis', async (req, res) => {
    try {
        const { projectId } = req.body;
        
        if (!projectId) {
            return res.status(400).json({
                error: 'Missing projectId in request body'
            });
        }
        
        console.log(`⚡ Running performance analysis for project ${projectId}...`);
        
        const connectionDetails = {
            isBigQuery: true,
            projectId: projectId
        };
        
        const performanceAnalysis = await bigQueryConnector.analyzePerformance(connectionDetails);
        
        res.json({
            success: performanceAnalysis.success,
            projectId: projectId,
            data: performanceAnalysis.data,
            source: performanceAnalysis.source,
            summary: performanceAnalysis.data ? {
                slowQueries: performanceAnalysis.data.length,
                avgDuration: performanceAnalysis.data.reduce((sum, q) => sum + (q.duration_seconds || 0), 0) / performanceAnalysis.data.length,
                longestQuery: Math.max(...performanceAnalysis.data.map(q => q.duration_seconds || 0))
            } : null,
            timestamp: new Date()
        });
        
    } catch (error) {
        console.error('❌ Performance analysis failed:', error);
        res.status(500).json({ 
            success: false,
            error: error.message
        });
    }
});

// =============================================================================
// DIAGNOSTIC ENDPOINTS (Enhanced with BigQuery integration)
// =============================================================================

// Enhanced pagination status endpoint
app.get('/api/pagination-status', (req, res) => {
    try {
        if (process.env.USE_MOCK_DATA === 'true') {
            res.json({
                paginationEnabled: true,
                totalDashboards: 336,
                batchSize: 25,
                estimatedBatches: 14,
                mockData: true,
                bigQueryIntegration: true,
                status: 'Demo mode - 336 mock dashboards ready with BigQuery optimization'
            });
            return;
        }

        res.json({
            paginationEnabled: true,
            totalDashboards: 'Unknown (will be determined during fetch)',
            batchSize: 25,
            estimatedBatches: 'Unknown',
            mockData: false,
            bigQueryIntegration: true,
            status: 'Live mode - will analyze actual slow queries with BigQuery optimization'
        });
        
    } catch (error) {
        res.status(500).json({ 
            error: error.message 
        });
    }
});

// Main enhanced diagnostic run with BigQuery integration
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

        console.log('🚀 Starting enhanced diagnostic with BigQuery integration...');
        
        const diagnostic = new QueryPerformanceDiagnostic(config);
        const results = await diagnostic.runQueryPerformanceDiagnostic();
        
        // Add BigQuery optimization results if available
        if (results.slowQueryAnalysis && results.slowQueryAnalysis.totalSlowQueries > 0) {
            console.log('🔍 Running BigQuery optimization analysis...');
            
            try {
                const bigQueryOptimizations = [];
                
                // Check if any connections are BigQuery
                if (diagnostic.lookerApiConnector && diagnostic.lookerApiConnector.getStatus().connected) {
                    const connections = await diagnostic.lookerApiConnector.makeApiRequest('/connections');
                    
                    for (const connection of connections || []) {
                        if (connection.dialect === 'bigquery_standard_sql') {
                            const connectionDetails = await bigQueryConnector.getConnectionDetails(
                                connection.name, 
                                diagnostic.lookerApiConnector
                            );
                            
                            if (connectionDetails.isBigQuery) {
                                const optimization = await bigQueryConnector.runOptimizationAnalysis(connectionDetails);
                                bigQueryOptimizations.push({
                                    connection: connection.name,
                                    ...optimization
                                });
                            }
                        }
                    }
                }
                
                if (bigQueryOptimizations.length > 0) {
                    results.bigQueryOptimization = {
                        connectionsAnalyzed: bigQueryOptimizations.length,
                        optimizations: bigQueryOptimizations,
                        totalRecommendations: bigQueryOptimizations.reduce((sum, opt) => 
                            sum + (opt.recommendations?.length || 0), 0)
                    };
                    
                    console.log(`✅ Added BigQuery optimization for ${bigQueryOptimizations.length} connections`);
                }
            } catch (bqError) {
                console.log('⚠️  BigQuery optimization failed, continuing without it:', bqError.message);
                results.bigQueryOptimization = {
                    error: 'BigQuery optimization failed',
                    message: bqError.message,
                    fallbackMode: true
                };
            }
        }
        
        console.log('📊 Enhanced diagnostic completed with BigQuery integration');
        
        res.json(results);
    } catch (error) {
        console.error('Enhanced diagnostic failed:', error);
        res.status(500).json({ 
            error: error.message,
            stack: process.env.NODE_ENV === 'development' ? error.stack : undefined
        });
    }
});

// Fast diagnostic scan (Phase 1) - Now uses the correct existing method
app.post('/api/diagnostic/scan', async (req, res) => {
    try {
        console.log('🔍 Starting fast diagnostic scan...');
        
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
        
        // Use the existing runFastScan method from your diagnostic-engine.js
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

// Test MCP connectivity with BigQuery info
app.post('/api/test-mcp', async (req, res) => {
    try {
        console.log('🧪 Testing MCP and BigQuery connectivity...');
        
        const config = {
            lookerUrl: process.env.LOOKER_BASE_URL,
            clientId: process.env.LOOKER_CLIENT_ID,
            clientSecret: process.env.LOOKER_CLIENT_SECRET
        };

        const diagnostic = new QueryPerformanceDiagnostic(config);
        const connectorResults = await diagnostic.initializeConnectors();
        
        // Test BigQuery connection as well
        const bigQueryTest = await bigQueryConnector.testConnection();
        
        res.json({
            success: true,
            connectors: {
                mcp: connectorResults.mcp?.success || false,
                lookerApi: connectorResults.lookerApi || false,
                bigQuery: bigQueryTest.success
            },
            bigQueryDetails: {
                projectId: bigQueryTest.projectId,
                connected: bigQueryTest.success,
                message: bigQueryTest.message
            },
            mcpTools: diagnostic.mcpConnector?.isConnected ? 
                ['get_dashboards', 'get_looks', 'query'] : [],
            timestamp: new Date(),
            message: 'MCP and BigQuery connectivity test completed'
        });
        
    } catch (error) {
        console.error('❌ MCP/BigQuery test failed:', error);
        res.status(500).json({ 
            success: false,
            error: error.message
        });
    }
});

// Batch AI analysis endpoint
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
        const analysisResults = [];
        
        for (const query of queriesToAnalyze) {
            try {
                const analysis = await diagnostic.sqlAnalyzer.analyzeRealQuery(
                    query, 
                    diagnostic.mcpConnector
                );
                
                analysisResults.push({
                    queryId: query.query_id || query.id,
                    analysis: analysis
                });
                
            } catch (queryError) {
                console.log(`Failed to analyze query ${query.query_id}: ${queryError.message}`);
                analysisResults.push({
                    queryId: query.query_id || query.id,
                    error: queryError.message,
                    analysis: null
                });
            }
        }
        
        res.json({
            success: true,
            results: analysisResults,
            queriesAnalyzed: analysisResults.length,
            aiPowered: !!process.env.GEMINI_API_KEY,
            timestamp: new Date(),
            message: !process.env.GEMINI_API_KEY ? 
                'AI analysis disabled - set GEMINI_API_KEY to enable' : 
                'Batch analysis completed successfully'
        });
        
    } catch (error) {
        console.error('❌ Batch analysis failed:', error);
        res.status(500).json({ 
            success: false,
            error: error.message
        });
    }
});

// Add this endpoint to your app.js file
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
        
        // Limit to prevent timeouts
        const queriesToAnalyze = queries.slice(0, maxQueries);
        const analysisResults = [];
        
        // For now, return mock analysis results
        for (const query of queriesToAnalyze) {
            analysisResults.push({
                queryId: query.query_id || query.id,
                analysis: {
                    issues: [
                        {
                            type: 'performance',
                            severity: 'medium',
                            description: `Query runtime of ${query.runtime_seconds}s indicates optimization opportunity`,
                            recommendation: 'Consider adding indexes or implementing PDT'
                        }
                    ],
                    recommendations: [
                        {
                            action: 'Create PDT for frequently accessed data',
                            expectedImprovement: '60-75% faster execution',
                            effort: 'medium'
                        }
                    ],
                    aiPowered: !!process.env.GEMINI_API_KEY
                }
            });
        }
        
        res.json({
            success: true,
            results: analysisResults,
            queriesAnalyzed: analysisResults.length,
            aiPowered: !!process.env.GEMINI_API_KEY,
            timestamp: new Date(),
            message: 'Batch analysis completed successfully'
        });
        
    } catch (error) {
        console.error('❌ Batch analysis failed:', error);
        res.status(500).json({ 
            success: false,
            error: error.message
        });
    }
});

// AI analysis endpoints
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

        console.log(`🤖 Starting AI analysis for query ${queryId}...`);
        
        const diagnostic = new QueryPerformanceDiagnostic(config);
        await diagnostic.initializeConnectors();
        
        diagnostic.sqlAnalyzer.setLookerApiConnector(diagnostic.lookerApiConnector);
        
        const analysis = await diagnostic.sqlAnalyzer.analyzeRealQuery(
            query, 
            diagnostic.mcpConnector
        );
        
        console.log('✅ AI analysis completed');
        res.json({
            success: true,
            queryId: queryId,
            analysis: analysis,
            timestamp: new Date(),
            aiPowered: !!process.env.GEMINI_API_KEY,
            bigQueryContext: bigQueryConnector.getStatus().connected
        });
        
    } catch (error) {
        console.error('❌ AI analysis failed:', error);
        res.status(500).json({ 
            success: false,
            error: error.message
        });
    }
});

// =============================================================================
// STATIC ROUTES
// =============================================================================

// Serve main dashboard
app.get('/', (req, res) => {
    res.sendFile(path.join(__dirname, '..', 'public', 'index.html'));
});

// Serve advanced React dashboard
app.get('/dashboard', (req, res) => {
    res.sendFile(path.join(__dirname, '..', 'public', 'dashboard.html'));
});

// =============================================================================
// SERVER STARTUP
// =============================================================================

app.listen(PORT, () => {
    console.log(`🚀 Looker Health Diagnostic Assistant (Enhanced with BigQuery)`);
    console.log(`📍 Server: http://localhost:${PORT}`);
    console.log(`📊 Dashboard: http://localhost:${PORT}`);
    console.log(`🔧 Health Check: http://localhost:${PORT}/api/health`);
    console.log(`⚙️  Configuration: http://localhost:${PORT}/api/config`);
    console.log('');
    
    // Display configuration status
    console.log('🔍 Configuration Status:');
    console.log(`   LOOKER_BASE_URL: ${process.env.LOOKER_BASE_URL ? '✅ Configured' : '⚠️  Missing'}`);
    console.log(`   LOOKER_CLIENT_ID: ${process.env.LOOKER_CLIENT_ID ? '✅ Configured' : '⚠️  Missing'}`);
    console.log(`   LOOKER_CLIENT_SECRET: ${process.env.LOOKER_CLIENT_SECRET ? '✅ Configured' : '⚠️  Missing'}`);
    console.log(`   GCP_PROJECT_ID: ${process.env.GCP_PROJECT_ID ? '✅ Configured' : '⚠️  Missing (BigQuery features limited)'}`);
    console.log(`   GOOGLE_APPLICATION_CREDENTIALS: ${process.env.GOOGLE_APPLICATION_CREDENTIALS ? '✅ Configured' : '⚠️  Missing (BigQuery will use mock data)'}`);
    console.log(`   GEMINI_API_KEY: ${process.env.GEMINI_API_KEY ? '✅ Configured' : '⚠️  Missing (AI features disabled)'}`);
    
    console.log('');
    console.log('🎯 BigQuery Endpoints:');
    console.log('   POST /api/test-bigquery                    # Test BigQuery connection');
    console.log('   POST /api/bigquery/initialize              # Initialize BigQuery');
    console.log('   POST /api/bigquery/optimize                # Run full optimization analysis');
    console.log('   POST /api/bigquery/cost-analysis           # Cost analysis only');
    console.log('   POST /api/bigquery/performance-analysis    # Performance analysis only');
    console.log('   POST /api/bigquery/connection-details      # Get connection info');
    console.log('');
    console.log('📝 Next Steps:');
    console.log('   1. Test BigQuery: npm run test-bigquery');
    console.log('   2. Test connectivity: curl -X POST http://localhost:3000/api/test-bigquery');
    console.log('   3. Visit dashboard: http://localhost:3000');
    console.log('');
    console.log('🚀 Enhanced with BigQuery cost optimization and performance analysis!');
});