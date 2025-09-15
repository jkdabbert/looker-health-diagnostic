// Production Looker Health Diagnostic Assistant - Real MCP Integration Only
// Comprehensive tool with pagination, MCP tools, and AI query analysis

class LookerHealthDiagnostic {
    constructor(config) {
        this.config = config;
        this.mcpClient = null;
        this.healthMetrics = {
            performance: 0,
            governance: 0,
            usage: 0,
            dataQuality: 0,
            security: 0
        };
        this.diagnosticResults = [];
        this.mcpMetrics = {
            connectionType: 'Unknown',
            callsPerformed: 0,
            totalDashboards: 0,
            totalLooks: 0,
            avgResponseTime: 0,
            errors: 0
        };
    }

    async initializeMCP() {
        try {
            console.log('🔌 Initializing MCP connection...');
            
            // Validate required configuration
            if (!this.config.lookerUrl || !this.config.clientId || !this.config.clientSecret) {
                throw new Error('Missing required Looker configuration');
            }
            
            this.mcpClient = {
                baseUrl: this.config.lookerUrl,
                clientId: this.config.clientId,
                isConnected: true,
                initialized: false,
                startTime: Date.now()
            };
            
            console.log('✅ MCP client configured');
            return true;
        } catch (error) {
            console.error('❌ MCP connection failed:', error);
            return false;
        }
    }

    // Test Gemini connection for debugging
    async testGeminiConnection() {
        console.log('🧪 Testing Gemini AI connection...');
        
        if (!process.env.GEMINI_API_KEY) {
            console.log('⚠️ No Gemini API key configured');
            return false;
        }
        
        const axios = require('axios');
        try {
            const response = await axios.post(
                `https://generativelanguage.googleapis.com/v1beta/models/gemini-1.5-flash:generateContent?key=${process.env.GEMINI_API_KEY}`,
                {
                    contents: [{
                        parts: [{ text: "Test connection" }]
                    }]
                }
            );
            console.log('✅ Gemini AI connection successful');
            return true;
        } catch (error) {
            console.log('❌ Gemini AI connection failed:', error.response?.data?.error?.message || error.message);
            return false;
        }
    }

    async fetchAllDashboardsWithPagination() {
        console.log('📊 Fetching all dashboards with pagination...');
        
        const allDashboards = [];
        const allLooks = [];
        let offset = 0;
        const limit = 25;
        let hasMore = true;

        console.log('🔗 Starting paginated MCP data collection...');
        
        // Fetch dashboards with pagination
        while (hasMore) {
            try {
                console.log(`📄 Fetching dashboards batch: offset=${offset}, limit=${limit}`);
                const dashboardBatch = await this.fetchDashboardBatch(offset, limit);
                
                if (dashboardBatch && dashboardBatch.length > 0) {
                    allDashboards.push(...dashboardBatch);
                    offset += limit;
                    this.mcpMetrics.callsPerformed++;
                    
                    // Stop if we got less than limit (last page)
                    if (dashboardBatch.length < limit) {
                        hasMore = false;
                    }
                    
                    // Add small delay to be respectful to the API
                    await new Promise(resolve => setTimeout(resolve, 100));
                } else {
                    hasMore = false;
                }
            } catch (error) {
                console.log(`⚠️ Error fetching batch at offset ${offset}: ${error.message}`);
                this.mcpMetrics.errors++;
                hasMore = false;
            }
        }

        // Fetch looks separately
        try {
            console.log('👀 Fetching looks via MCP...');
            const looks = await this.fetchLooksViaMCP();
            if (looks && looks.length > 0) {
                allLooks.push(...looks);
                this.mcpMetrics.callsPerformed++;
            }
        } catch (error) {
            console.log('⚠️ Could not fetch looks:', error.message);
            this.mcpMetrics.errors++;
        }

        // Fetch query performance data
        let queryMetrics = [];
        try {
            console.log('🔍 Fetching query performance data...');
            queryMetrics = await this.fetchQueryMetrics();
            this.mcpMetrics.callsPerformed++;
        } catch (error) {
            console.log('⚠️ Could not fetch query metrics:', error.message);
            this.mcpMetrics.errors++;
        }

        console.log(`✅ Successfully collected ${allDashboards.length} dashboards and ${allLooks.length} looks via MCP`);
        
        this.mcpMetrics.totalDashboards = allDashboards.length;
        this.mcpMetrics.totalLooks = allLooks.length;
        this.mcpMetrics.connectionType = 'Live MCP Integration (Paginated)';
        this.mcpMetrics.avgResponseTime = this.calculateAvgResponseTime();

        const transformedDashboards = allDashboards.map(d => this.transformLookerDashboard(d, queryMetrics));
        const transformedLooks = allLooks.map(l => this.transformLook(l));

        return {
            dashboards: transformedDashboards,
            looks: transformedLooks,
            queryMetrics: queryMetrics,
            systemMetrics: {
                totalDashboards: transformedDashboards.length,
                totalLooks: transformedLooks.length,
                totalUsers: this.estimateUserCount(allDashboards),
                avgSystemLoad: this.calculateSystemLoad(allDashboards),
                dataFreshnessScore: this.calculateDataFreshness(transformedDashboards),
                connectionType: this.mcpMetrics.connectionType,
                mcpMetrics: this.mcpMetrics
            }
        };
    }

    async fetchDashboardBatch(offset, limit) {
        return new Promise((resolve, reject) => {
            const { spawn } = require('child_process');
            const path = require('path');
            
            const toolboxPath = path.join(__dirname, '..', 'scripts', 'toolbox');
            
            const toolbox = spawn(toolboxPath, [
                '--stdio', 
                '--prebuilt', 
                'looker'
            ], {
                env: {
                    ...process.env,
                    LOOKER_BASE_URL: this.config.lookerUrl,
                    LOOKER_CLIENT_ID: this.config.clientId,
                    LOOKER_CLIENT_SECRET: this.config.clientSecret,
                    LOOKER_VERIFY_SSL: 'true'
                },
                stdio: ['pipe', 'pipe', 'pipe']
            });

            let responseBuffer = '';
            let processCompleted = false;

            toolbox.stdout.on('data', (data) => {
                responseBuffer += data.toString();
            });

            toolbox.stderr.on('data', (data) => {
                console.log(`MCP STDERR: ${data.toString().trim()}`);
            });

            toolbox.on('error', (error) => {
                reject(new Error(`MCP process error: ${error.message}`));
            });

            toolbox.on('close', (code) => {
                if (processCompleted) return;
                processCompleted = true;
                
                try {
                    const dashboards = this.processMCPDashboardResponse(responseBuffer);
                    resolve(dashboards);
                } catch (error) {
                    reject(error);
                }
            });

            const command = {
                jsonrpc: "2.0",
                id: 1,
                method: "tools/call",
                params: {
                    name: "get_dashboards",
                    arguments: { 
                        limit: limit,
                        offset: offset
                    }
                }
            };

            try {
                toolbox.stdin.write(JSON.stringify(command) + '\n');
                toolbox.stdin.end();
            } catch (error) {
                reject(error);
            }

            setTimeout(() => {
                if (!processCompleted) {
                    toolbox.kill('SIGKILL');
                    reject(new Error('MCP batch timeout'));
                }
            }, 15000);
        });
    }

    async fetchLooksViaMCP() {
        return new Promise((resolve, reject) => {
            const { spawn } = require('child_process');
            const path = require('path');
            
            const toolboxPath = path.join(__dirname, '..', 'scripts', 'toolbox');
            
            const toolbox = spawn(toolboxPath, [
                '--stdio', 
                '--prebuilt', 
                'looker'
            ], {
                env: {
                    ...process.env,
                    LOOKER_BASE_URL: this.config.lookerUrl,
                    LOOKER_CLIENT_ID: this.config.clientId,
                    LOOKER_CLIENT_SECRET: this.config.clientSecret
                },
                stdio: ['pipe', 'pipe', 'pipe']
            });

            let responseBuffer = '';
            let processCompleted = false;

            toolbox.stdout.on('data', (data) => {
                responseBuffer += data.toString();
            });

            toolbox.on('close', (code) => {
                if (processCompleted) return;
                processCompleted = true;
                
                try {
                    const looks = this.processMCPLooksResponse(responseBuffer);
                    resolve(looks);
                } catch (error) {
                    resolve([]);
                }
            });

            const command = {
                jsonrpc: "2.0",
                id: 2,
                method: "tools/call",
                params: {
                    name: "get_looks",
                    arguments: { limit: 100 }
                }
            };

            toolbox.stdin.write(JSON.stringify(command) + '\n');
            toolbox.stdin.end();

            setTimeout(() => {
                if (!processCompleted) {
                    toolbox.kill('SIGKILL');
                    resolve([]);
                }
            }, 10000);
        });
    }
// Enhanced query collection methods for your diagnostic-engine.js

async fetchQueryMetricsEnhanced() {
    console.log('🔍 Fetching enhanced query metrics with LookML context...');
    
    return new Promise((resolve, reject) => {
        const { spawn } = require('child_process');
        const path = require('path');
        
        const toolboxPath = path.join(__dirname, '..', 'scripts', 'toolbox');
        
        const toolbox = spawn(toolboxPath, [
            '--stdio', 
            '--prebuilt', 
            'looker'
        ], {
            env: {
                ...process.env,
                LOOKER_BASE_URL: this.config.lookerUrl,
                LOOKER_CLIENT_ID: this.config.clientId,
                LOOKER_CLIENT_SECRET: this.config.clientSecret
            },
            stdio: ['pipe', 'pipe', 'pipe']
        });

        let responseBuffer = '';
        let processCompleted = false;

        toolbox.stdout.on('data', (data) => {
            responseBuffer += data.toString();
        });

        toolbox.on('close', (code) => {
            if (processCompleted) return;
            processCompleted = true;
            
            try {
                const queries = this.processEnhancedQueryResponse(responseBuffer);
                resolve(queries);
            } catch (error) {
                resolve([]);
            }
        });

        // Enhanced query that captures LookML context
        const command = {
            jsonrpc: "2.0",
            id: 3,
            method: "tools/call",
            params: {
                name: "query",
                arguments: {
                    query: `
                        SELECT
                            h.query_id,
                            h.id as history_id,
                            CAST(h.runtime AS DECIMAL(19,4)) AS runtime_seconds,
                            h.created_date,
                            h.dashboard_id,
                            h.look_id,
                            q.model,
                            q.explore,
                            q.sql,
                            q.slug as query_slug,
                            d.title as dashboard_title,
                            d.description as dashboard_description,
                            l.title as look_title,
                            u.first_name || ' ' || u.last_name as user_name,
                            h.cache_key,
                            h.status,
                            qm.execute_main_query as main_query_time,
                            qm.acquire_connection as connection_time,
                            qm.prepare_connection as prepare_time,
                            qm.execute_main_query / CAST(h.runtime AS DECIMAL(19,4)) * 100 as query_percentage
                        FROM history h
                        LEFT JOIN query q ON h.query_id = q.id
                        LEFT JOIN dashboard d ON h.dashboard_id = d.id
                        LEFT JOIN look l ON h.look_id = l.id
                        LEFT JOIN user u ON h.user_id = u.id
                        LEFT JOIN query_metrics qm ON h.slug = qm.query_task_id
                        WHERE 
                            CAST(h.runtime AS DECIMAL(19,4)) > 3.0
                            AND h.created_date >= CURRENT_DATE - 7
                            AND h.status = 'complete'
                            AND q.model IS NOT NULL
                        ORDER BY h.runtime DESC
                        LIMIT 100
                    `
                }
            }
        };

        toolbox.stdin.write(JSON.stringify(command) + '\n');
        toolbox.stdin.end();

        setTimeout(() => {
            if (!processCompleted) {
                toolbox.kill('SIGKILL');
                resolve([]);
            }
        }, 15000);
    });
}

// Enhanced query processing with LookML context
processEnhancedQueryResponse(responseBuffer) {
    const queries = [];
    const lines = responseBuffer.split('\n');
    
    for (const line of lines) {
        if (line.trim()) {
            try {
                const response = JSON.parse(line.trim());
                
                if (response.result && response.result.content && Array.isArray(response.result.content)) {
                    for (const item of response.result.content) {
                        if (item.type === 'text' && item.text) {
                            try {
                                const queryData = JSON.parse(item.text);
                                
                                const enhancedQuery = {
                                    query_id: queryData.query_id,
                                    history_id: queryData.history_id,
                                    runtime_seconds: parseFloat(queryData.runtime_seconds || 0),
                                    created_date: queryData.created_date,
                                    
                                    // Context information
                                    dashboard_id: queryData.dashboard_id,
                                    dashboard_title: queryData.dashboard_title,
                                    look_id: queryData.look_id,
                                    look_title: queryData.look_title,
                                    user_name: queryData.user_name,
                                    
                                    // LookML information
                                    model: queryData.model,
                                    explore: queryData.explore,
                                    sql: queryData.sql,
                                    
                                    // Performance breakdown
                                    main_query_time: parseFloat(queryData.main_query_time || 0),
                                    connection_time: parseFloat(queryData.connection_time || 0),
                                    prepare_time: parseFloat(queryData.prepare_time || 0),
                                    query_percentage: parseFloat(queryData.query_percentage || 0),
                                    
                                    // Technical details
                                    cache_key: queryData.cache_key,
                                    status: queryData.status,
                                    query_slug: queryData.query_slug
                                };
                                
                                if (enhancedQuery.runtime_seconds > 3) {
                                    queries.push(enhancedQuery);
                                }
                            } catch (e) {
                                console.log('Could not parse enhanced query data');
                            }
                        }
                    }
                    break;
                }
            } catch (e) {
                // Skip non-JSON lines
            }
        }
    }
    
    console.log(`🔍 Found ${queries.length} slow queries with LookML context`);
    return queries;
}

// Get LookML files related to slow queries
async fetchLookMLForSlowQueries(slowQueries) {
    console.log('📝 Fetching LookML files for optimization analysis...');
    
    const uniqueModels = [...new Set(slowQueries.map(q => q.model).filter(Boolean))];
    const lookmlFiles = [];
    
    for (const model of uniqueModels) {
        try {
            const modelFiles = await this.fetchLookMLFilesForModel(model);
            lookmlFiles.push(...modelFiles);
        } catch (error) {
            console.log(`⚠️ Could not fetch LookML for model ${model}:`, error.message);
        }
    }
    
    return lookmlFiles;
}

async fetchLookMLFilesForModel(modelName) {
    return new Promise((resolve, reject) => {
        const { spawn } = require('child_process');
        const path = require('path');
        
        const toolboxPath = path.join(__dirname, '..', 'scripts', 'toolbox');
        
        const toolbox = spawn(toolboxPath, [
            '--stdio', 
            '--prebuilt', 
            'looker'
        ], {
            env: {
                ...process.env,
                LOOKER_BASE_URL: this.config.lookerUrl,
                LOOKER_CLIENT_ID: this.config.clientId,
                LOOKER_CLIENT_SECRET: this.config.clientSecret
            },
            stdio: ['pipe', 'pipe', 'pipe']
        });

        let responseBuffer = '';
        let processCompleted = false;

        toolbox.stdout.on('data', (data) => {
            responseBuffer += data.toString();
        });

        toolbox.on('close', (code) => {
            if (processCompleted) return;
            processCompleted = true;
            
            try {
                const files = this.processLookMLResponse(responseBuffer, modelName);
                resolve(files);
            } catch (error) {
                resolve([]);
            }
        });

        const command = {
            jsonrpc: "2.0",
            id: 4,
            method: "tools/call",
            params: {
                name: "get_lookml_model",
                arguments: {
                    model_name: modelName
                }
            }
        };

        toolbox.stdin.write(JSON.stringify(command) + '\n');
        toolbox.stdin.end();

        setTimeout(() => {
            if (!processCompleted) {
                toolbox.kill('SIGKILL');
                resolve([]);
            }
        }, 10000);
    });
}

// Analyze slow queries and suggest LookML optimizations
async analyzeLookMLOptimizations(slowQueries, lookmlFiles = []) {
    console.log('🏗️ Analyzing LookML optimization opportunities...');
    
    const optimizations = [];
    
    // Group queries by model/explore
    const queryGroups = this.groupQueriesByModelExplore(slowQueries);
    
    for (const [modelExplore, queries] of Object.entries(queryGroups)) {
        const [model, explore] = modelExplore.split('.');
        
        // Find related LookML files
        const relatedLookML = lookmlFiles.filter(file => 
            file.model === model && (file.type === 'explore' || file.explore === explore)
        );
        
        // Analyze common patterns in slow queries
        const optimization = await this.generateLookMLOptimization(
            model, 
            explore, 
            queries, 
            relatedLookML
        );
        
        if (optimization) {
            optimizations.push(optimization);
        }
    }
    
    return optimizations;
}

groupQueriesByModelExplore(queries) {
    const groups = {};
    
    queries.forEach(query => {
        if (query.model && query.explore) {
            const key = `${query.model}.${query.explore}`;
            if (!groups[key]) {
                groups[key] = [];
            }
            groups[key].push(query);
        }
    });
    
    return groups;
}

async generateLookMLOptimization(model, explore, queries, lookmlFiles) {
    const avgRuntime = queries.reduce((sum, q) => sum + q.runtime_seconds, 0) / queries.length;
    const totalQueries = queries.length;
    
    // Common slow query patterns
    const commonIssues = this.identifyCommonSlowQueryPatterns(queries);
    const lookmlSuggestions = this.generateLookMLSuggestions(commonIssues, lookmlFiles);
    
    return {
        model: model,
        explore: explore,
        impact: {
            totalSlowQueries: totalQueries,
            averageRuntime: Math.round(avgRuntime * 100) / 100,
            totalTimeWasted: Math.round(queries.reduce((sum, q) => sum + q.runtime_seconds, 0)),
            affectedDashboards: [...new Set(queries.map(q => q.dashboard_title).filter(Boolean))],
            affectedUsers: [...new Set(queries.map(q => q.user_name).filter(Boolean))]
        },
        issues: commonIssues,
        lookmlOptimizations: lookmlSuggestions,
        priority: this.calculateOptimizationPriority(totalQueries, avgRuntime),
        estimatedImprovements: this.estimatePerformanceGains(commonIssues)
    };
}

identifyCommonSlowQueryPatterns(queries) {
    const issues = [];
    
    // Analyze SQL patterns
    queries.forEach(query => {
        const sql = query.sql || '';
        
        if (sql.includes('SELECT *')) {
            issues.push({
                type: 'inefficient_select',
                count: 1,
                description: 'Queries using SELECT * instead of specific columns',
                severity: 'medium'
            });
        }
        
        if (sql.toLowerCase().includes('group by') && !sql.toLowerCase().includes('aggregate_table')) {
            issues.push({
                type: 'missing_aggregate_table',
                count: 1,
                description: 'Frequent aggregations without aggregate tables',
                severity: 'high'
            });
        }
        
        if ((sql.match(/join/gi) || []).length > 3) {
            issues.push({
                type: 'complex_joins',
                count: 1,
                description: 'Complex multi-table joins without optimization',
                severity: 'high'
            });
        }
        
        if (query.runtime_seconds > 10 && !sql.toLowerCase().includes('index')) {
            issues.push({
                type: 'missing_indexes',
                count: 1,
                description: 'Very slow queries likely missing proper indexes',
                severity: 'critical'
            });
        }
    });
    
    // Consolidate duplicate issues
    return this.consolidateIssues(issues);
}

generateLookMLSuggestions(issues, lookmlFiles) {
    const suggestions = [];
    
    issues.forEach(issue => {
        switch (issue.type) {
            case 'missing_aggregate_table':
                suggestions.push({
                    type: 'aggregate_table',
                    suggestion: 'Create aggregate tables for common grouping patterns',
                    implementation: 'Add aggregate_table definitions in your model file',
                    expectedImprovement: '60-80% reduction in query time',
                    lookmlCode: `
aggregate_table: daily_sales_summary {
  query: {
    dimensions: [date_date, product_category]
    measures: [total_sales, order_count]
  }
  materialization: {
    datagroup_trigger: sales_datagroup
  }
}`
                });
                break;
                
            case 'complex_joins':
                suggestions.push({
                    type: 'join_optimization',
                    suggestion: 'Optimize join relationships and add proper indexes',
                    implementation: 'Review join logic and add sql_on conditions with indexes',
                    expectedImprovement: '40-60% reduction in query time',
                    lookmlCode: `
join: orders {
  type: left_outer
  sql_on: \${users.id} = \${orders.user_id} ;;
  # Add index on orders.user_id for better performance
}`
                });
                break;
                
            case 'missing_indexes':
                suggestions.push({
                    type: 'database_indexes',
                    suggestion: 'Add database indexes for frequently joined/filtered columns',
                    implementation: 'Work with DBA to add composite indexes',
                    expectedImprovement: '70-90% reduction in query time',
                    lookmlCode: `
# In your view file, document recommended indexes:
# CREATE INDEX idx_orders_user_date ON orders (user_id, order_date);
# CREATE INDEX idx_products_category ON products (category, status);`
                });
                break;
                
            case 'inefficient_select':
                suggestions.push({
                    type: 'column_optimization',
                    suggestion: 'Use specific dimensions instead of broad SELECT statements',
                    implementation: 'Review dashboard tiles and remove unused dimensions',
                    expectedImprovement: '20-30% reduction in query time',
                    lookmlCode: `
# Instead of selecting all dimensions, be specific:
dimension: essential_field {
  type: string
  sql: \${TABLE}.essential_field ;;
  # Hide unnecessary fields from explores
}`
                });
                break;
        }
    });
    
    return suggestions;
}

calculateOptimizationPriority(queryCount, avgRuntime) {
    const impactScore = queryCount * avgRuntime;
    
    if (impactScore > 100) return 'critical';
    if (impactScore > 50) return 'high';
    if (impactScore > 20) return 'medium';
    return 'low';
}

consolidateIssues(issues) {
    const consolidated = {};
    
    issues.forEach(issue => {
        if (consolidated[issue.type]) {
            consolidated[issue.type].count += issue.count;
        } else {
            consolidated[issue.type] = { ...issue };
        }
    });
    
    return Object.values(consolidated);
}
    async fetchQueryMetrics() {
        return new Promise((resolve, reject) => {
            const { spawn } = require('child_process');
            const path = require('path');
            
            const toolboxPath = path.join(__dirname, '..', 'scripts', 'toolbox');
            
            const toolbox = spawn(toolboxPath, [
                '--stdio', 
                '--prebuilt', 
                'looker'
            ], {
                env: {
                    ...process.env,
                    LOOKER_BASE_URL: this.config.lookerUrl,
                    LOOKER_CLIENT_ID: this.config.clientId,
                    LOOKER_CLIENT_SECRET: this.config.clientSecret
                },
                stdio: ['pipe', 'pipe', 'pipe']
            });
    
            let responseBuffer = '';
            let processCompleted = false;
    
            toolbox.stdout.on('data', (data) => {
                responseBuffer += data.toString();
            });
    
            toolbox.on('close', (code) => {
                if (processCompleted) return;
                processCompleted = true;
                
                try {
                    const queries = this.processMCPQueryResponse(responseBuffer);
                    resolve(queries);
                } catch (error) {
                    resolve([]);
                }
            });
    
            // Query using the actual field structure from your Looker instance
            const command = {
                jsonrpc: "2.0",
                id: 3,
                method: "tools/call",
                params: {
                    name: "query",
                    arguments: {
                        query: `
                            SELECT
                                query.id AS query_id,
                                CAST(history.runtime AS DECIMAL(19,4)) AS runtime,
                                query.sql AS sql_query_text,
                                COALESCE(query_metrics.execute_main_query, 0) AS main_query_time
                            FROM query_metrics
                            LEFT JOIN history ON query_metrics.query_task_id = history.slug
                            LEFT JOIN query ON history.query_id = query.id
                            WHERE CAST(history.runtime AS DECIMAL(19,4)) > 3.0
                            ORDER BY history.runtime DESC
                            LIMIT 50
                        `
                    }
                }
            };
    
            toolbox.stdin.write(JSON.stringify(command) + '\n');
            toolbox.stdin.end();
    
            setTimeout(() => {
                if (!processCompleted) {
                    toolbox.kill('SIGKILL');
                    resolve([]);
                }
            }, 10000);
        });
    }

    processMCPDashboardResponse(responseBuffer) {
        const dashboards = [];
        const lines = responseBuffer.split('\n');
        
        for (const line of lines) {
            if (line.trim()) {
                try {
                    const response = JSON.parse(line.trim());
                    
                    if (response.result && response.result.content && Array.isArray(response.result.content)) {
                        for (const item of response.result.content) {
                            if (item.type === 'text' && item.text) {
                                try {
                                    const dashboard = JSON.parse(item.text);
                                    dashboards.push(dashboard);
                                } catch (e) {
                                    // Skip invalid dashboard entries
                                }
                            }
                        }
                        break;
                    }
                } catch (e) {
                    // Skip non-JSON lines
                }
            }
        }
        
        return dashboards;
    }

    processMCPLooksResponse(responseBuffer) {
        const looks = [];
        const lines = responseBuffer.split('\n');
        
        for (const line of lines) {
            if (line.trim()) {
                try {
                    const response = JSON.parse(line.trim());
                    
                    if (response.result && response.result.content && Array.isArray(response.result.content)) {
                        for (const item of response.result.content) {
                            if (item.type === 'text' && item.text) {
                                try {
                                    const look = JSON.parse(item.text);
                                    looks.push(look);
                                } catch (e) {
                                    // Skip invalid look entries
                                }
                            }
                        }
                        break;
                    }
                } catch (e) {
                    // Skip non-JSON lines
                }
            }
        }
        
        return looks;
    }

    processMCPQueryResponse(responseBuffer) {
        console.log('🔍 DEBUG: Query response buffer length:', responseBuffer.length);
        
        const queries = [];
        const lines = responseBuffer.split('\n');
        
        for (const line of lines) {
            if (line.trim()) {
                try {
                    const response = JSON.parse(line.trim());
                    
                    if (response.result && response.result.content && Array.isArray(response.result.content)) {
                        for (const item of response.result.content) {
                            if (item.type === 'text' && item.text) {
                                try {
                                    // Parse the query result data
                                    const queryData = JSON.parse(item.text);
                                    
                                    // Transform to expected format
                                    const query = {
                                        query_id: queryData.query_id || queryData.id,
                                        runtime: parseFloat(queryData.runtime || queryData.history_runtime || 0),
                                        sql_query_text: queryData.sql_query_text || queryData.sql || 'N/A'
                                    };
                                    
                                    if (query.runtime > 3) {
                                        queries.push(query);
                                    }
                                } catch (e) {
                                    console.log('Could not parse query data:', item.text.substring(0, 100));
                                }
                            }
                        }
                        break;
                    }
                } catch (e) {
                    // Skip non-JSON lines
                }
            }
        }
        
        console.log(`🔍 DEBUG: Parsed ${queries.length} slow queries from query_metrics explore`);
        return queries;
    }

    transformLook(look) {
        return {
            id: look.id,
            title: look.title || 'Untitled Look',
            lastAccessed: new Date(look.last_accessed_at || look.updated_at || Date.now()),
            queryCount: look.view_count || 0,
            avgLoadTime: this.estimateLoadTime(look),
            userCount: look.favorite_count || 0
        };
    }

    transformLookerDashboard(dashboard, queryMetrics = []) {
        const id = dashboard.id || dashboard.dashboard_id;
        const title = dashboard.title || dashboard.name || dashboard.dashboard_title || 'Untitled Dashboard';
        
        // Find related slow queries
        const relatedSlowQueries = queryMetrics.filter(q => 
            q.sql_query_text && (
                q.sql_query_text.toLowerCase().includes(title.toLowerCase().replace(/\s+/g, '_')) ||
                q.dashboard_id === id
            )
        );

        return {
            id: id,
            title: title,
            tiles: dashboard.dashboard_elements?.length || dashboard.element_count || 0,
            lastAccessed: new Date(dashboard.updated_at || dashboard.last_updated_at || dashboard.last_accessed || Date.now()),
            queryCount: dashboard.query_count || dashboard.view_count || 0,
            avgLoadTime: dashboard.avg_load_time || this.estimateLoadTime(dashboard),
            userCount: dashboard.user_count || dashboard.view_count || 0,
            dataSourceCount: this.extractDataSourceCount(dashboard),
            filterCount: dashboard.dashboard_filters?.length || dashboard.filter_count || 0,
            hasSlowQueries: relatedSlowQueries.length > 0,
            slowQueryDetails: relatedSlowQueries,
            missingDocumentation: !dashboard.description || dashboard.description.length < 10,
            unusedFilters: this.detectUnusedFilters(dashboard),
            lookmlErrors: this.detectLookMLErrors(dashboard)
        };
    }

    // AI analysis methods
    async analyzeSlowQueriesWithAI(dashboards, queryMetrics) {
        console.log('🤖 Analyzing slow queries with AI...');
        
        const slowQueries = queryMetrics.filter(q => q.runtime > 3);
        console.log(`Found ${slowQueries.length} slow queries (runtime > 3s)`);
        
        const aiAnalysis = [];

        for (const query of slowQueries.slice(0, 10)) {
            try {
                const analysis = await this.analyzeQueryWithAI(query);
                aiAnalysis.push(analysis);
            } catch (error) {
                console.log(`⚠️ Failed to analyze query ${query.query_id}:`, error.message);
            }
        }

        console.log(`AI analyzed ${aiAnalysis.length} queries`);
        return aiAnalysis;
    }

    async analyzeQueryWithAI(queryData) {
        if (!process.env.GEMINI_API_KEY) {
            return this.generateLocalQueryAnalysis(queryData);
        }

        try {
            const axios = require('axios');
            
            const prompt = `You are a Looker performance expert. Analyze this slow-running query and provide optimization recommendations:

Query ID: ${queryData.query_id}
Runtime: ${queryData.runtime} seconds
SQL: ${queryData.sql_query_text || 'N/A'}

Please provide analysis in this JSON format:
{
  "queryId": "${queryData.query_id}",
  "runtime": ${queryData.runtime},
  "issues": [
    {
      "type": "performance_issue_type",
      "description": "What's causing the slowness",
      "severity": "high/medium/low"
    }
  ],
  "recommendations": [
    {
      "type": "optimization_type",
      "action": "Specific action to take",
      "expectedImprovement": "Expected performance gain",
      "effort": "high/medium/low"
    }
  ],
  "lookmlSuggestions": [
    {
      "suggestion": "LookML optimization suggestion",
      "reason": "Why this will help performance"
    }
  ]
}

Focus on practical optimizations like indexing, query structure, LookML improvements, and caching strategies.`;

            const response = await axios.post(
                `https://generativelanguage.googleapis.com/v1beta/models/gemini-1.5-flash:generateContent?key=${process.env.GEMINI_API_KEY}`,
                {
                    contents: [{
                        parts: [{ text: prompt }]
                    }]
                },
                {
                    timeout: 30000,
                    headers: {
                        'Content-Type': 'application/json'
                    }
                }
            );

            const geminiText = response.data.candidates[0].content.parts[0].text;
            const jsonMatch = geminiText.match(/\{[\s\S]*\}/);
            
            if (jsonMatch) {
                return JSON.parse(jsonMatch[0]);
            } else {
                throw new Error('Could not parse Gemini response');
            }
        } catch (error) {
            console.log('⚠️ Gemini analysis failed, using local analysis');
            return this.generateLocalQueryAnalysis(queryData);
        }
    }

    generateLocalQueryAnalysis(queryData) {
        const sqlText = queryData.sql_query_text || '';
        const issues = [];
        const recommendations = [];
        const lookmlSuggestions = [];

        // Basic SQL analysis
        if (sqlText.includes('SELECT *')) {
            issues.push({
                type: 'inefficient_select',
                description: 'Query uses SELECT * which fetches unnecessary columns',
                severity: 'medium'
            });
            recommendations.push({
                type: 'column_optimization',
                action: 'Specify only needed columns in SELECT statement',
                expectedImprovement: '20-40% faster query execution',
                effort: 'low'
            });
        }

        if (sqlText.toLowerCase().includes('join') && !sqlText.toLowerCase().includes('index')) {
            issues.push({
                type: 'missing_indexes',
                description: 'Complex joins without proper indexing',
                severity: 'high'
            });
            recommendations.push({
                type: 'indexing',
                action: 'Add indexes on join columns in the database',
                expectedImprovement: '50-80% faster joins',
                effort: 'medium'
            });
        }

        if (queryData.runtime > 5) {
            lookmlSuggestions.push({
                suggestion: 'Consider adding aggregate tables or materialized views',
                reason: 'Very slow queries benefit from pre-computed aggregations'
            });
            lookmlSuggestions.push({
                suggestion: 'Implement incremental PDTs (Persistent Derived Tables)',
                reason: 'Reduces computation time for large datasets'
            });
        }

        return {
            queryId: queryData.query_id,
            runtime: queryData.runtime,
            issues,
            recommendations,
            lookmlSuggestions
        };
    }

    // Analysis methods
    async analyzePerformance(dashboards, queryMetrics = []) {
        console.log('⚡ Analyzing performance metrics...');
        
        let performanceScore = 100;
        const issues = [];

        // Analyze slow queries with AI if available
        if (queryMetrics.length > 0) {
            const slowQueryAnalysis = await this.analyzeSlowQueriesWithAI(dashboards, queryMetrics);
            
            for (const analysis of slowQueryAnalysis) {
                performanceScore -= 15;
                
                const lookmlSuggestions = analysis.lookmlSuggestions.map(s => s.suggestion).join('; ');
                const aiRecommendations = analysis.recommendations.map(r => r.action).join('; ');
                
                issues.push({
                    type: 'performance',
                    severity: 'high',
                    dashboard: `Query ${analysis.queryId}`,
                    issue: `Slow query (${analysis.runtime}s): ${analysis.issues.map(i => i.description).join(', ')}`,
                    recommendation: `AI Analysis: ${aiRecommendations}. LookML: ${lookmlSuggestions}`,
                    aiPowered: true,
                    queryAnalysis: analysis
                });
            }
        }

        // Dashboard performance analysis
        dashboards.forEach(dash => {
            if (dash.avgLoadTime > 3.0) {
                performanceScore -= 10;
                issues.push({
                    type: 'performance',
                    severity: 'high',
                    dashboard: dash.title,
                    issue: `Slow loading time: ${dash.avgLoadTime}s`,
                    recommendation: 'Optimize queries, consider data modeling improvements, add proper indexing'
                });
            }

            if (dash.hasSlowQueries) {
                performanceScore -= 5;
                issues.push({
                    type: 'performance',
                    severity: 'medium',
                    dashboard: dash.title,
                    issue: 'Contains slow-running queries',
                    recommendation: 'Review query logic, add appropriate indexes, consider aggregate tables'
                });
            }

            if (dash.tiles > 15) {
                performanceScore -= 5;
                issues.push({
                    type: 'performance',
                    severity: 'low',
                    dashboard: dash.title,
                    issue: `High tile count: ${dash.tiles}`,
                    recommendation: 'Consider breaking into multiple focused dashboards'
                });
            }
        });

        this.healthMetrics.performance = Math.max(0, performanceScore);
        return issues;
    }

    analyzeGovernance(dashboards, looks = []) {
        console.log('📋 Analyzing governance compliance...');
        
        let governanceScore = 100;
        const issues = [];

        dashboards.forEach(dash => {
            if (dash.missingDocumentation) {
                governanceScore -= 15;
                issues.push({
                    type: 'governance',
                    severity: 'high',
                    dashboard: dash.title,
                    issue: 'Missing documentation',
                    recommendation: 'Add descriptions for dashboard and key metrics'
                });
            }

            if (dash.unusedFilters.length > 0) {
                governanceScore -= 8;
                issues.push({
                    type: 'governance',
                    severity: 'medium',
                    dashboard: dash.title,
                    issue: `Unused filters: ${dash.unusedFilters.join(', ')}`,
                    recommendation: 'Remove unused filters to improve user experience'
                });
            }

            if (dash.lookmlErrors.length > 0) {
                governanceScore -= 12;
                issues.push({
                    type: 'governance',
                    severity: 'high',
                    dashboard: dash.title,
                    issue: `LookML issues: ${dash.lookmlErrors.join(', ')}`,
                    recommendation: 'Fix LookML errors to ensure data accuracy'
                });
            }
        });

        this.healthMetrics.governance = Math.max(0, governanceScore);
        return issues;
    }

    analyzeUsage(dashboards, looks = []) {
        console.log('👥 Analyzing usage patterns...');
        
        let usageScore = 100;
        const issues = [];
        const thirtyDaysAgo = new Date();
        thirtyDaysAgo.setDate(thirtyDaysAgo.getDate() - 30);

        dashboards.forEach(dash => {
            if (dash.lastAccessed < thirtyDaysAgo) {
                usageScore -= 20;
                issues.push({
                    type: 'usage',
                    severity: 'high',
                    dashboard: dash.title,
                    issue: 'Low usage - not accessed in 30 days',
                    recommendation: 'Consider archiving or promoting to increase adoption'
                });
            }

            if (dash.userCount < 3) {
                usageScore -= 10;
                issues.push({
                    type: 'usage',
                    severity: 'medium',
                    dashboard: dash.title,
                    issue: `Low user engagement: ${dash.userCount} users`,
                    recommendation: 'Review dashboard relevance and promote to target audience'
                });
            }

            if (dash.queryCount > 0 && dash.userCount > 0) {
                const queriesPerUser = dash.queryCount / dash.userCount;
                if (queriesPerUser < 1) {
                    usageScore -= 5;
                    issues.push({
                        type: 'usage',
                        severity: 'low',
                        dashboard: dash.title,
                        issue: 'Low interaction rate per user',
                        recommendation: 'Improve dashboard interactivity and user training'
                    });
                }
            }
        });

        this.healthMetrics.usage = Math.max(0, usageScore);
        return issues;
    }

    analyzeDataQuality(dashboards, systemMetrics, queryMetrics = []) {
        console.log('🔍 Analyzing data quality...');
        
        let dataQualityScore = systemMetrics.dataFreshnessScore || 85;
        const issues = [];

        dashboards.forEach(dash => {
            if (dash.dataSourceCount > 5) {
                dataQualityScore -= 8;
                issues.push({
                    type: 'dataQuality',
                    severity: 'medium',
                    dashboard: dash.title,
                    issue: `Multiple data sources: ${dash.dataSourceCount}`,
                    recommendation: 'Consider data consolidation for consistency'
                });
            }
        });

        if (systemMetrics.dataFreshnessScore && systemMetrics.dataFreshnessScore < 70) {
            issues.push({
                type: 'dataQuality',
                severity: 'high',
                dashboard: 'System-wide',
                issue: 'Data freshness below threshold',
                recommendation: 'Review ETL processes and data refresh schedules'
            });
        }

        const errorProneQueries = queryMetrics.filter(q => q.runtime > 10);
        if (errorProneQueries.length > 5) {
            dataQualityScore -= 15;
            issues.push({
                type: 'dataQuality',
                severity: 'high',
                dashboard: 'Query Performance',
                issue: `${errorProneQueries.length} queries taking over 10 seconds`,
                recommendation: 'Optimize slow queries to prevent timeouts and data quality issues'
            });
        }

        this.healthMetrics.dataQuality = Math.max(0, dataQualityScore);
        return issues;
    }

    // AI recommendations
    async generateAIRecommendations(allIssues, queryAnalysis = []) {
        console.log('🤖 Generating AI-powered recommendations...');
        
        if (process.env.GEMINI_API_KEY) {
            try {
                return await this.generateGeminiRecommendations(allIssues, queryAnalysis);
            } catch (error) {
                console.log('⚠️ Gemini API failed, using local analysis');
            }
        }
        
        return this.generateLocalRecommendations(allIssues, queryAnalysis);
    }

    async generateGeminiRecommendations(allIssues, queryAnalysis) {
        const axios = require('axios');
        
        const prompt = `You are a Looker dashboard health expert. Analyze these dashboard issues and query performance data to provide strategic recommendations:

Issues Found:
${JSON.stringify(allIssues.slice(0, 20), null, 2)}

Query Analysis (AI-powered):
${JSON.stringify(queryAnalysis.slice(0, 5), null, 2)}

Provide actionable recommendations in this exact JSON format:
{
  "priorities": [
    {
      "priority": 1,
      "title": "Brief title",
      "description": "Detailed description including query optimization insights",
                  "estimatedImpact": "High/Medium/Low",
            "estimatedEffort": "High/Medium/Low", 
            "timeframe": "1-2 weeks",
            "includesQueryOptimization": true/false
        }
    ],
    "strategicRecommendations": [
        {
            "category": "Performance/Governance/Usage/QueryOptimization",
            "recommendation": "Specific actionable recommendation",
            "rationale": "Why this is important",
            "expectedOutcome": "What will improve",
            "lookmlImpact": "How this affects LookML development"
        }
    ],
    "queryOptimizationPlan": {
        "immediateActions": ["List of quick wins"],
        "mediumTermGoals": ["List of medium-term optimizations"],
        "longTermStrategy": "Overall optimization strategy"
    }
}

Focus on practical, actionable recommendations that address the most critical issues first.`;

        const response = await axios.post(
            `https://generativelanguage.googleapis.com/v1beta/models/gemini-1.5-flash:generateContent?key=${process.env.GEMINI_API_KEY}`,
            {
                contents: [{
                    parts: [{ text: prompt }]
                }]
            },
            {
                timeout: 30000,
                headers: {
                    'Content-Type': 'application/json'
                }
            }
        );

        const geminiText = response.data.candidates[0].content.parts[0].text;
        const jsonMatch = geminiText.match(/\{[\s\S]*\}/);
        
        if (jsonMatch) {
            const parsed = JSON.parse(jsonMatch[0]);
            
            return {
                ...parsed,
                overallHealthGrade: this.calculateOverallHealth(),
                nextReviewDate: new Date(Date.now() + 30 * 24 * 60 * 60 * 1000),
                aiPowered: true,
                queryAnalysisIncluded: queryAnalysis.length > 0
            };
        } else {
            throw new Error('Could not parse Gemini response');
        }
    }

    generateLocalRecommendations(allIssues, queryAnalysis) {
        const priorities = [];
        const strategicRecommendations = [];

        const highImpactIssues = allIssues.filter(issue => 
            issue.severity === 'high' && 
            (issue.type === 'performance' || issue.type === 'governance')
        );

        const queryOptimizationIssues = allIssues.filter(issue => 
            issue.aiPowered || issue.type === 'performance'
        );

        if (queryOptimizationIssues.length > 0) {
            priorities.push({
                priority: 1,
                title: 'Optimize Query Performance with AI Insights',
                description: `Found ${queryOptimizationIssues.length} performance issues with AI-analyzed solutions. Focus on query optimization, indexing, and LookML improvements.`,
                estimatedImpact: 'High',
                estimatedEffort: 'Medium',
                timeframe: '1-2 weeks',
                includesQueryOptimization: true
            });
        }

        if (highImpactIssues.length > 0) {
            priorities.push({
                priority: 2,
                title: 'Address Critical Issues',
                description: `Found ${highImpactIssues.length} high-priority issues affecting user experience and data reliability`,
                estimatedImpact: 'High',
                estimatedEffort: 'Medium',
                timeframe: '2-3 weeks',
                includesQueryOptimization: false
            });
        }

        if (queryAnalysis.length > 0) {
            strategicRecommendations.push({
                category: 'QueryOptimization',
                recommendation: 'Implement systematic query performance monitoring and optimization',
                rationale: 'AI analysis revealed specific optimization opportunities in slow queries',
                expectedOutcome: 'Reduce query execution times by 40-70%',
                lookmlImpact: 'Requires LookML updates for aggregate tables and efficient joins'
            });
        }

        const queryOptimizationPlan = {
            immediateActions: [
                'Add indexes on frequently joined columns',
                'Replace SELECT * with specific column selections',
                'Implement basic query caching'
            ],
            mediumTermGoals: [
                'Create aggregate tables for common calculations',
                'Implement incremental PDTs',
                'Optimize complex join patterns'
            ],
            longTermStrategy: 'Establish AI-powered query performance monitoring and automatic optimization suggestions'
        };

        return {
            priorities,
            strategicRecommendations,
            queryOptimizationPlan,
            overallHealthGrade: this.calculateOverallHealth(),
            nextReviewDate: new Date(Date.now() + 30 * 24 * 60 * 60 * 1000),
            aiPowered: false,
            queryAnalysisIncluded: queryAnalysis.length > 0
        };
    }

    // Helper methods
    calculateAvgResponseTime() {
        if (this.mcpMetrics.callsPerformed === 0) return 0;
        return Math.round((Date.now() - this.mcpClient.startTime) / this.mcpMetrics.callsPerformed);
    }

    extractDataSourceCount(dashboard) {
        if (!dashboard.dashboard_elements) return 1;
        
        const sources = new Set();
        dashboard.dashboard_elements.forEach(element => {
            if (element.look && element.look.model) {
                sources.add(element.look.model);
            }
            if (element.query && element.query.model) {
                sources.add(element.query.model);
            }
        });
        
        return sources.size || 1;
    }

    detectUnusedFilters(dashboard) {
        const unusedFilters = [];
        if (dashboard.dashboard_filters) {
            dashboard.dashboard_filters.forEach(filter => {
                if (!filter.default_value) {
                    unusedFilters.push(filter.name || filter.title || 'unnamed_filter');
                }
            });
        }
        return unusedFilters;
    }

    detectLookMLErrors(dashboard) {
        const errors = [];
        // This would typically check for real LookML validation errors
        // For now, we'll detect based on common patterns
        if (dashboard.dashboard_elements) {
            dashboard.dashboard_elements.forEach(element => {
                if (element.query && element.query.client_id && !element.query.model) {
                    errors.push('missing_model_reference');
                }
            });
        }
        return errors;
    }

    estimateLoadTime(item) {
        // Estimate based on complexity indicators
        const tileCount = item.dashboard_elements?.length || 1;
        const baseTime = 0.5;
        const complexityFactor = tileCount * 0.1;
        return Math.round((baseTime + complexityFactor) * 10) / 10;
    }

    estimateUserCount(dashboards) {
        // Estimate total users based on individual dashboard user counts
        const uniqueUsers = new Set();
        dashboards.forEach(dash => {
            if (dash.user_count) {
                for (let i = 0; i < dash.user_count; i++) {
                    uniqueUsers.add(`user_${dash.id}_${i}`);
                }
            }
        });
        return Math.max(uniqueUsers.size, 10);
    }

    calculateSystemLoad(dashboards) {
        const avgQueries = dashboards.reduce((sum, dash) => sum + (dash.query_count || 0), 0) / dashboards.length;
        return Math.round((avgQueries / 100) * 10) / 10;
    }

    calculateDataFreshness(dashboards) {
        const now = new Date();
        const avgAge = dashboards.reduce((sum, dash) => {
            const ageInDays = (now - dash.lastAccessed) / (1000 * 60 * 60 * 24);
            return sum + ageInDays;
        }, 0) / dashboards.length;
        
        const freshnessScore = Math.max(0, 100 - (avgAge * 3));
        return Math.round(freshnessScore);
    }

    calculateOverallHealth() {
        const weights = {
            performance: 0.25,
            governance: 0.25,
            usage: 0.20,
            dataQuality: 0.20,
            security: 0.10
        };

        this.healthMetrics.security = 75; // Default security score

        const weightedScore = Object.keys(weights).reduce((total, metric) => {
            return total + (this.healthMetrics[metric] * weights[metric]);
        }, 0);

        if (weightedScore >= 90) return 'A';
        if (weightedScore >= 80) return 'B';
        if (weightedScore >= 70) return 'C';
        if (weightedScore >= 60) return 'D';
        return 'F';
    }

    // Main diagnostic method
// Fixed runDiagnostic method - replace your existing one with this

async runDiagnostic() {
    console.log('🩺 Starting Looker Health Diagnostic with Enhanced Query Analysis...');
    
    try {
        // Test Gemini connection if available
        if (process.env.GEMINI_API_KEY) {
            await this.testGeminiConnection();
        }

        const mcpConnected = await this.initializeMCP();
        if (!mcpConnected) {
            throw new Error('MCP initialization failed');
        }

        // 1. Collect basic dashboard and look data
        const data = await this.fetchAllDashboardsWithPagination();
        
        if (data.dashboards.length === 0) {
            throw new Error('No dashboards found. Check your Looker credentials and permissions.');
        }
        
        // 2. Enhanced: Collect slow queries with full context
        const slowQueries = await this.fetchQueryMetricsEnhanced();
        console.log(`Found ${slowQueries.length} slow queries for optimization`);
        
        // 3. Enhanced: Get LookML files for optimization analysis
        const lookmlFiles = await this.fetchLookMLForSlowQueries(slowQueries);
        
        // 4. Enhanced: Analyze LookML optimization opportunities
        const lookmlOptimizations = await this.analyzeLookMLOptimizations(slowQueries, lookmlFiles);
        
        // 5. Run analysis methods (updated to use enhanced query data)
        const queryAnalysis = await this.analyzeSlowQueriesWithAI(data.dashboards, slowQueries);
        const performanceIssues = await this.analyzePerformance(data.dashboards, slowQueries);
        const governanceIssues = this.analyzeGovernance(data.dashboards, data.looks || []);
        const usageIssues = this.analyzeUsage(data.dashboards, data.looks || []);
        const dataQualityIssues = this.analyzeDataQuality(data.dashboards, data.systemMetrics, slowQueries);
        
        // 6. Combine all issues
        const allIssues = [
            ...performanceIssues,
            ...governanceIssues,
            ...usageIssues,
            ...dataQualityIssues
        ];

        // 7. Generate AI recommendations
        const aiRecommendations = await this.generateAIRecommendations(allIssues, queryAnalysis);
        
        // 8. Enhanced results structure
        const results = {
            timestamp: new Date(),
            healthMetrics: this.healthMetrics,
            overallGrade: aiRecommendations.overallHealthGrade,
            totalIssuesFound: allIssues.length,
            issuesByType: {
                performance: performanceIssues.length,
                governance: governanceIssues.length,
                usage: usageIssues.length,
                dataQuality: dataQualityIssues.length
            },
            
            // Enhanced query and LookML data
            slowQueryAnalysis: {
                totalSlowQueries: slowQueries.length,
                byModel: this.groupQueriesByModel(slowQueries),
                topBottlenecks: slowQueries.slice(0, 10),
                lookmlOptimizations: lookmlOptimizations
            },
            
            // Standard diagnostic data
            detailedIssues: allIssues,
            aiRecommendations: aiRecommendations,
            queryAnalysis: queryAnalysis,
            systemInfo: data.systemMetrics,
            enhancedFeatures: {
                paginationEnabled: true,
                aiQueryAnalysis: queryAnalysis.length > 0,
                lookmlOptimizations: lookmlOptimizations.length > 0,
                mcpToolsUsed: ['get_dashboards', 'get_looks', 'query'],
                totalMCPCalls: this.mcpMetrics.callsPerformed
            }
        };

        console.log('✅ Health diagnostic completed successfully');
        console.log(`📊 Analyzed ${data.dashboards.length} dashboards, ${data.looks?.length || 0} looks`);
        console.log(`🤖 AI analyzed ${queryAnalysis.length} slow queries`);
        console.log(`🏗️ Found ${lookmlOptimizations.length} LookML optimization opportunities`);
        console.log(`🔌 MCP made ${this.mcpMetrics.callsPerformed} calls with ${this.mcpMetrics.errors} errors`);
        
        return results;

    } catch (error) {
        console.error('❌ Health diagnostic failed:', error);
        throw error;
    }
}

// Add this missing helper method
groupQueriesByModel(queries) {
    const groups = {};
    
    queries.forEach(query => {
        if (query.model) {
            if (!groups[query.model]) {
                groups[query.model] = [];
            }
            groups[query.model].push(query);
        }
    });
    
    // Transform to summary format
    const summary = {};
    Object.keys(groups).forEach(model => {
        summary[model] = {
            totalQueries: groups[model].length,
            avgRuntime: Math.round(
                groups[model].reduce((sum, q) => sum + q.runtime_seconds, 0) / groups[model].length * 100
            ) / 100,
            explores: [...new Set(groups[model].map(q => q.explore).filter(Boolean))]
        };
    });
    
    return summary;
}

// Add this missing method for LookML response processing
processLookMLResponse(responseBuffer, modelName) {
    const files = [];
    const lines = responseBuffer.split('\n');
    
    for (const line of lines) {
        if (line.trim()) {
            try {
                const response = JSON.parse(line.trim());
                
                if (response.result && response.result.content && Array.isArray(response.result.content)) {
                    for (const item of response.result.content) {
                        if (item.type === 'text' && item.text) {
                            try {
                                const lookmlData = JSON.parse(item.text);
                                
                                // Transform LookML data to expected format
                                const file = {
                                    model: modelName,
                                    name: lookmlData.name || lookmlData.file_name || 'unknown',
                                    type: lookmlData.type || 'view',
                                    explore: lookmlData.explore_name,
                                    content: lookmlData.content || lookmlData.lookml_content || ''
                                };
                                
                                files.push(file);
                            } catch (e) {
                                console.log('Could not parse LookML data');
                            }
                        }
                    }
                    break;
                }
            } catch (e) {
                // Skip non-JSON lines
            }
        }
    }
    
    console.log(`📝 Found ${files.length} LookML files for model ${modelName}`);
    return files;
}

// Add this missing method for performance gain estimation
estimatePerformanceGains(issues) {
    let totalGain = 0;
    let complexity = 'low';
    
    issues.forEach(issue => {
        switch (issue.type) {
            case 'missing_aggregate_table':
                totalGain += 70; // 70% improvement potential
                complexity = 'high';
                break;
            case 'complex_joins':
                totalGain += 50; // 50% improvement potential
                complexity = 'medium';
                break;
            case 'missing_indexes':
                totalGain += 80; // 80% improvement potential
                complexity = 'medium';
                break;
            case 'inefficient_select':
                totalGain += 25; // 25% improvement potential
                complexity = 'low';
                break;
        }
    });
    
    return {
        estimatedSpeedupPercentage: Math.min(totalGain, 90), // Cap at 90%
        implementationComplexity: complexity,
        timeToImplement: complexity === 'high' ? '2-4 weeks' : 
                        complexity === 'medium' ? '1-2 weeks' : '1-3 days'
    };
}
}
module.exports = { LookerHealthDiagnostic };