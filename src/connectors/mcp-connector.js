// src/connectors/mcp-connector.js
// IMPROVED: Gets top 5-10 queries per explore, filters out system/tool explores

class MCPConnector {
    constructor(config) {
        this.config = config;
        this.isConnected = false;
        this.toolboxPath = null;
        
        // System/tool explores to exclude
        this.excludedExplores = new Set([
            'api_explorer',
            'system__activity',
            'i__looker',
            'content_usage',
            'version_set',
            'project_configuration',
            'marketplace_installation',
            'oauth_client',
            'oauth_authorization',
            'ldap_config',
            'saml_config',
            'oidc_config',
            'permission_set',
            'model_set',
            'git_branch',
            'git_status',
            'project_file',
            'lookml_model',
            'lookml_model_explore',
            'lookml_dashboard',
            'datagroup',
            'connection',
            'dialect_info',
            'database',
            'schema',
            'table',
            'column'
        ]);
        
        // System/tool models to exclude
        this.excludedModels = new Set([
            'system__activity',
            'i__looker',
            'api_explorer',
            'marketplace',
            'tools',
            'admin',
            'system',
            'internal'
        ]);
    }

    async initialize() {
        try {
            console.log('Initializing MCP connector...');
            
            if (!this.config.lookerUrl || !this.config.clientId || !this.config.clientSecret) {
                throw new Error('Missing required Looker configuration');
            }
            
            const path = require('path');
            this.toolboxPath = path.join(__dirname, '..', '..', 'scripts', 'toolbox');
            
            this.isConnected = true;
            console.log('MCP connector initialized successfully');
            
            return true;
        } catch (error) {
            console.error('MCP connector initialization failed:', error);
            this.isConnected = false;
            return false;
        }
    }

    /**
     * Check if an explore should be excluded
     */
    isSystemExplore(exploreName, modelName) {
        if (!exploreName) return false;
        
        const exploreLower = exploreName.toLowerCase();
        const modelLower = (modelName || '').toLowerCase();
        
        // Check explicit exclusion lists
        if (this.excludedExplores.has(exploreLower)) return true;
        if (this.excludedModels.has(modelLower)) return true;
        
        // Check patterns that indicate system/tool explores
        const systemPatterns = [
            'api_explorer',
            'system__',
            'i__looker',
            '__internal',
            'marketplace_',
            'admin_',
            'tools_',
            'git_',
            'ldap_',
            'saml_',
            'oauth_',
            'permission_',
            'model_set',
            'content_',
            'project_',
            'lookml_',
            'datagroup',
            'connection_',
            'database_',
            'schema_',
            'table_',
            'column_',
            'block',
            'extension'
        ];
        
        return systemPatterns.some(pattern => 
            exploreLower.includes(pattern) || modelLower.includes(pattern)
        );
    }

    /**
     * Get slow queries grouped by explore with limits per explore
     */
    async getSlowQueriesGroupedByExplore(
        timeRange = '30 days ago for 30 days',
        runtimeThreshold = 5,
        queriesPerExplore = 5,
        maxExplores = 1
    ) {
        console.log(`\n📊 Fetching top ${queriesPerExplore} slowest queries per explore...`);
        console.log(`   Time range: ${timeRange}`);
        console.log(`   Runtime threshold: >${runtimeThreshold}s`);
        console.log(`   Max explores to analyze: ${maxExplores}`);
        
        const allQueries = [];
        const exploreQueryCounts = new Map();
        
        try {
            // First, get all slow queries to understand the landscape
            console.log('\n🔍 Step 1: Getting initial slow queries overview...');
            const initialQueries = await this.executeQuery({
                model: "system__activity",
                explore: "history",
                fields: [
                    "query.id",
                    "query.slug",
                    "query.model",
                    "query.explore",
                    "history.runtime",
                    "history.created_date",
                    "dashboard.title",
                    "user.email"
                ],
                filters: {
                    "history.runtime": `>${runtimeThreshold}`,
                    "history.created_date": timeRange,
                    "history.status": "complete"
                },
                sorts: ["history.runtime desc"],
                limit: 500 // Get more initially to understand distribution
            });
            
            console.log(`   Found ${initialQueries.length} total slow queries`);
            
            // Group queries by model.explore and filter out system explores
            const queryGroups = new Map();
            
            for (const query of initialQueries) {
                const model = query.model || query['query.model'];
                const explore = query.explore || query['query.explore'];
                
                if (!model || !explore) continue;
                
                // Skip system/tool explores
                if (this.isSystemExplore(explore, model)) {
                    console.log(`   ⚠️ Skipping system explore: ${model}.${explore}`);
                    continue;
                }
                
                const key = `${model}.${explore}`;
                
                if (!queryGroups.has(key)) {
                    queryGroups.set(key, []);
                }
                
                queryGroups.get(key).push(query);
            }
            
            console.log(`\n📈 Found slow queries in ${queryGroups.size} unique explores (after filtering)`);
            
            // Sort explores by total runtime to prioritize worst performers
            const sortedExplores = Array.from(queryGroups.entries())
                .map(([key, queries]) => {
                    const totalRuntime = queries.reduce((sum, q) => {
                        const runtime = parseFloat(q.runtime_seconds || q['history.runtime'] || 0);
                        return sum + runtime;
                    }, 0);
                    
                    return {
                        key,
                        queries,
                        totalRuntime,
                        avgRuntime: totalRuntime / queries.length,
                        queryCount: queries.length
                    };
                })
                .sort((a, b) => b.totalRuntime - a.totalRuntime)
                .slice(0, maxExplores); // Limit to top N explores
            
            console.log('\n🎯 Top explores by total runtime:');
            sortedExplores.slice(0, 5).forEach((explore, index) => {
                console.log(`   ${index + 1}. ${explore.key}: ${explore.queryCount} queries, avg ${explore.avgRuntime.toFixed(1)}s`);
            });
            
            // Get top queries from each explore
            console.log(`\n📝 Collecting top ${queriesPerExplore} queries from each explore...`);
            
            for (const exploreData of sortedExplores) {
                const topQueries = exploreData.queries
                    .sort((a, b) => {
                        const runtimeA = parseFloat(a.runtime_seconds || a['history.runtime'] || 0);
                        const runtimeB = parseFloat(b.runtime_seconds || b['history.runtime'] || 0);
                        return runtimeB - runtimeA;
                    })
                    .slice(0, queriesPerExplore);
                
                // Add explore metadata to each query
                topQueries.forEach(query => {
                    query.exploreKey = exploreData.key;
                    query.exploreQueryCount = exploreData.queryCount;
                    query.exploreAvgRuntime = exploreData.avgRuntime;
                });
                
                allQueries.push(...topQueries);
                exploreQueryCounts.set(exploreData.key, topQueries.length);
                
                console.log(`   ✅ Added ${topQueries.length} queries from ${exploreData.key}`);
            }
            
            // If we don't have enough queries, try with lower threshold
            if (allQueries.length < 10 && runtimeThreshold > 1) {
                console.log(`\n⚠️ Only found ${allQueries.length} queries, trying with lower threshold...`);
                
                const additionalQueries = await this.executeQuery({
                    model: "system__activity",
                    explore: "history",
                    fields: [
                        "query.id",
                        "query.slug", 
                        "query.model",
                        "query.explore",
                        "history.runtime",
                        "history.created_date",
                        "dashboard.title",
                        "user.email"
                    ],
                    filters: {
                        "history.runtime": `>1`,
                        "history.created_date": timeRange,
                        "history.status": "complete"
                    },
                    sorts: ["history.runtime desc"],
                    limit: 20
                });
                
                // Filter out system explores and duplicates
                const filteredAdditional = additionalQueries.filter(q => {
                    const model = q.model || q['query.model'];
                    const explore = q.explore || q['query.explore'];
                    
                    if (this.isSystemExplore(explore, model)) return false;
                    
                    const queryId = q.query_id || q['query.id'];
                    return !allQueries.some(existing => 
                        (existing.query_id || existing['query.id']) === queryId
                    );
                });
                
                allQueries.push(...filteredAdditional);
                console.log(`   Added ${filteredAdditional.length} additional queries with >1s runtime`);
            }
            
            console.log(`\n✅ Final result: ${allQueries.length} queries from ${exploreQueryCounts.size} explores`);
            
            // Add summary statistics
            const summary = {
                totalQueries: allQueries.length,
                uniqueExplores: exploreQueryCounts.size,
                averageRuntime: allQueries.reduce((sum, q) => {
                    return sum + parseFloat(q.runtime_seconds || q['history.runtime'] || 0);
                }, 0) / allQueries.length,
                exploreBreakdown: Array.from(exploreQueryCounts.entries()).map(([explore, count]) => ({
                    explore,
                    queryCount: count
                }))
            };
            
            console.log(`\n📊 Summary:`);
            console.log(`   Total queries: ${summary.totalQueries}`);
            console.log(`   Unique explores: ${summary.uniqueExplores}`);
            console.log(`   Average runtime: ${summary.averageRuntime.toFixed(1)}s`);
            
            return {
                queries: allQueries,
                summary: summary
            };
            
        } catch (error) {
            console.error('Failed to get grouped slow queries:', error.message);
            return {
                queries: [],
                summary: {
                    totalQueries: 0,
                    uniqueExplores: 0,
                    averageRuntime: 0,
                    exploreBreakdown: []
                }
            };
        }
    }

    /**
     * Get slow queries with automatic threshold adjustment
     */
    async getSlowQueriesAdaptive(
        timeRange = '30 days ago for 30 days',
        targetCount = 50,
        maxQueriesPerExplore = 5
    ) {
        console.log(`\n🎯 Adaptive slow query fetching (target: ${targetCount} queries)`);
        
        const thresholds = [10, 5, 3, 2, 1]; // Try progressively lower thresholds
        
        for (const threshold of thresholds) {
            console.log(`\n🔄 Trying with >${threshold}s threshold...`);
            
            const result = await this.getSlowQueriesGroupedByExplore(
                timeRange,
                threshold,
                maxQueriesPerExplore,
                Math.ceil(targetCount / maxQueriesPerExplore)
            );
            
            if (result.queries.length >= targetCount * 0.5) { // Accept if we get at least 50% of target
                console.log(`✅ Found sufficient queries with ${threshold}s threshold`);
                return result.queries;
            }
            
            console.log(`⚠️ Only found ${result.queries.length} queries, trying lower threshold...`);
        }
        
        // If all thresholds fail, return what we found with the lowest threshold
        console.log('⚠️ Could not find target number of queries, returning best effort');
        const finalResult = await this.getSlowQueriesGroupedByExplore(
            timeRange,
            0.5, // Very low threshold as last resort
            maxQueriesPerExplore,
            Math.ceil(targetCount / maxQueriesPerExplore)
        );
        
        return finalResult.queries;
    }

    async executeTool(toolName, toolArguments = {}, timeout = 20000) {
        if (!this.isConnected) {
            throw new Error('MCP connector not initialized');
        }

        console.log(`   Executing MCP tool: ${toolName}`);
        
        return new Promise((resolve, reject) => {
            const { spawn } = require('child_process');
            
            const toolbox = spawn(this.toolboxPath, [
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
                
                try {
                    const result = this.parseResponse(responseBuffer, toolName);
                    resolve(result);
                } catch (error) {
                    reject(new Error(`Failed to parse ${toolName} response: ${error.message}`));
                }
            });

            toolbox.on('error', (error) => {
                if (!processCompleted) {
                    processCompleted = true;
                    reject(new Error(`MCP process error for ${toolName}: ${error.message}`));
                }
            });

            const command = {
                jsonrpc: "2.0",
                id: 1,
                method: "tools/call",
                params: {
                    name: toolName,
                    arguments: toolArguments
                }
            };
            
            toolbox.stdin.write(JSON.stringify(command) + '\n');
            toolbox.stdin.end();

            setTimeout(() => {
                if (!processCompleted) {
                    processCompleted = true;
                    toolbox.kill('SIGKILL');
                    reject(new Error(`MCP tool ${toolName} timed out after ${timeout}ms`));
                }
            }, timeout);
        });
    }

    async getAllModels() {
        try {
            const response = await this.executeTool('get_models', {});
            const models = this.parseModelsResponse(response);
            
            // Filter out system models
            const userModels = models.filter(model => 
                !this.excludedModels.has(model.name.toLowerCase())
            );
            
            console.log(`   Found ${userModels.length} user models (filtered from ${models.length} total)`);
            return userModels;
            
        } catch (error) {
            console.error('Failed to get models:', error.message);
            return [];
        }
    }

    async getExploresForModel(modelName) {
        try {
            const response = await this.executeTool('get_explores', { model: modelName });
            const explores = this.parseExploresResponse(response, modelName);
            
            // Filter out system explores
            const userExplores = explores.filter(explore => 
                !this.isSystemExplore(explore.name, modelName)
            );
            
            return userExplores;
            
        } catch (error) {
            console.error(`Failed to get explores for model ${modelName}:`, error.message);
            return [];
        }
    }

    async executeQuery(queryParams) {
        try {
            const response = await this.executeTool('query', queryParams, 30000);
            return this.parseQueryResponse(response);
        } catch (error) {
            console.error('Failed to execute query:', error.message);
            return [];
        }
    }

    parseResponse(responseBuffer, toolName) {
        if (!responseBuffer || responseBuffer.length < 10) {
            return [];
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
                    throw new Error(response.error.message || `MCP error in ${toolName}`);
                }
            } catch (lineError) {
                continue;
            }
        }
        
        return results;
    }

    parseModelsResponse(responseData) {
        const models = [];
        const dataArray = Array.isArray(responseData) ? responseData : [responseData];
        
        for (const item of dataArray) {
            if (item.raw_text) continue;
            
            const modelArray = Array.isArray(item) ? item : [item];
            
            for (const model of modelArray) {
                if (model && model.name) {
                    models.push({
                        name: model.name,
                        project: model.project_name,
                        explores: model.explores || [],
                        rawData: model
                    });
                }
            }
        }
        
        return models;
    }

    parseExploresResponse(responseData, modelName) {
        const explores = [];
        const dataArray = Array.isArray(responseData) ? responseData : [responseData];
        
        for (const item of dataArray) {
            if (item.raw_text) continue;
            
            const exploreArray = Array.isArray(item) ? item : [item];
            
            for (const explore of exploreArray) {
                if (explore && explore.name) {
                    explores.push({
                        model: modelName,
                        name: explore.name,
                        label: explore.label || explore.name,
                        description: explore.description || '',
                        group_label: explore.group_label,
                        hidden: explore.hidden || false,
                        rawData: explore
                    });
                }
            }
        }
        
        return explores;
    }

    parseQueryResponse(responseData) {
        const queries = [];
        const dataArray = Array.isArray(responseData) ? responseData : [responseData];
        
        for (const item of dataArray) {
            if (item.raw_text) continue;
            
            const queryArray = Array.isArray(item) ? item : [item];
            
            for (const row of queryArray) {
                if (row && (row.query_id || row['query.id'])) {
                    const query = {
                        query_id: row.query_id || row['query.id'],
                        slug: row.slug || row['query.slug'],
                        runtime_seconds: parseFloat(
                            row.runtime_seconds || 
                            row['history.runtime'] || 
                            row.runtime || 0
                        ),
                        created_date: row.created_date || row['history.created_date'],
                        model: row.model || row['query.model'],
                        explore: row.explore || row['query.explore'],
                        dashboard_title: row.dashboard_title || row['dashboard.title'],
                        user_email: row.user_email || row['user.email']
                    };
                    
                    queries.push(query);
                }
            }
        }
        
        return queries;
    }

    async testConnection() {
        try {
            const models = await this.getAllModels();
            return {
                success: true,
                message: 'MCP connection successful',
                userModelsFound: models.length
            };
        } catch (error) {
            return {
                success: false,
                message: `MCP connection failed: ${error.message}`
            };
        }
    }

    getStatus() {
        return {
            connected: this.isConnected,
            toolboxPath: this.toolboxPath,
            config: {
                hasUrl: !!this.config.lookerUrl,
                hasClientId: !!this.config.clientId,
                hasSecret: !!this.config.clientSecret
            },
            excludedExplores: this.excludedExplores.size,
            excludedModels: this.excludedModels.size
        };
    }
}

module.exports = { MCPConnector };