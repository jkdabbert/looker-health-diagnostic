// src/connectors/mcp-connector.js
// FIXED: Parameter passing issue in MCP calls

class MCPConnector {
    constructor(config) {
        this.config = config;
        this.isConnected = false;
        this.toolboxPath = null;
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

    // FIXED: Corrected parameter passing to MCP
    async executeTool(toolName, toolArguments = {}, timeout = 20000) {
        if (!this.isConnected) {
            throw new Error('MCP connector not initialized');
        }

        console.log(`Executing MCP tool: ${toolName}`);
        
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
                const errorMsg = data.toString().trim();
                errorBuffer += errorMsg + '\n';
                console.log(`MCP ${toolName} STDERR: ${errorMsg}`);
            });

            toolbox.on('close', (code) => {
                if (processCompleted) return;
                processCompleted = true;
                
                if (errorBuffer.includes('invalid tool name')) {
                    reject(new Error(`Invalid tool name: ${toolName}`));
                    return;
                }
                
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

            // FIXED: Send parameters directly, not wrapped in array
            const command = {
                jsonrpc: "2.0",
                id: 1,
                method: "tools/call",
                params: {
                    name: toolName,
                    arguments: toolArguments // Direct parameter object
                }
            };

            console.log(`Sending MCP command: ${JSON.stringify(command, null, 2)}`);
            
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
            return this.parseModelsResponse(response);
        } catch (error) {
            console.error('Failed to get models:', error.message);
            return [];
        }
    }

    async getExploresForModel(modelName) {
        try {
            // FIXED: Pass model parameter correctly
            const response = await this.executeTool('get_explores', { model: modelName });
            return this.parseExploresResponse(response, modelName);
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

    async getSlowQueries(timeRange = '7 days ago for 7 days', runtimeThreshold = 5, limit = 25) {
        const queryParams = {
            model: "system__activity",
            explore: "history",
            fields: [
                "query.id",
                "query.slug",
                "history.runtime",
                "history.created_date", 
                "query.model",
                "query.explore",
                "dashboard.title",
                "user.email"
            ],
            filters: {
                "history.runtime": `>${runtimeThreshold}`,
                "history.created_date": timeRange,
                "history.status": "complete"
            },
            sorts: ["history.runtime desc"],
            limit: limit
        };

        return await this.executeQuery(queryParams);
    }

    async listAvailableTools() {
        try {
            const response = await this.executeTool('list_tools', {});
            return response;
        } catch (error) {
            console.error('Failed to list tools:', error.message);
            return [];
        }
    }

    parseResponse(responseBuffer, toolName) {
        if (!responseBuffer || responseBuffer.length < 10) {
            console.log(`Empty response buffer for ${toolName}`);
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
        
        console.log(`Parsed ${results.length} items from ${toolName} response`);
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
        
        console.log(`Parsed ${models.length} models`);
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
        
        console.log(`Parsed ${explores.length} explores for model ${modelName}`);
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
        
        console.log(`Parsed ${queries.length} queries`);
        return queries;
    }

    async testConnection() {
        try {
            const tools = await this.listAvailableTools();
            return {
                success: true,
                message: 'MCP connection successful',
                availableTools: tools.length || 0
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
            }
        };
    }
}

module.exports = { MCPConnector };