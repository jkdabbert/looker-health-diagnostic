// Streamlined Query Performance Diagnostic - Focus on slow queries and LookML optimization

class QueryPerformanceDiagnostic {
    constructor(config) {
        this.config = config;
        this.mcpClient = null;
        this.slowQueries = [];
        this.lookmlFiles = [];
        this.optimizations = [];
    }

    async initializeMCP() {
        try {
            console.log('🔌 Initializing MCP connection for query analysis...');
            
            if (!this.config.lookerUrl || !this.config.clientId || !this.config.clientSecret) {
                throw new Error('Missing required Looker configuration');
            }
            
            this.mcpClient = {
                baseUrl: this.config.lookerUrl,
                clientId: this.config.clientId,
                isConnected: true,
                startTime: Date.now()
            };
            
            console.log('✅ MCP client configured for query performance analysis');
            return true;
        } catch (error) {
            console.error('❌ MCP connection failed:', error);
            return false;
        }
    }

    // Debug MCP parameters to understand the correct structure
    async debugMCPParameters() {
        console.log('🔧 Starting MCP parameter debugging...');
        
        try {
            // Test 1: List available tools
            console.log('\n=== Testing Available Tools ===');
            const toolsResponse = await this.testMCPCall({
                method: "tools/list",
                params: {}
            });
            console.log('Tools response:', toolsResponse);

            // Test 2: Get models 
            console.log('\n=== Testing Get Models ===');
            const modelsResponse = await this.testMCPCall({
                method: "tools/call",
                params: {
                    name: "get_models",
                    arguments: {}
                }
            });
            console.log('Models response:', modelsResponse);

            // Test 3: Try query with "fields" parameter
            console.log('\n=== Testing Query with Fields ===');
            const queryResponse = await this.testMCPCall({
                method: "tools/call", 
                params: {
                    name: "query",
                    arguments: {
                        model: "system__activity",
                        explore: "history",
                        fields: ["query.id", "history.runtime"],
                        limit: 5
                    }
                }
            });
            console.log('Query response:', queryResponse);

            // Test 4: Try alternative query structures
            console.log('\n=== Testing Alternative Query Structures ===');
            
            const alternatives = [
                {
                    name: "With view parameter",
                    args: {
                        model: "system__activity",
                        view: "history",
                        fields: ["query.id", "history.runtime"],
                        limit: 5
                    }
                },
                {
                    name: "String fields",
                    args: {
                        model: "system__activity",
                        explore: "history",
                        fields: "query.id,history.runtime",
                        limit: 5
                    }
                },
                {
                    name: "Dimensions/measures structure",
                    args: {
                        model: "system__activity",
                        explore: "history",
                        dimensions: ["query.id", "history.runtime"],
                        measures: [],
                        limit: 5
                    }
                }
            ];

            for (const alt of alternatives) {
                console.log(`\n--- Testing: ${alt.name} ---`);
                const response = await this.testMCPCall({
                    method: "tools/call",
                    params: {
                        name: "query",
                        arguments: alt.args
                    }
                });
                console.log(`${alt.name} response:`, response.stdout.substring(0, 200));
            }

        } catch (error) {
            console.error('MCP debugging failed:', error);
        }
    }

    // Helper method for testing MCP calls
    async testMCPCall(commandParams) {
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
                
                console.log('STDOUT:', responseBuffer.substring(0, 500));
                if (errorBuffer) console.log('STDERR:', errorBuffer);
                
                resolve({ stdout: responseBuffer, stderr: errorBuffer });
            });

            const command = {
                jsonrpc: "2.0",
                id: Math.floor(Math.random() * 1000),
                ...commandParams
            };

            console.log('Sending command:', JSON.stringify(command, null, 2));
            toolbox.stdin.write(JSON.stringify(command) + '\n');
            toolbox.stdin.end();

            setTimeout(() => {
                if (!processCompleted) {
                    toolbox.kill('SIGKILL');
                    resolve({ stdout: responseBuffer, stderr: errorBuffer, timeout: true });
                }
            }, 15000);
        });
    }

    // Test query_metrics explore access
    async testQueryMetricsExplore() {
        console.log('🧪 Testing direct access to query_metrics explore...');
        
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

            toolbox.stderr.on('data', (data) => {
                console.log(`Test Query STDERR: ${data.toString().trim()}`);
            });

            toolbox.on('close', (code) => {
                if (processCompleted) return;
                processCompleted = true;
                
                console.log('Query metrics test response:', responseBuffer);
                resolve(responseBuffer);
            });

            // Test with minimal fields first
            const command = {
                jsonrpc: "2.0",
                id: 99,
                method: "tools/call",
                params: {
                    name: "query",
                    arguments: {
                        model: "system__activity",
                        explore: "query_metrics",
                        fields: ["query.id", "query.model", "query.explore"],
                        limit: 5
                    }
                }
            };

            console.log('Testing query_metrics with command:', JSON.stringify(command, null, 2));
            toolbox.stdin.write(JSON.stringify(command) + '\n');
            toolbox.stdin.end();

            setTimeout(() => {
                if (!processCompleted) {
                    toolbox.kill('SIGKILL');
                    resolve('');
                }
            }, 15000);
        });
    }

    // Get available models to understand what's accessible
    async getAvailableModels() {
        console.log('📋 Getting available models...');
        
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

            toolbox.stderr.on('data', (data) => {
                console.log(`MCP Models STDERR: ${data.toString().trim()}`);
            });

            toolbox.on('close', (code) => {
                if (processCompleted) return;
                processCompleted = true;
                
                try {
                    console.log('Available models response:', responseBuffer);
                    const models = this.parseModelsResponse(responseBuffer);
                    resolve(models);
                } catch (error) {
                    resolve([]);
                }
            });

            const command = {
                jsonrpc: "2.0",
                id: 1,
                method: "tools/call",
                params: {
                    name: "get_models",
                    arguments: {}
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

    parseModelsResponse(responseBuffer) {
        const models = [];
        const lines = responseBuffer.split('\n').filter(line => line.trim());
        
        for (const line of lines) {
            try {
                const response = JSON.parse(line);
                if (response.result && response.result.content) {
                    for (const item of response.result.content) {
                        if (item.type === 'text' && item.text) {
                            try {
                                const modelData = JSON.parse(item.text);
                                if (Array.isArray(modelData)) {
                                    models.push(...modelData);
                                } else {
                                    models.push(modelData);
                                }
                            } catch (e) {
                                // If not JSON, treat as text
                                models.push({ name: item.text.trim() });
                            }
                        }
                    }
                }
            } catch (e) {
                continue;
            }
        }
        
        console.log(`📋 Found ${models.length} models:`, models.map(m => m.name || m));
        return models;
    }

    // Focused query collection using the correct parameter structure
    async fetchSlowQueriesWithContext() {
        console.log('🐌 Fetching slow queries with explore context...');
        
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

            toolbox.stderr.on('data', (data) => {
                console.log(`MCP Query STDERR: ${data.toString().trim()}`);
            });

            toolbox.on('close', (code) => {
                if (processCompleted) return;
                processCompleted = true;
                
                try {
                    const queries = this.processQueryPerformanceResponse(responseBuffer);
                    resolve(queries);
                } catch (error) {
                    console.error('Error processing query response:', error);
                    resolve([]);
                }
            });

            // Use the correct field names based on what we know works
            const command = {
                jsonrpc: "2.0",
                id: 1,
                method: "tools/call",
                params: {
                    name: "query",
                    arguments: {
                        model: "system__activity",
                        explore: "history", // Go back to history explore since it was working
                        fields: [
                            "query.id",
                            "history.runtime",
                            "history.created_date", 
                            "query.model",
                            "dashboard.title",
                            "user.email"
                        ],
                        filters: {
                            "history.runtime": ">5",
                            "history.created_date": "7 days ago for 7 days",
                            "history.status": "complete"
                        },
                        sorts: ["history.runtime desc"],
                        limit: 50
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
            }, 20000);
        });
    }

    // Process query performance response
    processQueryPerformanceResponse(responseBuffer) {
        console.log('🔍 Processing query performance response...');
        const queries = [];
        
        if (!responseBuffer || responseBuffer.length < 10) {
            console.log('⚠️ Empty or minimal response buffer');
            return queries;
        }

        const lines = responseBuffer.split('\n').filter(line => line.trim());
        
        for (const line of lines) {
            try {
                const response = JSON.parse(line);
                
                if (response.error) {
                    console.log('❌ MCP Query Error:', response.error);
                    continue;
                }
                
                if (response.result && response.result.content) {
                    for (const item of response.result.content) {
                        if (item.type === 'text' && item.text) {
                            try {
                                // Handle both single objects and arrays
                                const data = JSON.parse(item.text);
                                const dataArray = Array.isArray(data) ? data : [data];
                                
                                // Debug: log the first few rows to see the actual structure
                                if (dataArray.length > 0 && queries.length < 5) {
                                    console.log('🔍 Sample query data structure:', JSON.stringify(dataArray[0], null, 2));
                                }
                                
                                for (const row of dataArray) {
                                    if (row.query_id || row['query.id']) {
                                        const query = {
                                            query_id: row.query_id || row['query.id'],
                                            runtime_seconds: parseFloat(
                                                row.runtime_seconds || 
                                                row['history.runtime_in_seconds'] || 
                                                row['history.runtime'] || 
                                                row.runtime || 0
                                            ),
                                            created_date: row.created_date || row['history.created_date'],
                                            model: row.model || row['query.model'],
                                            explore: row.explore || row['query.explore'],
                                            sql: row.sql || row['query.sql'] || row['query.formatted_fields'] || '',
                                            dashboard_title: row.dashboard_title || row['dashboard.title'],
                                            look_id: row.look_id || row['look.id'],
                                            user_email: row.user_email || row['user.email'],
                                            raw_data: row // Include raw data for debugging
                                        };
                                        
                                        // Debug log for model/explore detection
                                        if (!query.model || !query.explore) {
                                            if (queries.length < 3) { // Only log first few to avoid spam
                                                console.log(`⚠️ Missing model/explore for query ${query.query_id}:`, {
                                                    model: query.model,
                                                    explore: query.explore,
                                                    available_fields: Object.keys(row),
                                                    sample_values: {
                                                        'query.model': row['query.model'],
                                                        'query.explore': row['query.explore'],
                                                        'model': row.model,
                                                        'explore': row.explore
                                                    }
                                                });
                                            }
                                        } else {
                                            if (queries.length < 3) {
                                                console.log(`✅ Found model.explore: ${query.model}.${query.explore} for query ${query.query_id}`);
                                            }
                                        }
                                        
                                        if (query.runtime_seconds > 5) {
                                            queries.push(query);
                                        }
                                    }
                                }
                            } catch (parseError) {
                                console.log('Could not parse query data item:', item.text.substring(0, 100) + '...');
                            }
                        }
                    }
                }
            } catch (lineError) {
                // Skip malformed JSON lines
                continue;
            }
        }
        
        console.log(`🐌 Found ${queries.length} slow queries for analysis`);
        
        // Debug: Show how many have model/explore data
        const withModelExplore = queries.filter(q => q.model && q.explore);
        console.log(`📊 ${withModelExplore.length} queries have model/explore data`);
        
        if (withModelExplore.length === 0 && queries.length > 0) {
            console.log('🔍 Available fields in first query:', Object.keys(queries[0].raw_data));
        }
        
        return queries;
    }

    // Get LookML files for the models we found in slow queries (even without explore info)
    async fetchLookMLForSlowQueries(slowQueries) {
        console.log('📝 Fetching LookML files for slow query optimization...');
        
        // Get unique models from slow queries
        const uniqueModels = [...new Set(
            slowQueries
                .filter(q => q.model)
                .map(q => q.model)
        )];
        
        console.log(`📂 Found ${uniqueModels.length} unique models with slow queries:`, uniqueModels);
        
        const lookmlFiles = [];
        
        for (const model of uniqueModels) {
            try {
                console.log(`🔍 Fetching LookML for model: ${model}`);
                const files = await this.fetchLookMLForModel(model);
                lookmlFiles.push(...files);
            } catch (error) {
                console.log(`⚠️ Could not fetch LookML for model ${model}:`, error.message);
            }
        }
        
        return lookmlFiles;
    }

    // Use Looker REST API directly to get LookML files
    async fetchLookMLForModel(model) {
        console.log(`🔍 Fetching LookML via Looker API for model: ${model}`);
        
        try {
            // First get an access token
            const accessToken = await this.getLookerAccessToken();
            if (!accessToken) {
                console.log('❌ Could not get Looker access token');
                return [];
            }
            
            // Get the model files
            const modelFiles = await this.getLookMLModelFiles(model, accessToken);
            return modelFiles;
            
        } catch (error) {
            console.log(`❌ Error fetching LookML for ${model}:`, error.message);
            return [];
        }
    }

    async getLookerAccessToken() {
        try {
            const axios = require('axios');
            
            const response = await axios.post(`${this.config.lookerUrl}/api/4.0/login`, {
                client_id: this.config.clientId,
                client_secret: this.config.clientSecret
            });
            
            console.log('✅ Got Looker access token');
            return response.data.access_token;
            
        } catch (error) {
            console.log('❌ Failed to get Looker access token:', error.message);
            return null;
        }
    }

    async getLookMLModelFiles(model, accessToken) {
        try {
            const axios = require('axios');
            
            // Get model metadata first
            const modelResponse = await axios.get(`${this.config.lookerUrl}/api/4.0/lookml_models/${model}`, {
                headers: { 
                    'Authorization': `Bearer ${accessToken}`,
                    'Content-Type': 'application/json'
                }
            });
            
            console.log(`📋 Model ${model} metadata:`, modelResponse.data);
            
            // Get project files for this model
            const projectName = modelResponse.data.project_name;
            if (!projectName) {
                console.log(`⚠️ No project found for model ${model}`);
                return [];
            }
            
            const filesResponse = await axios.get(`${this.config.lookerUrl}/api/4.0/projects/${projectName}/files`, {
                headers: { 
                    'Authorization': `Bearer ${accessToken}`,
                    'Content-Type': 'application/json'
                }
            });
            
            console.log(`📁 Found ${filesResponse.data.length} files in project ${projectName}`);
            
            const lookmlFiles = [];
            
            // Get content for .model and .view files
            for (const file of filesResponse.data) {
                if (file.name.endsWith('.model') || file.name.endsWith('.view') || file.name.endsWith('.explore')) {
                    try {
                        const fileResponse = await axios.get(`${this.config.lookerUrl}/api/4.0/projects/${projectName}/files/file`, {
                            params: { file_path: file.name },
                            headers: { 
                                'Authorization': `Bearer ${accessToken}`,
                                'Content-Type': 'application/json'
                            }
                        });
                        
                        const lookmlFile = {
                            model: model,
                            name: file.name,
                            content: fileResponse.data,
                            type: this.determineLookMLType(fileResponse.data),
                            joins: this.extractJoins(fileResponse.data),
                            dimensions: this.extractDimensions(fileResponse.data),
                            measures: this.extractMeasures(fileResponse.data),
                            explores: this.extractExplores(fileResponse.data)
                        };
                        
                        lookmlFiles.push(lookmlFile);
                        console.log(`📄 Added LookML file: ${file.name} (${lookmlFile.type})`);
                        
                    } catch (fileError) {
                        console.log(`⚠️ Could not fetch file ${file.name}:`, fileError.message);
                    }
                }
            }
            
            return lookmlFiles;
            
        } catch (error) {
            console.log(`❌ Error getting model files for ${model}:`, error.message);
            return [];
        }
    }

    processLookMLResponse(responseBuffer, model) {
        const files = [];
        
        if (!responseBuffer || responseBuffer.length < 10) {
            return files;
        }

        const lines = responseBuffer.split('\n').filter(line => line.trim());
        
        for (const line of lines) {
            try {
                const response = JSON.parse(line);
                
                if (response.result && response.result.content) {
                    for (const item of response.result.content) {
                        if (item.type === 'text' && item.text) {
                            try {
                                const lookmlData = JSON.parse(item.text);
                                
                                const file = {
                                    model: model,
                                    name: lookmlData.name || `${model}_content`,
                                    content: lookmlData.content || lookmlData.lookml_content || item.text,
                                    type: this.determineLookMLType(lookmlData.content || item.text),
                                    joins: this.extractJoins(lookmlData.content || item.text),
                                    dimensions: this.extractDimensions(lookmlData.content || item.text),
                                    measures: this.extractMeasures(lookmlData.content || item.text),
                                    explores: this.extractExplores(lookmlData.content || item.text)
                                };
                                
                                files.push(file);
                            } catch (parseError) {
                                // If it's not JSON, treat as raw LookML
                                files.push({
                                    model: model,
                                    name: `${model}_raw`,
                                    content: item.text,
                                    type: 'raw_lookml',
                                    joins: this.extractJoins(item.text),
                                    dimensions: this.extractDimensions(item.text),
                                    measures: this.extractMeasures(item.text),
                                    explores: this.extractExplores(item.text)
                                });
                            }
                        }
                    }
                }
            } catch (lineError) {
                continue;
            }
        }
        
        console.log(`📝 Extracted ${files.length} LookML files for model ${model}`);
        return files;
    }

    // Analyze slow queries with Gemini AI - updated to work without explore data
    async analyzeSlowQueriesWithGemini(slowQueries, lookmlFiles) {
        console.log('🤖 Analyzing slow queries with Gemini AI...');
        
        if (!process.env.GEMINI_API_KEY) {
            console.log('⚠️ No Gemini API key - using local analysis');
            return this.analyzeSlowQueriesLocally(slowQueries, lookmlFiles);
        }

        const analyses = [];
        
        // Group queries by model since we don't have explore data
        const queryGroups = this.groupQueriesByModel(slowQueries);
        
        for (const [model, queries] of Object.entries(queryGroups)) {
            const relatedLookML = lookmlFiles.filter(f => f.model === model);
            
            try {
                const analysis = await this.analyzeModelWithGemini(
                    model, 
                    queries, 
                    relatedLookML
                );
                analyses.push(analysis);
            } catch (error) {
                console.log(`⚠️ Gemini analysis failed for model ${model}:`, error.message);
                // Fallback to local analysis
                const localAnalysis = this.analyzeModelLocally(model, queries, relatedLookML);
                analyses.push(localAnalysis);
            }
        }
        
        return analyses;
    }

    async analyzeModelWithGemini(model, queries, lookmlFiles) {
        const axios = require('axios');
        
        const avgRuntime = queries.reduce((sum, q) => sum + q.runtime_seconds, 0) / queries.length;
        const totalQueries = queries.length;
        const lookmlContent = lookmlFiles.map(f => f.content).join('\n---\n');
        const availableExplores = [...new Set(lookmlFiles.flatMap(f => f.explores))];
        
        const prompt = `You are a Looker performance optimization expert. Analyze these slow queries and their LookML to suggest specific optimizations:

MODEL: ${model}
PERFORMANCE STATS:
- ${totalQueries} slow queries (avg: ${avgRuntime.toFixed(2)}s)
- Queries taking 5+ seconds in the past 7 days

AVAILABLE EXPLORES IN MODEL: ${availableExplores.join(', ') || 'Unknown'}

SAMPLE SLOW QUERIES:
${queries.slice(0, 5).map(q => `Query ${q.query_id}: ${q.runtime_seconds}s\nDashboard: ${q.dashboard_title}\nUser: ${q.user_email}`).join('\n\n')}

LOOKML CONTEXT (${lookmlFiles.length} files):
${lookmlContent.substring(0, 3000)}...

Provide optimization recommendations in this JSON format:
{
  "model": "${model}",
  "performanceIssues": [
    {
      "issue": "Specific performance problem identified",
      "severity": "critical|high|medium|low",
      "affectedQueries": 5,
      "cause": "Root cause explanation",
      "likelyExplore": "Best guess at which explore is causing issues"
    }
  ],
  "lookmlOptimizations": [
    {
      "type": "aggregate_table|index|join_optimization|dimension_cleanup|explore_optimization",
      "recommendation": "Specific LookML change to make",
      "expectedImprovement": "Expected performance gain",
      "implementation": "How to implement this change",
      "lookmlCode": "view: example { aggregate_table: fast_summary { ... } }",
      "targetExplore": "Which explore this applies to"
    }
  ],
  "databaseOptimizations": [
    {
      "type": "index|partitioning|materialized_view",
      "recommendation": "Database-level optimization",
      "sqlCode": "CREATE INDEX idx_name ON table (columns);"
    }
  ],
  "priority": "critical|high|medium|low",
  "estimatedSpeedup": "60-80% faster"
}

Focus on identifying which explores are likely causing the performance issues and provide the most impactful optimizations.`;

        const response = await axios.post(
            `https://generativelanguage.googleapis.com/v1beta/models/gemini-2.0-flash:generateContent?key=${process.env.GEMINI_API_KEY}`,
            {
                contents: [{
                    parts: [{ text: prompt }]
                }]
            },
            {
                timeout: 45000,
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
    }

    // Helper methods for LookML parsing
    determineLookMLType(content) {
        if (content.includes('explore:')) return 'explore';
        if (content.includes('view:')) return 'view';
        if (content.includes('dashboard:')) return 'dashboard';
        return 'unknown';
    }

    extractJoins(content) {
        const joinMatches = content.match(/join:\s*(\w+)/g) || [];
        return joinMatches.map(match => match.replace('join:', '').trim());
    }

    extractDimensions(content) {
        const dimMatches = content.match(/dimension:\s*(\w+)/g) || [];
        return dimMatches.map(match => match.replace('dimension:', '').trim());
    }

    extractMeasures(content) {
        const measureMatches = content.match(/measure:\s*(\w+)/g) || [];
        return measureMatches.map(match => match.replace('measure:', '').trim());
    }

    extractExplores(content) {
        const exploreMatches = content.match(/explore:\s*(\w+)/g) || [];
        return exploreMatches.map(match => match.replace('explore:', '').trim());
    }

    groupQueriesByModel(queries) {
        const groups = {};
        queries.forEach(query => {
            if (query.model) {
                if (!groups[query.model]) groups[query.model] = [];
                groups[query.model].push(query);
            }
        });
        return groups;
    }

    analyzeModelLocally(model, queries, lookmlFiles) {
        return {
            model: model,
            performanceIssues: [
                {
                    issue: `${queries.length} slow queries detected in model ${model}`,
                    severity: 'medium',
                    affectedQueries: queries.length,
                    cause: 'Local analysis - enable Gemini for detailed insights'
                }
            ],
            lookmlOptimizations: [
                {
                    type: 'general',
                    recommendation: 'Enable Gemini API for AI-powered LookML optimization recommendations',
                    expectedImprovement: 'Detailed analysis available with AI',
                    implementation: 'Set GEMINI_API_KEY in environment variables'
                }
            ],
            priority: 'medium',
            estimatedSpeedup: 'Analysis pending'
        };
    }

    analyzeSlowQueriesLocally(slowQueries, lookmlFiles) {
        return slowQueries.map(query => ({
            query_id: query.query_id,
            runtime_seconds: query.runtime_seconds,
            model: query.model,
            explore: query.explore,
            issues: ['Local analysis - needs Gemini for detailed insights'],
            recommendations: ['Enable Gemini API for AI-powered recommendations']
        }));
    }

    // Main diagnostic method
    async runQueryPerformanceDiagnostic() {
        console.log('🏁 Starting Query Performance Diagnostic...');
        
        try {
            const mcpConnected = await this.initializeMCP();
            if (!mcpConnected) {
                throw new Error('MCP initialization failed');
            }

            // First, get available models to understand what's accessible
            const models = await this.getAvailableModels();
            console.log(`Found ${models.length} available models`);

            // Test query_metrics explore access
            console.log('Testing query_metrics explore access...');
            await this.testQueryMetricsExplore();

            // Get slow queries with explore context
            this.slowQueries = await this.fetchSlowQueriesWithContext();
            
            if (this.slowQueries.length === 0) {
                console.log('✅ No slow queries found (>5s runtime in past 7 days)');
                return this.generateNoSlowQueriesReport();
            }

            // Get LookML for the models with slow queries (rather than model/explore combinations)
            this.lookmlFiles = await this.fetchLookMLForSlowQueries(this.slowQueries);

            // Analyze with Gemini AI
            this.optimizations = await this.analyzeSlowQueriesWithGemini(this.slowQueries, this.lookmlFiles);

            // Generate comprehensive report
            const report = {
                timestamp: new Date(),
                summary: {
                    totalSlowQueries: this.slowQueries.length,
                    uniqueModels: [...new Set(this.slowQueries.map(q => q.model).filter(Boolean))].length,
                    avgRuntime: this.slowQueries.reduce((sum, q) => sum + q.runtime_seconds, 0) / this.slowQueries.length,
                    lookmlFilesAnalyzed: this.lookmlFiles.length,
                    availableModels: models.length
                },
                slowQueries: this.slowQueries,
                optimizations: this.optimizations,
                lookmlFiles: this.lookmlFiles.map(f => ({ 
                    model: f.model, 
                    name: f.name, 
                    type: f.type,
                    joins: f.joins,
                    dimensions: f.dimensions?.length || 0,
                    measures: f.measures?.length || 0,
                    explores: f.explores
                })),
                recommendations: this.generateTopRecommendations(),
                models: models
            };

            console.log('✅ Query Performance Diagnostic completed');
            console.log(`🐌 Analyzed ${this.slowQueries.length} slow queries`);
            console.log(`🏗️ Generated ${this.optimizations.length} optimization strategies`);
            
            return report;

        } catch (error) {
            console.error('❌ Query Performance Diagnostic failed:', error);
            throw error;
        }
    }

    generateNoSlowQueriesReport() {
        return {
            timestamp: new Date(),
            summary: {
                totalSlowQueries: 0,
                message: 'No slow queries detected (>5s runtime in past 7 days)',
                status: 'healthy'
            },
            recommendations: [
                'Query performance is currently good',
                'Consider monitoring for queries >3s if you want more proactive optimization',
                'Set up regular performance monitoring to catch issues early'
            ]
        };
    }

    generateTopRecommendations() {
        if (this.optimizations.length === 0) return [];
        
        return this.optimizations
            .filter(opt => opt.priority === 'critical' || opt.priority === 'high')
            .slice(0, 5)
            .map(opt => ({
                model: opt.model,
                topRecommendation: opt.lookmlOptimizations?.[0]?.recommendation || 'See full analysis',
                expectedImprovement: opt.estimatedSpeedup || 'Significant improvement expected'
            }));
    }
}

module.exports = { QueryPerformanceDiagnostic };