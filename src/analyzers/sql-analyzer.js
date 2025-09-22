// src/analyzers/sql-analyzer.js
// Analyzes ACTUAL SQL queries from Looker with AI assistance

class SQLAnalyzer {
    constructor(config = {}) {
        this.config = config;
        this.geminiApiKey = config.geminiApiKey || process.env.GEMINI_API_KEY;
        this.hasAI = !!this.geminiApiKey;
    }

    /**
     * Analyze slow queries by fetching their actual SQL
     */
    async analyzeSlowQueries(queries, mcpConnector) {
        console.log(`Analyzing ${queries.length} slow queries for actual SQL optimization...`);
        
        const analyses = [];
        
        for (const query of queries) {
            try {
                const analysis = await this.analyzeRealQuery(query, mcpConnector);
                if (analysis) {
                    analyses.push(analysis);
                }
            } catch (error) {
                console.error(`Failed to analyze query ${query.query_id}:`, error.message);
                // Add a basic analysis without SQL
                analyses.push(this.createBasicAnalysis(query));
            }
        }

        return analyses;
    }

    /**
     * Analyze a single query by fetching its actual SQL
     */
    async analyzeRealQuery(query, mcpConnector) {
        const queryId = query.query_id || query['query.id'];
        const runtime = parseFloat(query.runtime_seconds || query['history.runtime'] || 0);
        
        console.log(`Fetching actual SQL for query ${queryId}...`);
        
        // Step 1: Get the actual SQL from Looker
        const actualSQL = await this.fetchActualSQL(query, mcpConnector);
        
        if (!actualSQL) {
            console.log(`Could not fetch SQL for query ${queryId}, using basic analysis`);
            return this.createBasicAnalysis(query);
        }

        console.log(`Got ${actualSQL.length} characters of SQL for query ${queryId}`);

        // Step 2: Analyze the real SQL with AI
        const aiAnalysis = this.hasAI 
            ? await this.analyzeWithAI(actualSQL, runtime, query)
            : this.analyzeWithHeuristics(actualSQL, runtime, query);

        return {
            queryId: queryId,
            slug: query.slug || query['query.slug'],
            runtime: runtime,
            model: query.model || query['query.model'],
            explore: query.explore || query['query.explore'],
            dashboard: query.dashboard_title || query['dashboard.title'],
            user: query.user_email || query['user.email'],
            createdDate: query.created_date || query['history.created_date'],
            
            // The ACTUAL SQL from Looker
            originalSQL: actualSQL,
            sqlLength: actualSQL.length,
            
            // AI-powered analysis
            aiAnalysis: aiAnalysis,
            
            // SQL-specific issues found in actual query
            issues: aiAnalysis.issues || [],
            
            // Specific recommendations for this query
            recommendations: aiAnalysis.recommendations || [],
            
            // Optimized version
            optimizedSQL: aiAnalysis.optimizedSQL || null,
            
            // Priority based on actual analysis
            overallPriority: this.calculatePriority(runtime, aiAnalysis.complexity || 'medium'),
            
            // Implementation steps
            implementationSteps: aiAnalysis.implementationSteps || []
        };
    }

    /**
     * Fetch actual SQL from Looker query
     */
    async fetchActualSQL(query, mcpConnector) {
        const queryId = query.query_id || query['query.id'];
        
        try {
            // Method 1: Try to get SQL via MCP query_sql tool (if available)
            if (mcpConnector) {
                try {
                    console.log(`Attempting to fetch SQL via MCP for query ${queryId}...`);
                    const sqlResponse = await mcpConnector.executeTool('query_sql', {
                        query_id: queryId
                    });
                    
                    if (sqlResponse && sqlResponse.length > 0) {
                        const sqlData = sqlResponse[0];
                        if (sqlData.sql || sqlData.query_sql) {
                            return sqlData.sql || sqlData.query_sql;
                        }
                    }
                } catch (mcpError) {
                    console.log(`MCP SQL fetch failed for ${queryId}:`, mcpError.message);
                }
            }

            // Method 2: Try to get SQL from query details (if query ID format suggests it's available)
            if (query.slug) {
                // Sometimes the slug contains information we can use
                console.log(`Query ${queryId} has slug: ${query.slug}`);
            }

            // Method 3: Return null if we can't fetch - the analyzer will use basic analysis
            console.log(`Could not fetch actual SQL for query ${queryId}`);
            return null;

        } catch (error) {
            console.error(`Error fetching SQL for query ${queryId}:`, error.message);
            return null;
        }
    }

    /**
     * Analyze SQL using AI (Gemini)
     */
    async analyzeWithAI(sql, runtime, query) {
        if (!this.hasAI) {
            return this.analyzeWithHeuristics(sql, runtime, query);
        }

        try {
            console.log('Analyzing SQL with AI...');
            
            const prompt = this.buildAnalysisPrompt(sql, runtime, query);
            const aiResponse = await this.callGeminiAPI(prompt);
            
            return this.parseAIResponse(aiResponse, sql, runtime);
            
        } catch (error) {
            console.error('AI analysis failed, falling back to heuristics:', error.message);
            return this.analyzeWithHeuristics(sql, runtime, query);
        }
    }

    /**
     * Build analysis prompt for AI
     */
    buildAnalysisPrompt(sql, runtime, query) {
        return `You are a Looker and BigQuery performance expert. Analyze this slow SQL query and provide specific optimization recommendations.

QUERY DETAILS:
- Runtime: ${runtime} seconds
- Model: ${query.model || 'Unknown'}
- Explore: ${query.explore || 'Unknown'}
- Dashboard: ${query.dashboard_title || 'None'}

SQL TO ANALYZE:
\`\`\`sql
${sql}
\`\`\`

Please provide a JSON response with the following structure:
{
  "complexity": "low|medium|high|critical",
  "issues": [
    {
      "type": "performance|structure|bigquery_specific",
      "severity": "low|medium|high|critical",
      "description": "Specific issue found",
      "lineNumbers": [1, 2],
      "recommendation": "Specific fix"
    }
  ],
  "recommendations": [
    {
      "type": "query_optimization|lookml_improvement|bigquery_optimization",
      "priority": "low|medium|high|critical",
      "title": "Recommendation title",
      "description": "What to change",
      "expectedImprovement": "Performance improvement estimate",
      "effort": "low|medium|high",
      "category": "sql|lookml|bigquery"
    }
  ],
  "optimizedSQL": "-- Optimized version of the SQL query with improvements",
  "bigqueryOptimizations": [
    {
      "type": "partitioning|clustering|materialized_view|query_structure",
      "suggestion": "Specific BigQuery optimization",
      "impact": "Expected performance impact"
    }
  ],
  "lookmlSuggestions": [
    {
      "type": "pdt|aggregate_table|datagroup|caching",
      "suggestion": "Specific LookML improvement",
      "code": "Example LookML code if applicable"
    }
  ]
}

Focus on:
1. Actual SQL performance issues (JOINs, WHERE clauses, GROUP BY, etc.)
2. BigQuery-specific optimizations (partitioning, clustering, slots)
3. LookML improvements (PDTs, aggregate tables, caching)
4. Provide specific, actionable recommendations with code examples`;
    }

    /**
     * Call Gemini API for analysis
     */
    async callGeminiAPI(prompt) {
        const axios = require('axios');
        
        const response = await axios.post(
            `https://generativelanguage.googleapis.com/v1beta/models/gemini-pro:generateContent?key=${this.geminiApiKey}`,
            {
                contents: [{
                    parts: [{
                        text: prompt
                    }]
                }],
                generationConfig: {
                    temperature: 0.1,
                    topK: 1,
                    topP: 1,
                    maxOutputTokens: 2048
                }
            },
            {
                headers: {
                    'Content-Type': 'application/json'
                },
                timeout: 30000
            }
        );

        return response.data.candidates[0].content.parts[0].text;
    }

    /**
     * Parse AI response
     */
    parseAIResponse(aiResponse, originalSQL, runtime) {
        try {
            // Try to extract JSON from the AI response
            const jsonMatch = aiResponse.match(/\{[\s\S]*\}/);
            if (jsonMatch) {
                const parsed = JSON.parse(jsonMatch[0]);
                return {
                    complexity: parsed.complexity || 'medium',
                    issues: parsed.issues || [],
                    recommendations: parsed.recommendations || [],
                    optimizedSQL: parsed.optimizedSQL || null,
                    bigqueryOptimizations: parsed.bigqueryOptimizations || [],
                    lookmlSuggestions: parsed.lookmlSuggestions || [],
                    aiPowered: true,
                    implementationSteps: this.generateImplementationSteps(parsed)
                };
            }
        } catch (parseError) {
            console.error('Failed to parse AI response:', parseError.message);
        }
        
        // Fallback to heuristic analysis
        return this.analyzeWithHeuristics(originalSQL, runtime, {});
    }

    /**
     * Fallback heuristic analysis when AI is not available
     */
    analyzeWithHeuristics(sql, runtime, query) {
        const issues = [];
        const recommendations = [];
        let complexity = 'medium';

        // Basic SQL analysis
        const sqlLower = sql.toLowerCase();
        
        // Check for common performance issues
        if (sqlLower.includes('select *')) {
            issues.push({
                type: 'performance',
                severity: 'medium',
                description: 'Query uses SELECT * which may fetch unnecessary columns',
                recommendation: 'Specify only required columns in SELECT clause'
            });
        }

        const joinCount = (sqlLower.match(/join/g) || []).length;
        if (joinCount > 3) {
            complexity = 'high';
            issues.push({
                type: 'performance',
                severity: 'high',
                description: `Query has ${joinCount} JOINs which may impact performance`,
                recommendation: 'Consider using PDT or aggregate tables to pre-compute joins'
            });
        }

        if (!sqlLower.includes('where') && sqlLower.includes('from')) {
            issues.push({
                type: 'performance',
                severity: 'high',
                description: 'Query lacks WHERE clause for filtering',
                recommendation: 'Add appropriate WHERE conditions to limit data scanned'
            });
        }

        // Generate recommendations based on runtime
        if (runtime > 30) {
            recommendations.push({
                type: 'lookml_improvement',
                priority: 'high',
                title: 'Implement PDT for this query pattern',
                description: 'Create a Persistent Derived Table to pre-compute this expensive query',
                expectedImprovement: '70-90% performance improvement',
                effort: 'medium',
                category: 'lookml'
            });
        }

        return {
            complexity,
            issues,
            recommendations,
            optimizedSQL: this.generateBasicOptimizedSQL(sql),
            bigqueryOptimizations: [],
            lookmlSuggestions: [],
            aiPowered: false,
            implementationSteps: []
        };
    }

    generateBasicOptimizedSQL(originalSQL) {
        // Basic optimization suggestions
        return `-- OPTIMIZED VERSION (Heuristic Analysis)
-- Original query with suggested improvements:

${originalSQL}

-- SUGGESTED OPTIMIZATIONS:
-- 1. Add specific column selection instead of SELECT *
-- 2. Add appropriate WHERE clauses for filtering
-- 3. Consider adding LIMIT clause if not present
-- 4. Review JOIN order and conditions
-- 5. Consider using PDT for complex aggregations`;
    }

    generateImplementationSteps(parsedAnalysis) {
        const steps = [];
        
        if (parsedAnalysis.lookmlSuggestions?.length > 0) {
            steps.push({
                step: 1,
                title: 'Implement LookML Improvements',
                description: 'Apply suggested LookML optimizations',
                effort: 'Medium',
                timeframe: '1-2 days'
            });
        }

        if (parsedAnalysis.bigqueryOptimizations?.length > 0) {
            steps.push({
                step: 2,
                title: 'Apply BigQuery Optimizations',
                description: 'Implement database-level improvements',
                effort: 'High',
                timeframe: '2-5 days'
            });
        }

        return steps;
    }

    createBasicAnalysis(query) {
        const runtime = parseFloat(query.runtime_seconds || query['history.runtime'] || 0);
        
        return {
            queryId: query.query_id || query['query.id'],
            slug: query.slug || query['query.slug'],
            runtime: runtime,
            model: query.model || query['query.model'],
            explore: query.explore || query['query.explore'],
            dashboard: query.dashboard_title || query['dashboard.title'],
            user: query.user_email || query['user.email'],
            
            originalSQL: null,
            sqlLength: 0,
            
            aiAnalysis: {
                complexity: runtime > 60 ? 'high' : runtime > 30 ? 'medium' : 'low',
                issues: [{
                    type: 'performance',
                    severity: runtime > 60 ? 'high' : 'medium',
                    description: `Query runtime of ${runtime}s indicates performance issues`,
                    recommendation: 'Unable to fetch SQL for detailed analysis - consider manual review'
                }],
                recommendations: [{
                    type: 'query_optimization',
                    priority: 'medium',
                    title: 'Manual Query Review Needed',
                    description: 'Could not fetch actual SQL - requires manual investigation',
                    expectedImprovement: 'Unknown',
                    effort: 'medium',
                    category: 'sql'
                }],
                aiPowered: false
            },
            
            issues: [],
            recommendations: [],
            overallPriority: this.calculatePriority(runtime, 'medium')
        };
    }

    calculatePriority(runtime, complexity) {
        if (runtime > 120 || complexity === 'critical') return 'critical';
        if (runtime > 60 || complexity === 'high') return 'high';
        if (runtime > 30 || complexity === 'medium') return 'medium';
        return 'low';
    }
}

module.exports = { SQLAnalyzer };