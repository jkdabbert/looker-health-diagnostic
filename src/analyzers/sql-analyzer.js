// src/analyzers/sql-analyzer.js
// Complete SQL analyzer with direct Looker API SQL fetching

class SQLAnalyzer {
    constructor(config = {}) {
        this.config = config;
        this.geminiApiKey = config.geminiApiKey || process.env.GEMINI_API_KEY;
        this.hasAI = !!this.geminiApiKey;
        this.lookerApiConnector = null; // Will be injected
    }

    /**
     * Set the Looker API connector for direct API calls
     */
    setLookerApiConnector(lookerApiConnector) {
        this.lookerApiConnector = lookerApiConnector;
    }

    /**
     * Analyze slow queries with direct API SQL fetching
     */
    async analyzeSlowQueries(queries, mcpConnector, lookerApiConnector = null) {
        console.log(`Analyzing ${queries.length} slow queries for actual SQL optimization...`);
        
        // Set the API connector if provided
        if (lookerApiConnector) {
            this.lookerApiConnector = lookerApiConnector;
        }
        
        const analyses = [];
        
        for (const query of queries) {
            try {
                const analysis = await this.analyzeRealQuery(query, mcpConnector);
                if (analysis) {
                    analyses.push(analysis);
                }
            } catch (error) {
                console.error(`Failed to analyze query ${query.query_id}:`, error.message);
                analyses.push(this.createEnhancedBasicAnalysis(query));
            }
        }

        console.log(`Completed analysis of ${analyses.length} queries`);
        return analyses;
    }

    /**
     * Analyze query with direct API SQL fetching
     */
    async analyzeRealQuery(query, mcpConnector) {
        const queryId = query.query_id || query['query.id'];
        const runtime = parseFloat(query.runtime_seconds || query['history.runtime'] || 0);
        
        console.log(`Analyzing query ${queryId} with ${runtime}s runtime...`);
        
        // Strategy: Use direct Looker API to get SQL
        const actualSQL = await this.fetchActualSQLViaAPI(query);
        
        if (!actualSQL) {
            console.log(`Could not fetch SQL for query ${queryId}, using enhanced heuristic analysis`);
            return this.createEnhancedBasicAnalysis(query);
        }

        console.log(`Got ${actualSQL.length} characters of actual SQL for query ${queryId}`);

        // Analyze the real SQL
        const analysis = this.hasAI 
            ? await this.analyzeWithAI(actualSQL, runtime, query)
            : this.analyzeWithEnhancedHeuristics(actualSQL, runtime, query);

        return {
            queryId: queryId,
            slug: query.slug || query['query.slug'],
            runtime: runtime,
            model: query.model || query['query.model'],
            explore: query.explore || query['query.explore'],
            dashboard: query.dashboard_title || query['dashboard.title'],
            user: query.user_email || query['user.email'],
            createdDate: query.created_date || query['history.created_date'],
            
            // The ACTUAL SQL from Looker API
            originalSQL: actualSQL,
            sqlLength: actualSQL.length,
            
            // Enhanced analysis
            analysis: analysis,
            issues: analysis.issues || [],
            recommendations: analysis.recommendations || [],
            optimizedSQL: analysis.optimizedSQL || null,
            overallPriority: this.calculatePriority(runtime, analysis.complexity || 'medium'),
            implementationSteps: analysis.implementationSteps || [],
            
            // SQL metadata
            sqlAnalysis: {
                hasActualSQL: true,
                fetchMethod: 'looker_api',
                sqlComplexityScore: this.calculateSQLComplexity(actualSQL),
                joinCount: this.countJoins(actualSQL),
                whereClausePresent: actualSQL.toLowerCase().includes('where'),
                orderByClauseCount: (actualSQL.toLowerCase().match(/order by/g) || []).length,
                subqueryCount: this.countSubqueries(actualSQL),
                lookmlGenerated: actualSQL.includes('-- Looker') || actualSQL.includes('${TABLE}'),
                estimatedRows: this.estimateRowsFromSQL(actualSQL),
                hasAggregations: this.hasAggregations(actualSQL)
            }
        };
    }

    /**
     * Fetch actual SQL using direct Looker API calls
     */
    async fetchActualSQLViaAPI(query) {
        const slug = query.slug || query['query.slug'];
        const queryId = query.query_id || query['query.id'];
        
        if (!slug && !queryId) {
            console.log(`No slug or query ID available for query`);
            return null;
        }

        // Strategy 1: Use query slug with run endpoint (most reliable)
        if (slug && this.lookerApiConnector) {
            try {
                console.log(`API Strategy 1: Fetching SQL via query slug ${slug}...`);
                const sql = await this.fetchSQLBySlug(slug);
                if (sql && this.isValidSQL(sql)) {
                    console.log(`✅ API Strategy 1 succeeded: Found SQL via slug`);
                    return sql;
                }
            } catch (error) {
                console.log(`API Strategy 1 failed: ${error.message}`);
            }
        }

        // Strategy 2: Try query ID if available
        if (queryId && this.lookerApiConnector) {
            try {
                console.log(`API Strategy 2: Fetching SQL via query ID ${queryId}...`);
                const sql = await this.fetchSQLByQueryId(queryId);
                if (sql && this.isValidSQL(sql)) {
                    console.log(`✅ API Strategy 2 succeeded: Found SQL via query ID`);
                    return sql;
                }
            } catch (error) {
                console.log(`API Strategy 2 failed: ${error.message}`);
            }
        }

        // Strategy 3: Try reconstructing query and getting SQL
        if (query.model && query.explore && this.lookerApiConnector) {
            try {
                console.log(`API Strategy 3: Reconstructing query to get similar SQL...`);
                const sql = await this.reconstructAndFetchSQL(query);
                if (sql && this.isValidSQL(sql)) {
                    console.log(`✅ API Strategy 3 succeeded: Generated representative SQL`);
                    return `-- Representative SQL based on query characteristics\n-- Original query: ${slug || queryId}\n\n${sql}`;
                }
            } catch (error) {
                console.log(`API Strategy 3 failed: ${error.message}`);
            }
        }

        console.log(`All API strategies failed for query ${slug || queryId}`);
        return null;
    }

    /**
     * Fetch SQL using query slug via direct API call
     */
    async fetchSQLBySlug(slug) {
        if (!this.lookerApiConnector) {
            throw new Error('Looker API connector not available');
        }

        try {
            // Try the queries/slug/run/sql endpoint
            console.log(`Trying API endpoint: /queries/${slug}/run/sql`);
            const response = await this.lookerApiConnector.makeApiRequest(
                `/queries/${slug}/run/sql`,
                'GET',
                null,
                null,
                45000 // 45 second timeout for SQL generation
            );

            return this.extractSQLFromAPIResponse(response);

        } catch (error) {
            console.log(`Primary SQL endpoint failed: ${error.message}`);
            
            // Try alternative endpoint format
            try {
                console.log(`Trying alternative endpoint: /queries/run/${slug}`);
                const altResponse = await this.lookerApiConnector.makeApiRequest(
                    `/queries/run/${slug}`,
                    'GET',
                    null,
                    { result_format: 'sql' },
                    45000
                );

                return this.extractSQLFromAPIResponse(altResponse);

            } catch (altError) {
                console.log(`Alternative SQL endpoint failed: ${altError.message}`);
                
                // Try POST method
                try {
                    console.log(`Trying POST method for SQL retrieval...`);
                    const postResponse = await this.lookerApiConnector.makeApiRequest(
                        `/queries/${slug}/run/sql`,
                        'POST',
                        {},
                        null,
                        45000
                    );

                    return this.extractSQLFromAPIResponse(postResponse);

                } catch (postError) {
                    throw new Error(`All SQL fetch attempts failed: ${postError.message}`);
                }
            }
        }
    }

    /**
     * Fetch SQL using query ID
     */
    async fetchSQLByQueryId(queryId) {
        if (!this.lookerApiConnector) {
            throw new Error('Looker API connector not available');
        }

        try {
            // Get query details first
            console.log(`Getting query details for ID ${queryId}...`);
            const queryDetails = await this.lookerApiConnector.makeApiRequest(
                `/queries/${queryId}`,
                'GET',
                null,
                null,
                30000
            );

            if (queryDetails && queryDetails.slug) {
                console.log(`Found slug ${queryDetails.slug} for query ID ${queryId}, fetching SQL...`);
                return await this.fetchSQLBySlug(queryDetails.slug);
            }

            // Try direct run with query ID
            const response = await this.lookerApiConnector.makeApiRequest(
                `/queries/${queryId}/run`,
                'GET',
                null,
                { result_format: 'sql' },
                45000
            );

            return this.extractSQLFromAPIResponse(response);

        } catch (error) {
            throw new Error(`Failed to fetch SQL by query ID: ${error.message}`);
        }
    }

    /**
     * Reconstruct query and fetch representative SQL
     */
    async reconstructAndFetchSQL(query) {
        if (!this.lookerApiConnector) {
            throw new Error('Looker API connector not available');
        }

        try {
            const model = query.model || query['query.model'];
            const explore = query.explore || query['query.explore'];

            console.log(`Reconstructing query for ${model}.${explore}...`);

            // Create a simple query to get representative SQL
            const queryBody = {
                model: model,
                view: explore,
                fields: [`${explore}.count`],
                limit: '10'
            };

            // Create and run query to get SQL
            const createResponse = await this.lookerApiConnector.makeApiRequest(
                '/queries',
                'POST',
                queryBody,
                null,
                30000
            );

            if (createResponse && createResponse.slug) {
                console.log(`Created representative query with slug ${createResponse.slug}`);
                const sql = await this.fetchSQLBySlug(createResponse.slug);
                return sql ? `${sql}\n\n-- Note: This is representative SQL for ${model}.${explore}, not the exact original query` : null;
            }

            return null;

        } catch (error) {
            throw new Error(`Failed to reconstruct and fetch SQL: ${error.message}`);
        }
    }

    /**
     * Extract SQL from API response
     */
    extractSQLFromAPIResponse(response) {
        if (!response) return null;

        // Handle string responses (most common for SQL)
        if (typeof response === 'string') {
            const trimmed = response.trim();
            if (this.isValidSQL(trimmed)) {
                return trimmed;
            }
        }

        // Handle object responses
        if (response && typeof response === 'object') {
            // Look for SQL in various possible fields
            const sqlFields = ['sql', 'query_sql', 'data', 'result', 'content'];
            
            for (const field of sqlFields) {
                if (response[field] && typeof response[field] === 'string') {
                    if (this.isValidSQL(response[field])) {
                        return response[field];
                    }
                }
            }

            // Handle array responses
            if (Array.isArray(response)) {
                for (const item of response) {
                    if (typeof item === 'string' && this.isValidSQL(item)) {
                        return item;
                    }
                    if (item && item.sql && this.isValidSQL(item.sql)) {
                        return item.sql;
                    }
                }
            }
        }

        console.log('Could not extract valid SQL from API response');
        return null;
    }

    /**
     * Enhanced SQL validation
     */
    isValidSQL(sqlString) {
        if (!sqlString || typeof sqlString !== 'string') return false;
        
        const sql = sqlString.toLowerCase().trim();
        
        // Must contain SELECT
        if (!sql.includes('select')) return false;
        
        // Should be substantial content
        if (sql.length < 50) return false;
        
        // Should not be error messages
        const errorPatterns = ['error', 'failed', 'access denied', 'unauthorized', 'forbidden'];
        if (errorPatterns.some(pattern => sql.includes(pattern))) return false;
        
        // Should not be HTML
        if (sql.includes('<html>') || sql.includes('<!doctype')) return false;
        
        // Should contain typical SQL keywords
        const sqlKeywords = ['from', 'join', 'where', 'group by', 'order by', 'limit'];
        const keywordCount = sqlKeywords.filter(keyword => sql.includes(keyword)).length;
        
        return keywordCount >= 2 || (sql.startsWith('select') && sql.length > 100);
    }

    /**
     * Calculate SQL complexity score
     */
    calculateSQLComplexity(sql) {
        let complexity = 0;
        const sqlLower = sql.toLowerCase();
        
        // Basic structure
        complexity += sql.split('\n').length * 0.5;
        
        // JOINs add significant complexity
        complexity += this.countJoins(sql) * 8;
        
        // Subqueries
        complexity += this.countSubqueries(sql) * 12;
        
        // Aggregations
        complexity += this.countAggregations(sql) * 3;
        
        // Window functions
        complexity += (sqlLower.match(/over\s*\(/g) || []).length * 15;
        
        // CASE statements
        complexity += (sqlLower.match(/case\s+when/g) || []).length * 4;
        
        // CTEs
        complexity += (sqlLower.match(/with\s+\w+\s+as/g) || []).length * 6;
        
        return Math.min(complexity, 150);
    }

    countJoins(sql) {
        const sqlLower = sql.toLowerCase();
        return (sqlLower.match(/\b(left|right|inner|outer|full)?\s*join\b/g) || []).length;
    }

    countSubqueries(sql) {
        const matches = sql.match(/\(\s*select/gi);
        return matches ? matches.length : 0;
    }

    countAggregations(sql) {
        const sqlLower = sql.toLowerCase();
        return (sqlLower.match(/\b(count|sum|avg|max|min|string_agg)\s*\(/g) || []).length;
    }

    hasAggregations(sql) {
        return this.countAggregations(sql) > 0;
    }

    estimateRowsFromSQL(sql) {
        const sqlLower = sql.toLowerCase();
        
        const limitMatch = sqlLower.match(/limit\s+(\d+)/);
        if (limitMatch) {
            const limit = parseInt(limitMatch[1]);
            if (limit < 1000) return `Up to ${limit} rows`;
            if (limit < 10000) return `${limit} rows (medium dataset)`;
            return `${limit} rows (large dataset)`;
        }
        
        const joinCount = this.countJoins(sql);
        const hasGroupBy = sqlLower.includes('group by');
        
        if (joinCount > 4 && hasGroupBy) return 'Potentially millions of rows';
        if (joinCount > 2 && hasGroupBy) return 'Hundreds of thousands of rows';
        if (hasGroupBy) return 'Tens of thousands of rows';
        if (joinCount > 2) return 'Large dataset (100k+ rows)';
        
        return 'Medium dataset (1k-100k rows)';
    }

    /**
     * Enhanced heuristic analysis for actual SQL
     */
    analyzeWithEnhancedHeuristics(sql, runtime, query) {
        const issues = [];
        const recommendations = [];
        let complexity = 'medium';

        const sqlLower = sql.toLowerCase();
        const sqlComplexity = this.calculateSQLComplexity(sql);
        
        // Analyze SQL structure
        if (sqlLower.includes('select *')) {
            issues.push({
                type: 'performance',
                severity: 'medium',
                description: 'Query uses SELECT * which fetches unnecessary columns',
                location: 'SELECT clause',
                recommendation: 'Specify only required columns'
            });
        }

        const joinCount = this.countJoins(sql);
        if (joinCount > 5) {
            complexity = 'high';
            issues.push({
                type: 'performance',
                severity: 'high',
                description: `Query has ${joinCount} JOINs which significantly impacts performance`,
                location: 'JOIN clauses',
                recommendation: 'Consider PDT to pre-compute joins'
            });
        }

        if (!sqlLower.includes('where') && sqlLower.includes('from')) {
            issues.push({
                type: 'performance',
                severity: 'high',
                description: 'Query lacks WHERE clause for data filtering',
                location: 'Missing WHERE clause',
                recommendation: 'Add WHERE conditions to limit data scanned'
            });
        }

        // Runtime-based recommendations
        if (runtime > 300) {
            recommendations.push({
                type: 'lookml_improvement',
                priority: 'critical',
                title: 'URGENT: Implement PDT',
                description: 'Query exceeds 5 minutes - needs immediate PDT implementation',
                expectedImprovement: '85-95%',
                effort: 'medium',
                category: 'lookml'
            });
        } else if (runtime > 120) {
            recommendations.push({
                type: 'lookml_improvement',
                priority: 'high',
                title: 'Implement PDT',
                description: 'Create PDT for this expensive query pattern',
                expectedImprovement: '70-85%',
                effort: 'medium',
                category: 'lookml'
            });
        }

        return {
            complexity,
            issues,
            recommendations,
            optimizedSQL: this.generateOptimizedSQL(sql, issues),
            lookmlSuggestions: this.generateLookMLSuggestions(query, sql, runtime),
            performanceAnalysis: {
                estimatedRowsProcessed: this.estimateRowsFromSQL(sql),
                expensiveOperations: this.findExpensiveOperations(sql),
                bottlenecks: this.identifyBottlenecks(sql, runtime)
            },
            implementationSteps: this.generateImplementationSteps(recommendations)
        };
    }

    findExpensiveOperations(sql) {
        const operations = [];
        const sqlLower = sql.toLowerCase();
        
        if (sqlLower.includes('distinct')) operations.push('DISTINCT operation');
        if (this.countJoins(sql) > 3) operations.push('Multiple complex JOINs');
        if (this.countSubqueries(sql) > 1) operations.push('Multiple subqueries');
        if (sqlLower.includes('group by') && this.countAggregations(sql) > 3) operations.push('Complex aggregations');
        if (sqlLower.includes('order by') && !sqlLower.includes('limit')) operations.push('Unlimited ORDER BY');
        
        return operations;
    }

    identifyBottlenecks(sql, runtime) {
        const bottlenecks = [];
        
        if (runtime > 300) bottlenecks.push('Extremely long runtime suggests major performance issues');
        if (this.countJoins(sql) > 5) bottlenecks.push('Multiple JOINs likely causing performance issues');
        if (!sql.toLowerCase().includes('where')) bottlenecks.push('Missing WHERE clause causing full table scan');
        if (sql.toLowerCase().includes('select *')) bottlenecks.push('SELECT * fetching unnecessary columns');
        
        return bottlenecks;
    }

    generateOptimizedSQL(originalSQL, issues) {
        let optimizations = `-- OPTIMIZED SQL VERSION\n-- Based on actual SQL analysis\n\n`;
        
        issues.forEach((issue, index) => {
            optimizations += `-- Issue ${index + 1}: ${issue.description}\n-- Fix: ${issue.recommendation}\n`;
        });
        
        optimizations += `\n-- ORIGINAL SQL:\n${originalSQL}`;
        
        return optimizations;
    }

    generateLookMLSuggestions(query, sql, runtime) {
        const suggestions = [];
        
        if (runtime > 120) {
            suggestions.push({
                type: 'pdt',
                suggestion: 'Create PDT for this query pattern',
                reasoning: `Query takes ${runtime}s, PDT would pre-compute results`,
                code: this.generatePDTCode(query, sql)
            });
        }
        
        return suggestions;
    }

    generatePDTCode(query, sql) {
        const model = query.model || 'model';
        const explore = query.explore || 'explore';
        
        return `view: pdt_optimized_${explore} {
  derived_table: {
    sql: ${sql.split('\n').map(line => `      ${line}`).join('\n')} ;;
    
    datagroup_trigger: ${model}_datagroup
    distribution_style: even
    sortkeys: ["created_date"]
  }
  
  dimension: id {
    primary_key: yes
    type: string
    sql: \${TABLE}.id ;;
  }
  
  measure: count {
    type: count
    label: "Record Count"
  }
}`;
    }

    generateImplementationSteps(recommendations) {
        return recommendations.map((rec, index) => ({
            step: index + 1,
            title: rec.title,
            description: rec.description,
            effort: rec.effort,
            priority: rec.priority,
            category: rec.category
        }));
    }

    calculatePriority(runtime, complexity) {
        if (runtime > 300 || complexity === 'critical') return 'critical';
        if (runtime > 120 || complexity === 'high') return 'high';
        if (runtime > 60 || complexity === 'medium') return 'medium';
        return 'low';
    }

    createEnhancedBasicAnalysis(query) {
        const runtime = parseFloat(query.runtime_seconds || query['history.runtime'] || 0);
        const queryId = query.query_id || query['query.id'];
        
        return {
            queryId: queryId,
            slug: query.slug || query['query.slug'],
            runtime: runtime,
            model: query.model || query['query.model'],
            explore: query.explore || query['query.explore'],
            dashboard: query.dashboard_title || query['dashboard.title'],
            user: query.user_email || query['user.email'],
            
            originalSQL: null,
            sqlLength: 0,
            
            analysis: {
                complexity: runtime > 300 ? 'critical' : runtime > 120 ? 'high' : 'medium',
                issues: [{
                    type: 'performance',
                    severity: runtime > 300 ? 'critical' : runtime > 120 ? 'high' : 'medium',
                    description: `Query runtime of ${runtime}s indicates performance issues`,
                    recommendation: 'SQL not accessible - requires manual investigation'
                }],
                recommendations: [{
                    type: 'manual_investigation',
                    priority: runtime > 300 ? 'critical' : 'high',
                    title: 'Manual Query Investigation Required',
                    description: 'Could not fetch actual SQL - requires direct analysis in Looker',
                    expectedImprovement: 'Unknown',
                    effort: 'medium',
                    category: 'investigation'
                }]
            },
            
            sqlAnalysis: {
                hasActualSQL: false,
                fetchMethod: 'failed',
                analysisMethod: 'metadata_only'
            },
            
            overallPriority: this.calculatePriority(runtime, 'medium')
        };
    }

    // AI analysis methods (keeping existing implementation)
    async analyzeWithAI(sql, runtime, query) {
        if (!this.hasAI) {
            return this.analyzeWithEnhancedHeuristics(sql, runtime, query);
        }

        try {
            console.log('Analyzing actual SQL with AI...');
            const prompt = this.buildEnhancedAnalysisPrompt(sql, runtime, query);
            const aiResponse = await this.callGeminiAPI(prompt);
            return this.parseAIResponse(aiResponse, sql, runtime);
        } catch (error) {
            console.error('AI analysis failed, falling back to heuristics:', error.message);
            return this.analyzeWithEnhancedHeuristics(sql, runtime, query);
        }
    }

    buildEnhancedAnalysisPrompt(sql, runtime, query) {
        return `You are a Looker and SQL performance expert. Analyze this slow query and provide specific optimization recommendations.

QUERY DETAILS:
- Runtime: ${runtime} seconds
- Model: ${query.model || 'Unknown'}
- Explore: ${query.explore || 'Unknown'}

ACTUAL SQL FROM LOOKER:
\`\`\`sql
${sql}
\`\`\`

Provide detailed JSON response with optimization recommendations.`;
    }

    async callGeminiAPI(prompt) {
        const axios = require('axios');
        
        const response = await axios.post(
            `https://generativelanguage.googleapis.com/v1beta/models/gemini-pro:generateContent?key=${this.geminiApiKey}`,
            {
                contents: [{ parts: [{ text: prompt }] }],
                generationConfig: { temperature: 0.1, maxOutputTokens: 2048 }
            },
            { headers: { 'Content-Type': 'application/json' }, timeout: 30000 }
        );

        return response.data.candidates[0].content.parts[0].text;
    }

    parseAIResponse(aiResponse, originalSQL, runtime) {
        try {
            const jsonMatch = aiResponse.match(/\{[\s\S]*\}/);
            if (jsonMatch) {
                const parsed = JSON.parse(jsonMatch[0]);
                return {
                    complexity: parsed.complexity || 'medium',
                    issues: parsed.issues || [],
                    recommendations: parsed.recommendations || [],
                    optimizedSQL: parsed.optimizedSQL || null,
                    aiPowered: true,
                    implementationSteps: this.generateImplementationSteps(parsed.recommendations || [])
                };
            }
        } catch (parseError) {
            console.error('Failed to parse AI response:', parseError.message);
        }
        
        return this.analyzeWithEnhancedHeuristics(originalSQL, runtime, {});
    }
}

module.exports = { SQLAnalyzer };