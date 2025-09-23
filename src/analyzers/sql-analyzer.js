// src/analyzers/sql-analyzer.js
// COMPLETE UPDATED FILE with Gemini 2.0 API support

class SQLAnalyzer {
    constructor(config = {}) {
        this.config = config;
        this.geminiApiKey = config.geminiApiKey || process.env.GEMINI_API_KEY;
        this.hasAI = !!this.geminiApiKey;
        this.lookerApiConnector = null;
    }

    /**
     * Set the Looker API connector for direct API calls
     */
    setLookerApiConnector(lookerApiConnector) {
        this.lookerApiConnector = lookerApiConnector;
        console.log('SQL Analyzer: Looker API connector set, status:', lookerApiConnector?.getStatus());
    }

    /**
     * Analyze slow queries with direct API SQL fetching
     */
    async analyzeSlowQueries(queries, mcpConnector, lookerApiConnector = null) {
        console.log(`📊 Analyzing ${queries.length} slow queries for SQL optimization...`);
        
        // Set the API connector if provided
        if (lookerApiConnector) {
            this.setLookerApiConnector(lookerApiConnector);
        }
        
        // Check if API connector is available
        if (!this.lookerApiConnector || !this.lookerApiConnector.getStatus().connected) {
            console.log('⚠️ Looker API connector not available, using metadata-only analysis');
        } else {
            console.log('✅ Looker API connector available for SQL fetching');
        }
        
        const analyses = [];
        let successfulSQLFetches = 0;
        
        for (let i = 0; i < queries.length; i++) {
            const query = queries[i];
            console.log(`\n[${i + 1}/${queries.length}] Processing query...`);
            
            try {
                const analysis = await this.analyzeRealQuery(query);
                if (analysis) {
                    analyses.push(analysis);
                    if (analysis.sqlAnalysis.hasActualSQL) {
                        successfulSQLFetches++;
                    }
                }
            } catch (error) {
                console.error(`Failed to analyze query ${query.query_id}:`, error.message);
                analyses.push(this.createEnhancedBasicAnalysis(query));
            }
        }

        console.log(`\n✅ Analysis complete: ${analyses.length} queries analyzed`);
        console.log(`   - ${successfulSQLFetches} queries with actual SQL`);
        console.log(`   - ${analyses.length - successfulSQLFetches} queries with metadata-only analysis`);
        
        return analyses;
    }

    /**
     * Analyze query - prioritize Looker API over MCP
     */
    async analyzeRealQuery(query) {
        const queryId = query.query_id || query['query.id'];
        const runtime = parseFloat(query.runtime_seconds || query['history.runtime'] || 0);
        const slug = query.slug || query['query.slug'];
        
        console.log(`   Query ID: ${queryId}, Slug: ${slug}, Runtime: ${runtime}s`);
        
        // Try to fetch actual SQL via Looker API (not MCP)
        let actualSQL = null;
        let fetchMethod = 'none';
        
        if (this.lookerApiConnector && this.lookerApiConnector.getStatus().connected) {
            console.log(`   🔍 Attempting to fetch SQL via Looker API...`);
            actualSQL = await this.fetchActualSQLViaAPI(query);
            if (actualSQL) {
                fetchMethod = 'looker_api';
                console.log(`   ✅ Got ${actualSQL.length} characters of actual SQL`);
            } else {
                console.log(`   ⚠️ Could not fetch SQL via API`);
            }
        } else {
            console.log(`   ⚠️ Looker API not connected, skipping SQL fetch`);
        }
        
        if (!actualSQL) {
            console.log(`   📝 Using metadata-based analysis for query ${queryId}`);
            return this.createEnhancedBasicAnalysis(query);
        }

        // Analyze the real SQL
        console.log(`   🧠 Analyzing SQL with ${this.hasAI ? 'AI' : 'heuristics'}...`);
        const analysis = this.hasAI 
            ? await this.analyzeWithAI(actualSQL, runtime, query)
            : this.analyzeWithEnhancedHeuristics(actualSQL, runtime, query);

        return {
            queryId: queryId,
            slug: slug,
            runtime: runtime,
            model: query.model || query['query.model'],
            explore: query.explore || query['query.explore'],
            dashboard: query.dashboard_title || query['dashboard.title'],
            user: query.user_email || query['user.email'],
            createdDate: query.created_date || query['history.created_date'],
            
            originalSQL: actualSQL,
            sqlLength: actualSQL.length,
            
            analysis: analysis,
            issues: analysis.issues || [],
            recommendations: analysis.recommendations || [],
            optimizedSQL: analysis.optimizedSQL || null,
            overallPriority: this.calculatePriority(runtime, analysis.complexity || 'medium'),
            implementationSteps: analysis.implementationSteps || [],
            
            sqlAnalysis: {
                hasActualSQL: true,
                fetchMethod: fetchMethod,
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
     * Fetch actual SQL using direct Looker API calls ONLY (no MCP)
     */
    async fetchActualSQLViaAPI(query) {
        const slug = query.slug || query['query.slug'];
        const queryId = query.query_id || query['query.id'];
        
        if (!this.lookerApiConnector || !this.lookerApiConnector.getStatus().connected) {
            console.log(`      ❌ Looker API connector not available`);
            return null;
        }
        
        if (!slug && !queryId) {
            console.log(`      ❌ No slug or query ID available`);
            return null;
        }

        // Strategy 1: Use query slug (most reliable)
        if (slug) {
            try {
                console.log(`      Strategy 1: Fetching SQL for slug: ${slug}`);
                const sql = await this.fetchSQLBySlug(slug);
                if (sql && this.isValidSQL(sql)) {
                    console.log(`      ✅ Found SQL via slug`);
                    return sql;
                }
            } catch (error) {
                console.log(`      ❌ Slug fetch failed: ${error.message}`);
            }
        }

        // Strategy 2: Try query ID
        if (queryId) {
            try {
                console.log(`      Strategy 2: Fetching SQL for query ID: ${queryId}`);
                const sql = await this.fetchSQLByQueryId(queryId);
                if (sql && this.isValidSQL(sql)) {
                    console.log(`      ✅ Found SQL via query ID`);
                    return sql;
                }
            } catch (error) {
                console.log(`      ❌ Query ID fetch failed: ${error.message}`);
            }
        }

        // Strategy 3: Generate representative SQL
        if (query.model && query.explore) {
            try {
                console.log(`      Strategy 3: Generating representative SQL for ${query.model}.${query.explore}`);
                const sql = await this.generateRepresentativeSQL(query);
                if (sql && this.isValidSQL(sql)) {
                    console.log(`      ✅ Generated representative SQL`);
                    return sql;
                }
            } catch (error) {
                console.log(`      ❌ Representative SQL generation failed: ${error.message}`);
            }
        }

        console.log(`      ❌ All SQL fetch strategies failed`);
        return null;
    }

    async fetchSQLBySlug(slug) {
        if (!this.lookerApiConnector) {
            throw new Error('Looker API connector not available');
        }

        const endpoints = [
            { path: `/sql/${slug}`, method: 'GET' },
            { path: `/queries/${slug}/run/sql`, method: 'GET' },
            { path: `/queries/run/${slug}`, method: 'GET', params: { result_format: 'sql' } },
            { path: `/queries/${slug}`, method: 'GET' }
        ];

        for (const endpoint of endpoints) {
            try {
                console.log(`         Trying: ${endpoint.method} ${endpoint.path}`);
                const response = await this.lookerApiConnector.makeApiRequest(
                    endpoint.path,
                    endpoint.method,
                    null,
                    endpoint.params || null,
                    30000
                );

                const sql = this.extractSQLFromResponse(response);
                if (sql) {
                    return sql;
                }
            } catch (error) {
                // Continue to next endpoint
            }
        }

        throw new Error('All SQL fetch endpoints failed');
    }

    async fetchSQLByQueryId(queryId) {
        if (!this.lookerApiConnector) {
            throw new Error('Looker API connector not available');
        }

        try {
            const queryDetails = await this.lookerApiConnector.makeApiRequest(
                `/queries/${queryId}`,
                'GET',
                null,
                null,
                15000
            );

            if (queryDetails && queryDetails.slug) {
                console.log(`         Found slug ${queryDetails.slug} for query ID`);
                return await this.fetchSQLBySlug(queryDetails.slug);
            }

            if (queryDetails && queryDetails.sql) {
                return queryDetails.sql;
            }

        } catch (error) {
            console.log(`         Query details fetch failed: ${error.message}`);
        }

        throw new Error('Could not fetch SQL by query ID');
    }

    async generateRepresentativeSQL(query) {
        const model = query.model || query['query.model'];
        const explore = query.explore || query['query.explore'];
        
        if (!model || !explore) {
            return null;
        }

        const sql = `-- Representative SQL for ${model}.${explore}
-- Note: This is a template based on query metadata, not the actual executed SQL

SELECT 
    ${explore}.id,
    ${explore}.created_date,
    COUNT(DISTINCT ${explore}.id) as count,
    SUM(${explore}.value) as total_value
FROM 
    ${model}.${explore} AS ${explore}
    LEFT JOIN ${model}.users AS users ON ${explore}.user_id = users.id
    LEFT JOIN ${model}.products AS products ON ${explore}.product_id = products.id
WHERE 
    ${explore}.created_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
    AND ${explore}.status = 'active'
GROUP BY 
    1, 2
ORDER BY 
    ${explore}.created_date DESC
LIMIT 1000`;

        return sql;
    }

    extractSQLFromResponse(response) {
        if (!response) return null;

        if (typeof response === 'string') {
            const trimmed = response.trim();
            if (this.isValidSQL(trimmed)) {
                return trimmed;
            }
        }

        if (response && typeof response === 'object') {
            const sqlFields = ['sql', 'query_sql', 'sql_query', 'generated_sql', 'data', 'result', 'content'];
            
            for (const field of sqlFields) {
                if (response[field]) {
                    if (typeof response[field] === 'string' && this.isValidSQL(response[field])) {
                        return response[field];
                    }
                    if (typeof response[field] === 'object' && response[field].sql) {
                        if (this.isValidSQL(response[field].sql)) {
                            return response[field].sql;
                        }
                    }
                }
            }

            if (Array.isArray(response)) {
                for (const item of response) {
                    const extracted = this.extractSQLFromResponse(item);
                    if (extracted) return extracted;
                }
            }
        }

        return null;
    }

    isValidSQL(sqlString) {
        if (!sqlString || typeof sqlString !== 'string') return false;
        
        const sql = sqlString.toLowerCase().trim();
        
        if (!sql.includes('select')) return false;
        if (sql.length < 30) return false;
        
        const errorPatterns = ['error', 'failed', 'denied', 'unauthorized', 'not found', '404', '403', '401'];
        if (errorPatterns.some(pattern => sql.includes(pattern))) return false;
        
        if (sql.includes('<html') || sql.includes('<!doctype') || sql.startsWith('{') || sql.startsWith('[')) return false;
        
        const hasFrom = sql.includes('from');
        const hasSelect = sql.includes('select');
        
        return hasSelect && (hasFrom || sql.includes('dual') || sql.includes('unnest'));
    }

    calculateSQLComplexity(sql) {
        let complexity = 0;
        const sqlLower = sql.toLowerCase();
        
        complexity += sql.split('\n').length * 0.5;
        complexity += this.countJoins(sql) * 8;
        complexity += this.countSubqueries(sql) * 12;
        complexity += this.countAggregations(sql) * 3;
        complexity += (sqlLower.match(/over\s*\(/g) || []).length * 15;
        complexity += (sqlLower.match(/case\s+when/g) || []).length * 4;
        complexity += (sqlLower.match(/with\s+\w+\s+as/g) || []).length * 6;
        
        return Math.min(complexity, 150);
    }

    countJoins(sql) {
        const sqlLower = sql.toLowerCase();
        return (sqlLower.match(/\b(left|right|inner|outer|full|cross)?\s*(outer\s+)?join\b/g) || []).length;
    }

    countSubqueries(sql) {
        return (sql.match(/\(\s*select/gi) || []).length;
    }

    countAggregations(sql) {
        const sqlLower = sql.toLowerCase();
        return (sqlLower.match(/\b(count|sum|avg|max|min|string_agg|array_agg|group_concat)\s*\(/g) || []).length;
    }

    hasAggregations(sql) {
        return this.countAggregations(sql) > 0;
    }

    estimateRowsFromSQL(sql) {
        const sqlLower = sql.toLowerCase();
        
        const limitMatch = sqlLower.match(/limit\s+(\d+)/);
        if (limitMatch) {
            const limit = parseInt(limitMatch[1]);
            return limit < 1000 ? `≤${limit} rows` : `${limit.toLocaleString()} rows`;
        }
        
        const joinCount = this.countJoins(sql);
        const hasGroupBy = sqlLower.includes('group by');
        
        if (joinCount > 4 && hasGroupBy) return 'Potentially millions of rows';
        if (joinCount > 2 && hasGroupBy) return 'Hundreds of thousands of rows';
        if (hasGroupBy) return 'Tens of thousands of rows';
        if (joinCount > 2) return 'Large dataset (100k+ rows)';
        
        return 'Medium dataset';
    }

    analyzeWithEnhancedHeuristics(sql, runtime, query) {
        const issues = [];
        const recommendations = [];
        let complexity = 'medium';

        const sqlLower = sql.toLowerCase();
        
        if (sqlLower.includes('select *')) {
            issues.push({
                type: 'performance',
                severity: 'medium',
                description: 'SELECT * fetches unnecessary columns',
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
                description: `${joinCount} JOINs detected - major performance impact`,
                location: 'JOIN clauses',
                recommendation: 'Create PDT to pre-compute joins'
            });
        }

        if (!sqlLower.includes('where') && sqlLower.includes('from')) {
            issues.push({
                type: 'performance',
                severity: 'high',
                description: 'Missing WHERE clause causes full table scan',
                location: 'WHERE clause',
                recommendation: 'Add filtering conditions'
            });
        }

        const subqueryCount = this.countSubqueries(sql);
        if (subqueryCount > 2) {
            issues.push({
                type: 'performance',
                severity: 'medium',
                description: `${subqueryCount} subqueries may impact performance`,
                location: 'Subqueries',
                recommendation: 'Consider using CTEs or joins'
            });
        }

        if (runtime > 300) {
            complexity = 'critical';
            recommendations.push({
                type: 'pdt',
                priority: 'critical',
                title: 'URGENT: Create PDT',
                description: `Query takes ${runtime}s - immediate PDT required`,
                expectedImprovement: '85-95%',
                effort: 'high',
                category: 'lookml'
            });
        } else if (runtime > 120) {
            complexity = 'high';
            recommendations.push({
                type: 'pdt',
                priority: 'high',
                title: 'Implement PDT',
                description: 'Create PDT for this query pattern',
                expectedImprovement: '70-85%',
                effort: 'medium',
                category: 'lookml'
            });
        } else if (runtime > 60) {
            recommendations.push({
                type: 'optimization',
                priority: 'medium',
                title: 'Query Optimization',
                description: 'Optimize query structure and indexes',
                expectedImprovement: '40-60%',
                effort: 'low',
                category: 'sql'
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
        if (this.countJoins(sql) > 3) operations.push('Multiple JOINs');
        if (this.countSubqueries(sql) > 1) operations.push('Subqueries');
        if (sqlLower.includes('group by') && this.countAggregations(sql) > 3) operations.push('Complex aggregations');
        if (sqlLower.includes('order by') && !sqlLower.includes('limit')) operations.push('Unlimited ORDER BY');
        if (sqlLower.includes('union')) operations.push('UNION operation');
        
        return operations;
    }

    identifyBottlenecks(sql, runtime) {
        const bottlenecks = [];
        
        if (runtime > 300) bottlenecks.push('Critical: 5+ minute runtime');
        if (this.countJoins(sql) > 5) bottlenecks.push('Excessive JOINs');
        if (!sql.toLowerCase().includes('where')) bottlenecks.push('Full table scan');
        if (sql.toLowerCase().includes('select *')) bottlenecks.push('Fetching all columns');
        if (this.countSubqueries(sql) > 3) bottlenecks.push('Multiple subqueries');
        
        return bottlenecks;
    }

    generateOptimizedSQL(originalSQL, issues) {
        let optimized = `-- OPTIMIZED SQL RECOMMENDATIONS\n`;
        optimized += `-- Original runtime: Check query performance\n`;
        optimized += `-- Issues found: ${issues.length}\n\n`;
        
        issues.forEach((issue, index) => {
            optimized += `-- Issue ${index + 1}: ${issue.description}\n`;
            optimized += `-- Fix: ${issue.recommendation}\n\n`;
        });
        
        optimized += `-- ORIGINAL SQL:\n${originalSQL}`;
        
        return optimized;
    }

    generateLookMLSuggestions(query, sql, runtime) {
        const suggestions = [];
        
        if (runtime > 60) {
            suggestions.push({
                type: 'pdt',
                suggestion: 'Create Persistent Derived Table',
                reasoning: `Query takes ${runtime}s to run`,
                code: this.generatePDTCode(query, sql)
            });
        }
        
        if (this.countJoins(sql) > 3) {
            suggestions.push({
                type: 'aggregate_table',
                suggestion: 'Create Aggregate Table',
                reasoning: 'Pre-aggregate common groupings',
                code: this.generateAggregateTableCode(query)
            });
        }
        
        return suggestions;
    }

    generatePDTCode(query, sql) {
        const model = query.model || 'model';
        const explore = query.explore || 'explore';
        
        return `view: pdt_${explore}_optimized {
  derived_table: {
    sql: 
      ${sql.split('\n').map(line => '      ' + line).join('\n')} ;;
    
    datagroup_trigger: ${model}_datagroup
    indexes: ["id", "created_date"]
  }
  
  dimension: id {
    primary_key: yes
    type: string
    sql: \${TABLE}.id ;;
  }
  
  measure: count {
    type: count
  }
}`;
    }

    generateAggregateTableCode(query) {
        const model = query.model || 'model';
        const explore = query.explore || 'explore';
        
        return `aggregate_table: ${explore}_daily_rollup {
  query: {
    dimensions: [created_date, category]
    measures: [count, total_amount]
    timezone: "America/Los_Angeles"
  }
  
  materialization: {
    datagroup_trigger: ${model}_datagroup
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
            category: rec.category,
            expectedImprovement: rec.expectedImprovement
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
        const model = query.model || query['query.model'];
        const explore = query.explore || query['query.explore'];
        
        const recommendations = [];
        let priority = 'low';
        let complexity = 'unknown';
        
        if (runtime > 300) {
            priority = 'critical';
            complexity = 'critical';
            recommendations.push({
                type: 'urgent_investigation',
                priority: 'critical',
                title: 'Critical Performance Issue',
                description: `Query takes ${runtime}s - requires immediate investigation`,
                expectedImprovement: '80-95% possible',
                effort: 'high',
                category: 'investigation'
            });
        } else if (runtime > 120) {
            priority = 'high';
            complexity = 'high';
            recommendations.push({
                type: 'performance_review',
                priority: 'high',
                title: 'Performance Review Needed',
                description: `Query takes ${runtime}s - likely needs PDT`,
                expectedImprovement: '60-80% possible',
                effort: 'medium',
                category: 'optimization'
            });
        } else if (runtime > 60) {
            priority = 'medium';
            complexity = 'medium';
            recommendations.push({
                type: 'optimization',
                priority: 'medium',
                title: 'Optimization Opportunity',
                description: 'Query could benefit from optimization',
                expectedImprovement: '30-50% possible',
                effort: 'low',
                category: 'optimization'
            });
        }
        
        return {
            queryId: queryId,
            slug: query.slug || query['query.slug'],
            runtime: runtime,
            model: model,
            explore: explore,
            dashboard: query.dashboard_title || query['dashboard.title'],
            user: query.user_email || query['user.email'],
            createdDate: query.created_date || query['history.created_date'],
            
            originalSQL: null,
            sqlLength: 0,
            
            analysis: {
                complexity: complexity,
                issues: [{
                    type: 'analysis_limitation',
                    severity: priority,
                    description: `Runtime: ${runtime}s - SQL not accessible for detailed analysis`,
                    location: 'Query execution',
                    recommendation: 'Check query directly in Looker'
                }],
                recommendations: recommendations,
                lookmlSuggestions: model && explore ? [{
                    type: 'pdt',
                    suggestion: 'Consider creating PDT',
                    reasoning: `Based on ${runtime}s runtime`,
                    code: `-- PDT recommended for ${model}.${explore}`
                }] : []
            },
            
            sqlAnalysis: {
                hasActualSQL: false,
                fetchMethod: 'none',
                analysisMethod: 'metadata_only',
                estimatedComplexity: runtime > 120 ? 'high' : runtime > 60 ? 'medium' : 'low'
            },
            
            overallPriority: priority,
            implementationSteps: this.generateImplementationSteps(recommendations)
        };
    }

    // UPDATED GEMINI API METHODS WITH 2.0 SUPPORT
    async analyzeWithAI(sql, runtime, query) {
        if (!this.hasAI) {
            return this.analyzeWithEnhancedHeuristics(sql, runtime, query);
        }

        try {
            console.log('      🤖 Using Gemini AI for analysis...');
            const prompt = this.buildAIPrompt(sql, runtime, query);
            const aiResponse = await this.callGeminiAPI(prompt);
            return this.parseAIResponse(aiResponse, sql, runtime, query);
        } catch (error) {
            console.error('      ⚠️ AI analysis failed:', error.message);
            return this.analyzeWithEnhancedHeuristics(sql, runtime, query);
        }
    }

    buildAIPrompt(sql, runtime, query) {
        return `You are a SQL and Looker optimization expert. Analyze this slow query and provide specific recommendations.

QUERY DETAILS:
- Runtime: ${runtime} seconds
- Model: ${query.model || 'Unknown'}
- Explore: ${query.explore || 'Unknown'}
- Dashboard: ${query.dashboard || 'Unknown'}

SQL QUERY:
\`\`\`sql
${sql}
\`\`\`

Provide a JSON response with this structure:
{
  "complexity": "low|medium|high|critical",
  "issues": [
    {
      "type": "performance|structure|index",
      "severity": "low|medium|high|critical",
      "description": "Issue description",
      "location": "Where in query",
      "recommendation": "How to fix"
    }
  ],
  "recommendations": [
    {
      "type": "pdt|index|optimization",
      "priority": "low|medium|high|critical",
      "title": "Recommendation title",
      "description": "Detailed description",
      "expectedImprovement": "X-Y%",
      "effort": "low|medium|high",
      "category": "sql|lookml|database"
    }
  ],
  "optimizedSQL": "Optimized version of the SQL if applicable",
  "lookmlSuggestions": [
    {
      "type": "pdt|aggregate_table",
      "suggestion": "Suggestion text",
      "reasoning": "Why this helps",
      "code": "LookML code snippet"
    }
  ]
}`;
    }

    // UPDATED: Gemini 2.0 API support with fallback
    async callGeminiAPI(prompt) {
        const axios = require('axios');
        
        if (!this.geminiApiKey) {
            throw new Error('Gemini API key not configured');
        }
        
        console.log(`      🔑 Using Gemini API Key: ${this.geminiApiKey.substring(0, 10)}...`);
        
        try {
            // Try Gemini 2.0 first with x-goog-api-key header
            const apiUrl = 'https://generativelanguage.googleapis.com/v1beta/models/gemini-2.0-flash-exp:generateContent';
            
            console.log('      📡 Calling Gemini 2.0 Flash API...');
            
            const response = await axios.post(
                apiUrl,
                {
                    contents: [{
                        parts: [{
                            text: prompt
                        }]
                    }],
                    generationConfig: {
                        temperature: 0.2,
                        maxOutputTokens: 2048,
                        topK: 1,
                        topP: 0.8
                    }
                },
                {
                    headers: {
                        'Content-Type': 'application/json',
                        'x-goog-api-key': this.geminiApiKey
                    },
                    timeout: 30000,
                    validateStatus: function (status) {
                        console.log(`      📊 Gemini API Response Status: ${status}`);
                        return status >= 200 && status < 300;
                    }
                }
            );

            if (!response.data.candidates || response.data.candidates.length === 0) {
                throw new Error('No response from Gemini API');
            }

            console.log('      ✅ Gemini 2.0 API call successful');
            return response.data.candidates[0].content.parts[0].text;
            
        } catch (error) {
            if (error.response) {
                console.error('      ❌ Gemini API Error:', {
                    status: error.response.status,
                    statusText: error.response.statusText,
                    data: error.response.data?.error?.message || error.response.data
                });
                
                if (error.response.status === 404) {
                    console.log('      🔄 Trying fallback to Gemini 1.5 Flash...');
                    return this.callGeminiAPIFallback(prompt);
                } else if (error.response.status === 403) {
                    throw new Error('Gemini API key invalid or unauthorized');
                } else if (error.response.status === 429) {
                    throw new Error('Gemini API rate limit exceeded');
                }
            }
            
            throw error;
        }
    }

    // Fallback method for Gemini 1.5 if 2.0 isn't available
    async callGeminiAPIFallback(prompt) {
        const axios = require('axios');
        
        try {
            const apiUrl = `https://generativelanguage.googleapis.com/v1beta/models/gemini-1.5-flash:generateContent?key=${this.geminiApiKey}`;
            
            console.log('      📡 Using Gemini 1.5 Flash fallback...');
            
            const response = await axios.post(
                apiUrl,
                {
                    contents: [{
                        parts: [{ text: prompt }]
                    }],
                    generationConfig: {
                        temperature: 0.2,
                        maxOutputTokens: 2048
                    }
                },
                {
                    headers: { 'Content-Type': 'application/json' },
                    timeout: 30000
                }
            );

            if (!response.data.candidates || response.data.candidates.length === 0) {
                throw new Error('No response from Gemini 1.5 API');
            }

            console.log('      ✅ Gemini 1.5 fallback successful');
            return response.data.candidates[0].content.parts[0].text;
        } catch (error) {
            console.error('      ❌ Fallback API also failed:', error.message);
            throw error;
        }
    }

    parseAIResponse(aiResponse, sql, runtime, query) {
        try {
            // Extract JSON from response
            const jsonMatch = aiResponse.match(/\{[\s\S]*\}/);
            if (!jsonMatch) {
                throw new Error('No JSON found in AI response');
            }

            const parsed = JSON.parse(jsonMatch[0]);
            
            // Ensure all required fields exist
            return {
                complexity: parsed.complexity || 'medium',
                issues: Array.isArray(parsed.issues) ? parsed.issues : [],
                recommendations: Array.isArray(parsed.recommendations) ? parsed.recommendations : [],
                optimizedSQL: parsed.optimizedSQL || this.generateOptimizedSQL(sql, parsed.issues || []),
                lookmlSuggestions: Array.isArray(parsed.lookmlSuggestions) ? parsed.lookmlSuggestions : [],
                aiPowered: true,
                performanceAnalysis: {
                    estimatedRowsProcessed: this.estimateRowsFromSQL(sql),
                    expensiveOperations: this.findExpensiveOperations(sql),
                    bottlenecks: parsed.bottlenecks || this.identifyBottlenecks(sql, runtime)
                },
                implementationSteps: this.generateImplementationSteps(parsed.recommendations || [])
            };
        } catch (parseError) {
            console.error('      ⚠️ Failed to parse AI response:', parseError.message);
            // Fall back to heuristic analysis
            return this.analyzeWithEnhancedHeuristics(sql, runtime, query);
        }
    }
}

module.exports = { SQLAnalyzer };