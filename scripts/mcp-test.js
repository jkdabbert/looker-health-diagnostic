#!/usr/bin/env node

/**
 * Test script for AI-powered query analysis functionality
 * Tests both Gemini API integration and local fallback analysis
 */

require('dotenv').config();
const { LookerHealthDiagnostic } = require('../src/diagnostic-engine');

// Sample slow queries for testing
const testQueries = [
    {
        query_id: 'test_query_1',
        runtime: 8.5,
        sql_query_text: `
            SELECT *
            FROM large_sales_table lst
            JOIN customer_table ct ON lst.customer_id = ct.id
            JOIN product_table pt ON lst.product_id = pt.id
            WHERE lst.date_created > '2023-01-01'
            ORDER BY lst.date_created DESC, ct.name ASC
        `
    },
    {
        query_id: 'test_query_2',
        runtime: 15.2,
        sql_query_text: `
            SELECT *
            FROM (
                SELECT customer_id, SUM(revenue) as total_revenue
                FROM sales_fact_table
                WHERE date_key >= 20230101
                GROUP BY customer_id
            ) revenue_summary
            JOIN customer_dimension ON revenue_summary.customer_id = customer_dimension.customer_id
            ORDER BY total_revenue DESC
        `
    },
    {
        query_id: 'test_query_3',
        runtime: 3.2,
        sql_query_text: `
            SELECT product_name, COUNT(*) as order_count
            FROM order_items oi
            JOIN products p ON oi.product_id = p.id
            GROUP BY product_name
            HAVING COUNT(*) > 100
        `
    }
];

async function testAIAnalysis() {
    console.log('🤖 Testing AI-Powered Query Analysis');
    console.log('====================================\n');

    // Test configuration
    console.log('📋 Configuration Check:');
    console.log(`   Gemini API Key: ${process.env.GEMINI_API_KEY ? '✅ Configured' : '❌ Missing'}`);
    console.log(`   Environment: ${process.env.NODE_ENV || 'development'}`);
    console.log('');

    const config = {
        lookerUrl: process.env.LOOKER_BASE_URL || 'demo',
        clientId: process.env.LOOKER_CLIENT_ID || 'demo',
        clientSecret: process.env.LOOKER_CLIENT_SECRET || 'demo'
    };

    const diagnostic = new LookerHealthDiagnostic(config);

    console.log('🔍 Testing Query Analysis...\n');

    for (let i = 0; i < testQueries.length; i++) {
        const query = testQueries[i];
        console.log(`📊 Analyzing Query ${i + 1}: ${query.query_id}`);
        console.log(`   Runtime: ${query.runtime}s`);
        console.log(`   SQL Preview: ${query.sql_query_text.substring(0, 100).replace(/\s+/g, ' ')}...`);

        try {
            const startTime = Date.now();
            const analysis = await diagnostic.analyzeQueryWithAI(query);
            const analysisTime = Date.now() - startTime;

            console.log(`   ✅ Analysis completed in ${analysisTime}ms`);
            console.log(`   🎯 Issues found: ${analysis.issues.length}`);
            console.log(`   💡 Recommendations: ${analysis.recommendations.length}`);
            console.log(`   🔧 LookML suggestions: ${analysis.lookmlSuggestions.length}`);

            if (analysis.issues.length > 0) {
                console.log(`   🚨 Top issue: ${analysis.issues[0].description}`);
            }

            if (analysis.recommendations.length > 0) {
                console.log(`   💭 Top recommendation: ${analysis.recommendations[0].action}`);
            }

            if (analysis.lookmlSuggestions.length > 0) {
                console.log(`   🏗️  LookML suggestion: ${analysis.lookmlSuggestions[0].suggestion}`);
            }

        } catch (error) {
            console.log(`   ❌ Analysis failed: ${error.message}`);
        }

        console.log('');
    }
}

async function testSlowQueryAnalysis() {
    console.log('🐌 Testing Bulk Slow Query Analysis');
    console.log('===================================\n');

    const config = {
        lookerUrl: process.env.LOOKER_BASE_URL || 'demo',
        clientId: process.env.LOOKER_CLIENT_ID || 'demo',
        clientSecret: process.env.LOOKER_CLIENT_SECRET || 'demo'
    };

    const diagnostic = new LookerHealthDiagnostic(config);

    try {
        const startTime = Date.now();
        const bulkAnalysis = await diagnostic.analyzeSlowQueriesWithAI([], testQueries);
        const totalTime = Date.now() - startTime;

        console.log(`✅ Bulk analysis completed in ${totalTime}ms`);
        console.log(`📊 Queries analyzed: ${bulkAnalysis.length}`);
        console.log(`🤖 AI-powered: ${bulkAnalysis.some(a => a.queryId)}`);

        bulkAnalysis.forEach((analysis, index) => {
            console.log(`\n🔍 Query ${index + 1} (${analysis.queryId}):`);
            console.log(`   Runtime: ${analysis.runtime}s`);
            console.log(`   Issues: ${analysis.issues.length}`);
            console.log(`   Recommendations: ${analysis.recommendations.length}`);
            console.log(`   LookML Suggestions: ${analysis.lookmlSuggestions.length}`);
        });

    } catch (error) {
        console.log(`❌ Bulk analysis failed: ${error.message}`);
    }
}

async function testAIRecommendations() {
    console.log('\n🎯 Testing AI Recommendation Generation');
    console.log('=====================================\n');

    const config = {
        lookerUrl: process.env.LOOKER_BASE_URL || 'demo',
        clientId: process.env.LOOKER_CLIENT_ID || 'demo',
        clientSecret: process.env.LOOKER_CLIENT_SECRET || 'demo'
    };

    const diagnostic = new LookerHealthDiagnostic(config);

    // Create sample issues for testing
    const sampleIssues = [
        {
            type: 'performance',
            severity: 'high',
            dashboard: 'Sales Dashboard',
            issue: 'Slow loading time: 8.5s',
            recommendation: 'Optimize queries and add indexes',
            aiPowered: true
        },
        {
            type: 'governance',
            severity: 'medium',
            dashboard: 'Marketing Analytics',
            issue: 'Missing documentation',
            recommendation: 'Add descriptions for key metrics'
        }
    ];

    const sampleQueryAnalysis = testQueries.map(q => ({
        queryId: q.query_id,
        runtime: q.runtime,
        issues: [{ type: 'performance', description: 'Inefficient query structure' }],
        recommendations: [{ action: 'Add proper indexing', expectedImprovement: '50%' }],
        lookmlSuggestions: [{ suggestion: 'Use aggregate tables' }]
    }));

    try {
        const recommendations = await diagnostic.generateAIRecommendations(sampleIssues, sampleQueryAnalysis);

        console.log(`✅ AI recommendations generated`);
        console.log(`🎯 Priorities: ${recommendations.priorities.length}`);
        console.log(`📋 Strategic recommendations: ${recommendations.strategicRecommendations.length}`);
        console.log(`🤖 AI-powered: ${recommendations.aiPowered}`);
        console.log(`🔍 Query analysis included: ${recommendations.queryAnalysisIncluded}`);

        if (recommendations.priorities.length > 0) {
            console.log(`\n🏆 Top Priority: ${recommendations.priorities[0].title}`);
            console.log(`   Impact: ${recommendations.priorities[0].estimatedImpact}`);
            console.log(`   Timeline: ${recommendations.priorities[0].timeframe}`);
        }

        if (recommendations.queryOptimizationPlan) {
            console.log(`\n🚀 Query Optimization Plan:`);
            console.log(`   Immediate actions: ${recommendations.queryOptimizationPlan.immediateActions.length}`);
            console.log(`   Medium-term goals: ${recommendations.queryOptimizationPlan.mediumTermGoals.length}`);
        }

    } catch (error) {
        console.log(`❌ AI recommendation generation failed: ${error.message}`);
    }
}

async function runAllTests() {
    console.log('🧪 Looker Health Diagnostic - AI Analysis Test Suite');
    console.log('===================================================\n');

    try {
        await testAIAnalysis();
        await testSlowQueryAnalysis();
        await testAIRecommendations();

        console.log('\n🎉 All AI tests completed!');
        console.log('\n💡 Next Steps:');
        console.log('   • Add your Gemini API key to .env for full AI features');
        console.log('   • Run the full diagnostic: npm run dev then POST /api/diagnostic/run');
        console.log('   • Test individual query analysis: POST /api/analyze-query');

    } catch (error) {
        console.error('\n❌ Test suite failed:', error.message);
        process.exit(1);
    }
}

// Run tests if called directly
if (require.main === module) {
    runAllTests().catch(console.error);
}

module.exports = {
    testAIAnalysis,
    testSlowQueryAnalysis,
    testAIRecommendations,
    runAllTests
};