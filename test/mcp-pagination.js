#!/usr/bin/env node

const { LookerHealthDiagnostic } = require('../src/diagnostic-engine');
require('dotenv').config();

async function testPagination() {
    console.log('📄 Testing MCP Pagination...');
    console.log('============================\n');
    
    const config = {
        lookerUrl: process.env.LOOKER_BASE_URL || 'demo',
        clientId: process.env.LOOKER_CLIENT_ID || 'demo',
        clientSecret: process.env.LOOKER_CLIENT_SECRET || 'demo'
    };

    const diagnostic = new LookerHealthDiagnostic(config);

    try {
        console.log('🔧 Configuration:');
        console.log(`   Use Mock Data: ${process.env.USE_MOCK_DATA || 'false'}`);
        console.log(`   Looker URL: ${config.lookerUrl}`);
        console.log(`   Batch Size: ${process.env.PAGINATION_BATCH_SIZE || '25'}`);
        console.log('');

        console.log('🔌 Initializing MCP connection...');
        const mcpConnected = await diagnostic.initializeMCP();
        console.log(`   MCP Status: ${mcpConnected ? 'Connected' : 'Failed'}`);
        console.log('');

        console.log('📊 Testing paginated dashboard fetching...');
        const startTime = Date.now();
        
        const data = await diagnostic.fetchAllDashboardsWithPagination();
        
        const totalTime = Date.now() - startTime;
        
        console.log('✅ Pagination test completed successfully!\n');
        
        console.log('📈 Results Summary:');
        console.log(`   Total Time: ${totalTime}ms`);
        console.log(`   Dashboards: ${data.dashboards.length}`);
        console.log(`   Looks: ${data.looks?.length || 0}`);
        console.log(`   Query Metrics: ${data.queryMetrics?.length || 0}`);
        console.log(`   Connection Type: ${data.systemMetrics.connectionType}`);
        
        if (data.systemMetrics.mcpMetrics) {
            console.log('\n🔌 MCP Performance:');
            console.log(`   Total MCP Calls: ${data.systemMetrics.mcpMetrics.callsPerformed}`);
            console.log(`   Average Response Time: ${data.systemMetrics.mcpMetrics.avgResponseTime}ms`);
            console.log(`   Errors: ${data.systemMetrics.mcpMetrics.errors}`);
            console.log(`   Estimated Batches: ${Math.ceil(data.dashboards.length / 25)}`);
        }
        
        console.log('\n📋 Sample Dashboard Data:');
        if (data.dashboards.length > 0) {
            const sample = data.dashboards[0];
            console.log(`   ID: ${sample.id}`);
            console.log(`   Title: ${sample.title}`);
            console.log(`   Tiles: ${sample.tiles}`);
            console.log(`   Last Accessed: ${sample.lastAccessed.toDateString()}`);
            console.log(`   Query Count: ${sample.queryCount}`);
            console.log(`   Load Time: ${sample.avgLoadTime}s`);
        }
        
        if (data.looks && data.looks.length > 0) {
            console.log('\n👀 Sample Look Data:');
            const sampleLook = data.looks[0];
            console.log(`   ID: ${sampleLook.id}`);
            console.log(`   Title: ${sampleLook.title}`);
            console.log(`   User Count: ${sampleLook.userCount}`);
        }
        
        if (data.queryMetrics && data.queryMetrics.length > 0) {
            console.log('\n🐌 Sample Query Metrics:');
            const slowQuery = data.queryMetrics[0];
            console.log(`   Query ID: ${slowQuery.query_id}`);
            console.log(`   Runtime: ${slowQuery.runtime}s`);
            console.log(`   SQL Preview: ${slowQuery.sql_query_text?.substring(0, 60)}...`);
        }
        
        console.log('\n🎯 Test Validation:');
        console.log(`   ✅ Pagination: ${data.dashboards.length > 25 ? 'WORKING' : 'LIMITED'}`);
        console.log(`   ✅ MCP Integration: ${data.systemMetrics.mcpMetrics.callsPerformed > 0 ? 'WORKING' : 'FAILED'}`);
        console.log(`   ✅ Data Quality: ${data.dashboards.length > 0 ? 'GOOD' : 'POOR'}`);
        console.log(`   ✅ Performance: ${totalTime < 10000 ? 'GOOD' : 'SLOW'}`);
        
        return data;
        
    } catch (error) {
        console.error('❌ Pagination test failed:', error.message);
        console.error('\n🔍 Troubleshooting:');
        console.error('   • Check that MCP toolbox is installed: ls scripts/toolbox');
        console.error('   • Verify .env configuration');
        console.error('   • Try with USE_MOCK_DATA=true for testing');
        console.error('   • Check file permissions: chmod +x scripts/toolbox');
        
        throw error;
    }
}

async function testSpecificBatch() {
    console.log('\n🧪 Testing Single Batch Fetch...');
    console.log('=================================\n');
    
    const config = {
        lookerUrl: process.env.LOOKER_BASE_URL || 'demo',
        clientId: process.env.LOOKER_CLIENT_ID || 'demo',
        clientSecret: process.env.LOOKER_CLIENT_SECRET || 'demo'
    };

    const diagnostic = new LookerHealthDiagnostic(config);
    
    try {
        console.log('📥 Fetching single batch (offset: 0, limit: 5)...');
        
        // Only test if not using mock data
        if (process.env.USE_MOCK_DATA === 'true') {
            console.log('⚠️  Skipping batch test in mock data mode');
            return;
        }
        
        const batch = await diagnostic.fetchDashboardBatch(0, 5);
        
        console.log(`✅ Batch fetch successful: ${batch.length} dashboards`);
        
        if (batch.length > 0) {
            console.log('\n📊 First Dashboard in Batch:');
            const first = batch[0];
            console.log(`   Raw Data Keys: ${Object.keys(first).join(', ')}`);
            console.log(`   ID: ${first.id || 'N/A'}`);
            console.log(`   Title: ${first.title || first.name || 'N/A'}`);
        }
        
    } catch (error) {
        console.error('❌ Batch test failed:', error.message);
    }
}

// Run tests if called directly
if (require.main === module) {
    testPagination()
        .then(() => testSpecificBatch())
        .then(() => {
            console.log('\n🎉 All pagination tests completed!');
            console.log('\n💡 Next Steps:');
            console.log('   • Run npm run test-ai to test AI features');
            console.log('   • Run npm run dev to start the server');
            console.log('   • Test full diagnostic: POST /api/diagnostic/run');
        })
        .catch(error => {
            console.error('\n💥 Test suite failed:', error.message);
            process.exit(1);
        });
}

module.exports = { testPagination, testSpecificBatch };