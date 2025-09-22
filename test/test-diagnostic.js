const { LookerHealthDiagnostic } = require('../src/diagnostic-engine');

async function testDiagnostic() {
    console.log('🏥 Testing Looker Health Diagnostic...');
    
    const config = {
        lookerUrl: process.env.LOOKER_BASE_URL,
        clientId: process.env.LOOKER_CLIENT_ID,
        clientSecret: process.env.LOOKER_CLIENT_SECRET
    };

    try {
        const diagnostic = new LookerHealthDiagnostic(config);
        const results = await diagnostic.runDiagnostic();
        
        console.log('✅ Diagnostic completed successfully!');
        console.log('📊 Overall Grade:', results.overallGrade);
        console.log('🔍 Issues Found:', results.totalIssuesFound);
        
        return results;
    } catch (error) {
        console.error('❌ Diagnostic failed:', error.message);
        throw error;
    }
}

if (require.main === module) {
    require('dotenv').config();
    testDiagnostic();
}

module.exports = { testDiagnostic };
