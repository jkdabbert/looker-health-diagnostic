// scripts/test-gemini-2.js
// Test script for Gemini 2.0 Flash API

const axios = require('axios');
require('dotenv').config();

async function testGemini2API() {
    console.log('🧪 Testing Gemini 2.0 Flash API Configuration');
    console.log('==========================================\n');
    
    const apiKey = process.env.GEMINI_API_KEY;
    
    if (!apiKey) {
        console.log('❌ GEMINI_API_KEY not found in environment variables');
        console.log('\nTo fix:');
        console.log('1. Get an API key from: https://aistudio.google.com/app/apikey');
        console.log('2. Add to your .env file: GEMINI_API_KEY=your_key_here');
        return false;
    }
    

    
    // Test different API methods
    const methods = [
        {
            name: 'Method 1: API Key in URL (Original)',
            url: `https://generativelanguage.googleapis.com/v1beta/models/gemini-2.0-flash-exp:generateContent?key=${apiKey}`,
            headers: { 'Content-Type': 'application/json' }
        },
        {
            name: 'Method 2: X-Goog-Api-Key Header (Google AI Studio)',
            url: 'https://generativelanguage.googleapis.com/v1beta/models/gemini-2.0-flash-exp:generateContent',
            headers: {
                'Content-Type': 'application/json',
                'x-goog-api-key': apiKey
            }
        },
        {
            name: 'Method 3: Gemini 1.5 Flash (Fallback)',
            url: `https://generativelanguage.googleapis.com/v1beta/models/gemini-1.5-flash:generateContent?key=${apiKey}`,
            headers: { 'Content-Type': 'application/json' }
        }
    ];
    
    console.log('🔍 Testing different API methods:\n');
    
    for (const method of methods) {
        console.log(`Testing ${method.name}...`);
        
        try {
            const response = await axios.post(
                method.url,
                {
                    contents: [{
                        parts: [{
                            text: 'Say "Hello, I am working!" in exactly 5 words.'
                        }]
                    }],
                    generationConfig: {
                        temperature: 0.1,
                        maxOutputTokens: 100
                    }
                },
                {
                    headers: method.headers,
                    timeout: 10000
                }
            );
            
            if (response.status === 200) {
                const reply = response.data.candidates[0].content.parts[0].text;
                console.log(`   ✅ SUCCESS - Working!`);
                console.log(`      Response: ${reply.trim()}`);
                console.log(`      This method works!\n`);
                return method; // Return the working method
            }
            
        } catch (error) {
            if (error.response) {
                const status = error.response.status;
                const message = error.response.data?.error?.message || error.response.statusText;
                
                if (status === 404) {
                    console.log(`   ❌ NOT FOUND (404) - Model doesn't exist`);
                } else if (status === 403) {
                    console.log(`   ❌ FORBIDDEN (403) - API key issue`);
                } else if (status === 429) {
                    console.log(`   ⚠️  RATE LIMITED (429) - Too many requests`);
                } else if (status === 400) {
                    console.log(`   ❌ BAD REQUEST (400) - ${message}`);
                } else {
                    console.log(`   ❌ ERROR ${status}: ${message}`);
                }
            } else {
                console.log(`   ❌ NETWORK ERROR: ${error.message}`);
            }
            console.log('');
        }
    }
    
    return null;
}

async function testSQLOptimization() {
    console.log('\n🔧 Testing SQL Query Optimization');
    console.log('==================================\n');
    
    const apiKey = process.env.GEMINI_API_KEY;
    if (!apiKey) {
        console.log('❌ No API key to test');
        return;
    }
    
    const testSQL = `
SELECT 
    c.customer_id,
    c.customer_name,
    o.*,
    p.*
FROM customers c
LEFT JOIN orders o ON c.customer_id = o.customer_id
LEFT JOIN order_items oi ON o.order_id = oi.order_id
LEFT JOIN products p ON oi.product_id = p.product_id
LEFT JOIN categories cat ON p.category_id = cat.category_id
LEFT JOIN suppliers s ON p.supplier_id = s.supplier_id
WHERE o.created_date > '2024-01-01'
GROUP BY c.customer_id, c.customer_name, o.order_id
ORDER BY o.created_date DESC`;
    
    const prompt = `You are a SQL optimization expert. Analyze this query and provide specific optimization recommendations.

SQL Query (Runtime: 85 seconds):
${testSQL}

Respond with a JSON object containing:
{
  "complexity": "low|medium|high|critical",
  "issues": [
    {
      "type": "performance|structure|index",
      "severity": "low|medium|high|critical", 
      "description": "specific issue",
      "location": "where in query",
      "recommendation": "how to fix"
    }
  ],
  "recommendations": [
    {
      "type": "pdt|index|query_rewrite",
      "priority": "low|medium|high|critical",
      "title": "action title",
      "description": "detailed description",
      "expectedImprovement": "X-Y%",
      "effort": "low|medium|high"
    }
  ],
  "optimizedSQL": "rewritten SQL if applicable"
}`;
    
    try {
        // Try Gemini 2.0 first
        let url = 'https://generativelanguage.googleapis.com/v1beta/models/gemini-2.0-flash-exp:generateContent';
        let headers = {
            'Content-Type': 'application/json',
            'x-goog-api-key': apiKey
        };
        
        console.log('Sending SQL optimization request to Gemini 2.0...');
        
        let response;
        try {
            response = await axios.post(
                url,
                {
                    contents: [{
                        parts: [{ text: prompt }]
                    }],
                    generationConfig: {
                        temperature: 0.2,
                        maxOutputTokens: 2048
                    }
                },
                { headers, timeout: 15000 }
            );
        } catch (error) {
            if (error.response?.status === 404) {
                console.log('⚠️ Gemini 2.0 not available, trying 1.5...');
                url = `https://generativelanguage.googleapis.com/v1beta/models/gemini-1.5-flash:generateContent?key=${apiKey}`;
                headers = { 'Content-Type': 'application/json' };
                
                response = await axios.post(
                    url,
                    {
                        contents: [{
                            parts: [{ text: prompt }]
                        }],
                        generationConfig: {
                            temperature: 0.2,
                            maxOutputTokens: 2048
                        }
                    },
                    { headers, timeout: 15000 }
                );
            } else {
                throw error;
            }
        }
        
        const aiResponse = response.data.candidates[0].content.parts[0].text;
        console.log('✅ SQL Analysis Response Received\n');
        
        // Try to parse JSON from response
        const jsonMatch = aiResponse.match(/\{[\s\S]*\}/);
        if (jsonMatch) {
            const analysis = JSON.parse(jsonMatch[0]);
            
            console.log('📊 Analysis Results:');
            console.log(`   Complexity: ${analysis.complexity}`);
            console.log(`   Issues found: ${analysis.issues?.length || 0}`);
            console.log(`   Recommendations: ${analysis.recommendations?.length || 0}\n`);
            
            if (analysis.issues && analysis.issues.length > 0) {
                console.log('🚨 Top Issues:');
                analysis.issues.slice(0, 3).forEach((issue, i) => {
                    console.log(`   ${i + 1}. [${issue.severity}] ${issue.description}`);
                    console.log(`      Fix: ${issue.recommendation}`);
                });
            }
            
            if (analysis.recommendations && analysis.recommendations.length > 0) {
                console.log('\n💡 Top Recommendations:');
                analysis.recommendations.slice(0, 3).forEach((rec, i) => {
                    console.log(`   ${i + 1}. ${rec.title} (${rec.expectedImprovement} improvement)`);
                    console.log(`      ${rec.description}`);
                });
            }
            
            return analysis;
        } else {
            console.log('⚠️ Response was not valid JSON');
            console.log('Raw response preview:', aiResponse.substring(0, 200));
        }
        
    } catch (error) {
        console.error('❌ SQL optimization test failed:', error.response?.data?.error || error.message);
    }
}

async function generateUpdateInstructions(workingMethod) {
    console.log('\n✅ Configuration Instructions');
    console.log('==============================\n');
    
    if (workingMethod) {
        console.log('Based on test results, update your sql-analyzer.js:');
        console.log('');
        
        if (workingMethod.name.includes('X-Goog-Api-Key')) {
            console.log('1. Use the x-goog-api-key header method:');
            console.log(`
const apiUrl = 'https://generativelanguage.googleapis.com/v1beta/models/gemini-2.0-flash-exp:generateContent';

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
        headers: {
            'Content-Type': 'application/json',
            'x-goog-api-key': this.geminiApiKey
        },
        timeout: 30000
    }
);`);
        } else if (workingMethod.name.includes('1.5')) {
            console.log('1. Use Gemini 1.5 Flash (2.0 not available yet):');
            console.log(`
const apiUrl = \`https://generativelanguage.googleapis.com/v1beta/models/gemini-1.5-flash:generateContent?key=\${this.geminiApiKey}\`;`);
        }
        
        console.log('\n2. Add fallback logic for reliability');
        console.log('3. Implement proper error handling');
        console.log('4. Test with your actual slow queries');
    } else {
        console.log('❌ No working method found. Please check:');
        console.log('1. Your API key is valid');
        console.log('2. You have API access enabled');
        console.log('3. Your project has the Generative Language API enabled');
    }
}

// Main test function
async function runAllTests() {
    console.log('🚀 Gemini 2.0 API Configuration Test Suite\n');
    
    const workingMethod = await testGemini2API();
    
    if (workingMethod) {
        await testSQLOptimization();
        await generateUpdateInstructions(workingMethod);
    } else {
        console.log('\n❌ All API methods failed!');
        console.log('\nTroubleshooting:');
        console.log('1. Verify your API key at: https://aistudio.google.com/app/apikey');
        console.log('2. Check if Gemini 2.0 is available in your region');
        console.log('3. Ensure you have sufficient quota');
        console.log('4. Try generating a new API key');
    }
    
    console.log('\n✅ Testing complete!');
}

if (require.main === module) {
    runAllTests().catch(console.error);
}

module.exports = { testGemini2API, testSQLOptimization };