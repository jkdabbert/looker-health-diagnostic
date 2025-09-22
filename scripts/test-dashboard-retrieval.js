#!/usr/bin/env node

const { spawn } = require('child_process');
const path = require('path');
require('dotenv').config();

async function testCorrectedParsing() {
    console.log('Testing corrected MCP dashboard parsing...');
    
    const toolboxPath = path.join(__dirname, 'toolbox');
    
    const toolbox = spawn(toolboxPath, [
        '--stdio', 
        '--prebuilt', 
        'looker'
    ], {
        env: {
            ...process.env,
            LOOKER_BASE_URL: process.env.LOOKER_BASE_URL,
            LOOKER_CLIENT_ID: process.env.LOOKER_CLIENT_ID,
            LOOKER_CLIENT_SECRET: process.env.LOOKER_CLIENT_SECRET,
            LOOKER_VERIFY_SSL: process.env.LOOKER_VERIFY_SSL || 'true'
        },
        stdio: ['pipe', 'pipe', 'pipe']
    });

    let responseBuffer = '';

    toolbox.stdout.on('data', (data) => {
        responseBuffer += data.toString();
    });

    toolbox.stderr.on('data', (data) => {
        const error = data.toString();
        if (error.includes('ERROR') && !error.includes('Making request') && !error.includes('Got response')) {
            console.error('MCP Error:', error.trim());
        }
    });

    toolbox.on('error', (error) => {
        console.error('Failed to start toolbox:', error.message);
        process.exit(1);
    });

    // Request dashboards with limit 10 for testing
    const getDashboardsCommand = {
        jsonrpc: "2.0",
        id: 1,
        method: "tools/call",
        params: {
            name: "get_dashboards",
            arguments: {
                limit: 10
            }
        }
    };

    console.log('Requesting dashboards...');
    toolbox.stdin.write(JSON.stringify(getDashboardsCommand) + '\n');

    setTimeout(() => {
        toolbox.kill();
        
        console.log('\n=== Processing Response ===');
        
        try {
            const lines = responseBuffer.split('\n');
            let dashboards = [];
            
            for (const line of lines) {
                if (line.trim()) {
                    try {
                        const response = JSON.parse(line.trim());
                        
                        if (response.result && response.result.content && Array.isArray(response.result.content)) {
                            console.log(`Found ${response.result.content.length} dashboard entries`);
                            
                            // Parse each dashboard from the content array
                            for (const item of response.result.content) {
                                if (item.type === 'text' && item.text) {
                                    try {
                                        const dashboard = JSON.parse(item.text);
                                        dashboards.push(dashboard);
                                        console.log(`✅ Parsed dashboard: ${dashboard.title} (ID: ${dashboard.id})`);
                                    } catch (e) {
                                        console.log('❌ Could not parse dashboard JSON:', item.text);
                                    }
                                }
                            }
                        }
                    } catch (e) {
                        // Skip non-JSON lines
                    }
                }
            }
            
            console.log(`\n🎯 Total dashboards parsed: ${dashboards.length}`);
            
            if (dashboards.length > 0) {
                console.log('\n📋 Sample dashboards:');
                dashboards.slice(0, 3).forEach((dash, i) => {
                    console.log(`${i + 1}. ${dash.title} (ID: ${dash.id})`);
                    console.log(`   Description: ${dash.description || 'No description'}`);
                });
                
                console.log('\n✅ MCP dashboard retrieval working correctly!');
                console.log('You can now update your diagnostic engine with the corrected parsing logic.');
            } else {
                console.log('\n❌ No dashboards were successfully parsed');
            }
            
        } catch (error) {
            console.error('Error processing response:', error);
        }
    }, 6000);
}

if (require.main === module) {
    testCorrectedParsing().catch(console.error);
}

module.exports = { testCorrectedParsing };