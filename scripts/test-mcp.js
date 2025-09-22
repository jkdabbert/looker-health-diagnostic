

const { spawn } = require('child_process');
const path = require('path');
require('dotenv').config();

async function tesctmcp() {
    console.log('Testing enhanced MCP dashboard retrieval...');
    
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
            LOOKER_VERIFY_SSL: 'true'
        },
        stdio: ['pipe', 'pipe', 'pipe']
    });

    let responseBuffer = '';

    toolbox.stdout.on('data', (data) => {
        responseBuffer += data.toString();
    });

    toolbox.stderr.on('data', (data) => {
        const error = data.toString();
        if (!error.includes('Making request') && !error.includes('Got response')) {
            console.error('MCP Error:', error.trim());
        }
    });

    // Try multiple dashboard commands
    const commands = [
        // Basic dashboard list
        {
            jsonrpc: "2.0",
            id: 1,
            method: "tools/call",
            params: {
                name: "get_dashboards",
                arguments: { limit: 3 }
            }
        },
        // Try with different parameters
        {
            jsonrpc: "2.0",
            id: 2,
            method: "tools/call",
            params: {
                name: "get_dashboards",
                arguments: {}
            }
        }
    ];

    // Send commands sequentially
    for (let i = 0; i < commands.length; i++) {
        console.log(`\nSending command ${i + 1}:`, commands[i].params.name);
        toolbox.stdin.write(JSON.stringify(commands[i]) + '\n');
        await new Promise(resolve => setTimeout(resolve, 3000));
    }

    setTimeout(() => {
        toolbox.kill();
        
        console.log('\n=== Full Response Analysis ===');
        console.log('Response length:', responseBuffer.length);
        
        try {
            // Parse each line as potential JSON
            const lines = responseBuffer.split('\n');
            let dashboardCount = 0;
            
            lines.forEach((line, index) => {
                if (line.trim()) {
                    try {
                        const response = JSON.parse(line.trim());
                        console.log(`\nResponse ${index + 1} (ID: ${response.id}):`);
                        
                        if (response.result && response.result.content) {
                            console.log('Content type:', typeof response.result.content);
                            console.log('Content is array:', Array.isArray(response.result.content));
                            
                            if (Array.isArray(response.result.content)) {
                                console.log('Content items:', response.result.content.length);
                                
                                response.result.content.forEach((item, i) => {
                                    if (item.type === 'text' && item.text) {
                                        try {
                                            const dashboard = JSON.parse(item.text);
                                            console.log(`  Dashboard ${i + 1}: ${dashboard.title || dashboard.id}`);
                                            dashboardCount++;
                                        } catch (e) {
                                            console.log(`  Item ${i + 1}: Could not parse as dashboard`);
                                        }
                                    }
                                });
                            }
                        } else if (response.error) {
                            console.log('Error:', response.error);
                        }
                    } catch (e) {
                        if (line.length > 20) {
                            console.log(`Line ${index + 1}: Not valid JSON (${line.substring(0, 50)}...)`);
                        }
                    }
                }
            });
            
            console.log(`\nTotal dashboards found: ${dashboardCount}`);
            
            if (dashboardCount === 0) {
                console.log('\nNo dashboards found. Debugging suggestions:');
                console.log('1. Check if your Looker credentials have dashboard access');
                console.log('2. Verify the MCP server version supports get_dashboards');
                console.log('3. Try running the command manually: ./scripts/toolbox --stdio --prebuilt looker');
            }
            
        } catch (error) {
            console.error('Error processing response:', error);
        }
    }, 10000);
}

tesctmcp().catch(console.error);