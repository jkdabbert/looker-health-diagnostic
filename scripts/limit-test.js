// Test different limit parameters to see if that's causing the hang
const { spawn } = require('child_process');
const path = require('path');
require('dotenv').config();

async function testLimit(limit) {
    console.log(`\nTesting limit: ${limit}`);
    
    return new Promise((resolve) => {
        const toolboxPath = path.join(__dirname, '..', 'scripts', 'toolbox');
        
        const toolbox = spawn(toolboxPath, ['--stdio', '--prebuilt', 'looker'], {
            env: {
                ...process.env,
                LOOKER_BASE_URL: process.env.LOOKER_BASE_URL,
                LOOKER_CLIENT_ID: process.env.LOOKER_CLIENT_ID,
                LOOKER_CLIENT_SECRET: process.env.LOOKER_CLIENT_SECRET,
                LOOKER_VERIFY_SSL: 'true'
            },
            stdio: ['pipe', 'pipe', 'pipe']
        });

        let stdoutData = '';
        let stderrData = '';
        const startTime = Date.now();

        toolbox.stdout.on('data', (data) => {
            stdoutData += data.toString();
        });

        toolbox.stderr.on('data', (data) => {
            stderrData += data.toString();
        });

        toolbox.on('close', (code) => {
            const duration = Date.now() - startTime;
            console.log(`Limit ${limit}: ${duration}ms, stdout: ${stdoutData.length}, stderr: ${stderrData.length}, exit: ${code}`);
            resolve({ limit, duration, stdoutLength: stdoutData.length, success: code === 0 && stdoutData.length > 0 });
        });

        const command = {
            jsonrpc: "2.0",
            id: 1,
            method: "tools/call",
            params: { name: "get_dashboards", arguments: { limit } }
        };

        toolbox.stdin.write(JSON.stringify(command) + '\n');
        toolbox.stdin.end();

        // Timeout after 10 seconds
        setTimeout(() => {
            if (!toolbox.killed) {
                console.log(`Limit ${limit}: TIMEOUT`);
                toolbox.kill();
                resolve({ limit, duration: 10000, stdoutLength: 0, success: false, timeout: true });
            }
        }, 10000);
    });
}

async function runLimitTests() {
    console.log('Testing different limit parameters...');
    
    const limits = [3, 5, 10, 25, 50, 100];
    const results = [];
    
    for (const limit of limits) {
        const result = await testLimit(limit);
        results.push(result);
        
        // If we find a failing limit, stop testing higher limits
        if (!result.success) {
            console.log(`Found failing limit: ${limit}`);
            break;
        }
    }
    
    console.log('\nResults summary:');
    results.forEach(r => {
        const status = r.success ? 'SUCCESS' : (r.timeout ? 'TIMEOUT' : 'FAILED');
        console.log(`Limit ${r.limit}: ${status} (${r.duration}ms)`);
    });
    
    return results;
}

if (require.main === module) {
    runLimitTests()
        .then(results => {
            console.log('\nLimit testing complete');
        })
        .catch(error => {
            console.error('Limit testing failed:', error);
        });
}

module.exports = { runLimitTests };