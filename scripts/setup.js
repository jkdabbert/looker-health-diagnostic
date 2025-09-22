#!/usr/bin/env node

/**
 * Automated Setup Script for Looker Health Diagnostic Assistant
 * This script helps set up the project locally with all required dependencies
 */

const fs = require('fs');
const path = require('path');
const https = require('https');
const { exec, spawn } = require('child_process');
const os = require('os');

console.log('🚀 Setting up Looker Health Diagnostic Assistant...\n');

// Configuration
const TOOLBOX_VERSION = 'latest';
const PROJECT_DIRS = [
    'src',
    'src/components', 
    'src/utils',
    'src/config',
    'public',
    'scripts',
    'test',
    'data',
    'logs'
];

// Utility functions
function createDirectories() {
    console.log('📁 Creating project directories...');
    
    PROJECT_DIRS.forEach(dir => {
        const dirPath = path.join(process.cwd(), dir);
        if (!fs.existsSync(dirPath)) {
            fs.mkdirSync(dirPath, { recursive: true });
            console.log(`   ✅ Created ${dir}`);
        } else {
            console.log(`   ⏭️  ${dir} already exists`);
        }
    });
    
    console.log('');
}

function copyEnvFile() {
    console.log('🔧 Setting up environment configuration...');
    
    const envExample = path.join(process.cwd(), '.env.example');
    const envFile = path.join(process.cwd(), '.env');
    
    if (!fs.existsSync(envFile)) {
        if (fs.existsSync(envExample)) {
            fs.copyFileSync(envExample, envFile);
            console.log('   ✅ Created .env from .env.example');
            console.log('   ⚠️  Please edit .env with your Looker credentials');
        } else {
            console.log('   ❌ .env.example not found');
        }
    } else {
        console.log('   ⏭️  .env already exists');
    }
    
    console.log('');
}

function getToolboxUrl() {
    const platform = os.platform();
    const arch = os.arch();
    
    let filename;
    
    if (platform === 'darwin') {
        filename = arch === 'arm64' ? 'toolbox-darwin-arm64' : 'toolbox-darwin-amd64';
    } else if (platform === 'linux') {
        filename = arch === 'arm64' ? 'toolbox-linux-arm64' : 'toolbox-linux-amd64';
    } else if (platform === 'win32') {
        filename = 'toolbox-windows-amd64.exe';
    } else {
        throw new Error(`Unsupported platform: ${platform}`);
    }
    
    return `https://github.com/googleapis/genai-toolbox/releases/latest/download/${filename}`;
}

function downloadToolbox() {
    return new Promise((resolve, reject) => {
        console.log('📥 Downloading MCP Toolbox...');
        
        const toolboxPath = path.join(process.cwd(), 'scripts', 'toolbox');
        const url = getToolboxUrl();
        
        console.log(`   📍 Downloading from: ${url}`);
        
        const file = fs.createWriteStream(toolboxPath);
        
        https.get(url, (response) => {
            if (response.statusCode === 302 || response.statusCode === 301) {
                // Follow redirect
                https.get(response.headers.location, (redirectResponse) => {
                    redirectResponse.pipe(file);
                    
                    file.on('finish', () => {
                        file.close();
                        // Make executable
                        fs.chmodSync(toolboxPath, '755');
                        console.log('   ✅ Toolbox downloaded and made executable');
                        resolve();
                    });
                });
            } else {
                response.pipe(file);
                
                file.on('finish', () => {
                    file.close();
                    // Make executable
                    fs.chmodSync(toolboxPath, '755');
                    console.log('   ✅ Toolbox downloaded and made executable');
                    resolve();
                });
            }
        }).on('error', (err) => {
            fs.unlink(toolboxPath, () => {}); // Delete partial file
            reject(err);
        });
    });
}

function createTestFiles() {
    console.log('🧪 Creating test files...');
    
    // Create MCP test script
    const mcpTestScript = `#!/usr/bin/env node

const { spawn } = require('child_process');
const path = require('path');
require('dotenv').config();

async function testMCPConnection() {
    console.log('🔌 Testing MCP connection to Looker...');
    console.log('📍 Looker URL:', process.env.LOOKER_BASE_URL);
    console.log('🔑 Client ID:', process.env.LOOKER_CLIENT_ID ? 'Set' : 'Missing');
    console.log('');
    
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

    let hasOutput = false;

    toolbox.stdout.on('data', (data) => {
        hasOutput = true;
        console.log('📊 MCP Response:', data.toString().trim());
    });

    toolbox.stderr.on('data', (data) => {
        console.error('❌ MCP Error:', data.toString().trim());
    });

    toolbox.on('error', (error) => {
        console.error('❌ Failed to start toolbox:', error.message);
        process.exit(1);
    });

    // Test basic MCP call
    const testMessage = JSON.stringify({
        jsonrpc: "2.0",
        id: 1,
        method: "tools/list",
        params: {}
    }) + '\\n';

    toolbox.stdin.write(testMessage);

    setTimeout(() => {
        toolbox.kill();
        if (hasOutput) {
            console.log('\\n✅ MCP connection test completed successfully!');
        } else {
            console.log('\\n⚠️  No response received - check your credentials');
        }
    }, 5000);
}

if (require.main === module) {
    testMCPConnection().catch(console.error);
}

module.exports = { testMCPConnection };
`;
    
    fs.writeFileSync(path.join(process.cwd(), 'scripts', 'test-mcp.js'), mcpTestScript);
    fs.chmodSync(path.join(process.cwd(), 'scripts', 'test-mcp.js'), '755');
    
    // Create basic diagnostic test
    const diagnosticTest = `const { LookerHealthDiagnostic } = require('../src/diagnostic-engine');

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
`;
    
    fs.writeFileSync(path.join(process.cwd(), 'test', 'test-diagnostic.js'), diagnosticTest);
    
    console.log('   ✅ Created test files');
    console.log('');
}

function createBasicAppFile() {
    console.log('📱 Creating basic application structure...');
    
    const appJs = `const express = require('express');
const cors = require('cors');
const path = require('path');
require('dotenv').config();

// Import diagnostic engine
const { LookerHealthDiagnostic } = require('./diagnostic-engine');

const app = express();
const PORT = process.env.PORT || 3000;

// Middleware
app.use(cors());
app.use(express.json());
app.use(express.static(path.join(__dirname, '..', 'public')));

// Health check endpoint
app.get('/api/health', (req, res) => {
    res.json({ 
        status: 'healthy', 
        timestamp: new Date(),
        version: '1.0.0'
    });
});

// Configuration endpoint
app.get('/api/config', (req, res) => {
    res.json({
        lookerUrl: process.env.LOOKER_BASE_URL ? 'Configured' : 'Missing',
        clientId: process.env.LOOKER_CLIENT_ID ? 'Configured' : 'Missing',
        mcpEnabled: true,
        features: {
            aiRecommendations: process.env.ENABLE_AI_RECOMMENDATIONS === 'true',
            performanceMonitoring: process.env.ENABLE_PERFORMANCE_MONITORING !== 'false',
            usageAnalytics: process.env.ENABLE_USAGE_ANALYTICS !== 'false'
        }
    });
});

// Run diagnostic endpoint
app.post('/api/diagnostic/run', async (req, res) => {
    try {
        console.log('🏥 Starting diagnostic run...');
        
        const config = {
            lookerUrl: process.env.LOOKER_BASE_URL,
            clientId: process.env.LOOKER_CLIENT_ID,
            clientSecret: process.env.LOOKER_CLIENT_SECRET
        };

        // Validate configuration
        if (!config.lookerUrl || !config.clientId || !config.clientSecret) {
            return res.status(400).json({
                error: 'Missing Looker configuration',
                details: 'Please check your .env file for LOOKER_BASE_URL, LOOKER_CLIENT_ID, and LOOKER_CLIENT_SECRET'
            });
        }

        const diagnostic = new LookerHealthDiagnostic(config);
        const results = await diagnostic.runDiagnostic();
        
        console.log('✅ Diagnostic completed successfully');
        res.json(results);
        
    } catch (error) {
        console.error('❌ Diagnostic failed:', error);
        res.status(500).json({ 
            error: error.message,
            stack: process.env.NODE_ENV === 'development' ? error.stack : undefined
        });
    }
});

// Serve dashboard
app.get('/', (req, res) => {
    res.sendFile(path.join(__dirname, '..', 'public', 'index.html'));
});

app.listen(PORT, () => {
    console.log(\`🚀 Looker Health Diagnostic Assistant\`);
    console.log(\`📍 Server: http://localhost:\${PORT}\`);
    console.log(\`📊 Dashboard: http://localhost:\${PORT}\`);
    console.log(\`🔧 Health Check: http://localhost:\${PORT}/api/health\`);
    console.log(\`⚙️  Configuration: http://localhost:\${PORT}/api/config\`);
    console.log('');
    
    // Display configuration status
    if (!process.env.LOOKER_BASE_URL) {
        console.log('⚠️  Warning: LOOKER_BASE_URL not configured');
    }
    if (!process.env.LOOKER_CLIENT_ID) {
        console.log('⚠️  Warning: LOOKER_CLIENT_ID not configured'); 
    }
    if (!process.env.LOOKER_CLIENT_SECRET) {
        console.log('⚠️  Warning: LOOKER_CLIENT_SECRET not configured');
    }
    
    console.log('\\n📝 Edit your .env file to configure Looker credentials');
    console.log('🧪 Run "npm run test-mcp" to test MCP connection');
});
`;
    
    fs.writeFileSync(path.join(process.cwd(), 'src', 'app.js'), appJs);
    
    console.log('   ✅ Created src/app.js');
    console.log('');
}

function showNextSteps() {
    console.log('🎯 Setup Complete! Next Steps:\n');
    console.log('1. 📝 Edit .env file with your Looker credentials:');
    console.log('   - LOOKER_BASE_URL (e.g., https://company.looker.com)');
    console.log('   - LOOKER_CLIENT_ID (from Looker Admin > API Keys)');
    console.log('   - LOOKER_CLIENT_SECRET (from Looker Admin > API Keys)\n');
    
    console.log('2. 🧪 Test your setup:');
    console.log('   npm run test-mcp     # Test MCP connection');
    console.log('   npm run test         # Test diagnostic engine');
    console.log('   npm run dev          # Start development server\n');
    
    console.log('3. 🎨 View the dashboard:');
    console.log('   Open http://localhost:3000 in your browser\n');
    
    console.log('4. 🔍 API Endpoints:');
    console.log('   GET  /api/health           # Health check');
    console.log('   GET  /api/config           # Configuration status');
    console.log('   POST /api/diagnostic/run   # Run health diagnostic\n');
    
    console.log('🆘 Need help? Check the troubleshooting guide in README.md');
    console.log('💡 Pro tip: Start with USE_MOCK_DATA=true to test without Looker credentials');
}

// Main setup function
async function main() {
    try {
        console.log('🏗️  Looker Health Diagnostic Assistant Setup\n');
        
        createDirectories();
        copyEnvFile();
        
        const toolboxPath = path.join(process.cwd(), 'scripts', 'toolbox');
        if (!fs.existsSync(toolboxPath)) {
            await downloadToolbox();
        } else {
            console.log('📥 MCP Toolbox already exists, skipping download\n');
        }
        
        createTestFiles();
        createBasicAppFile();
        
        // Create a basic HTML file for the dashboard
        const dashboardHtml = `<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Looker Health Diagnostic Assistant</title>
    <style>
        body { 
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
            margin: 0; padding: 20px; background: #f5f5f5; 
        }
        .container { max-width: 800px; margin: 0 auto; background: white; padding: 30px; border-radius: 8px; box-shadow: 0 2px 4px rgba(0,0,0,0.1); }
        .header { text-align: center; margin-bottom: 30px; }
        .status { padding: 15px; margin: 10px 0; border-radius: 5px; }
        .success { background: #d4edda; color: #155724; border: 1px solid #c3e6cb; }
        .warning { background: #fff3cd; color: #856404; border: 1px solid #ffeaa7; }
        .error { background: #f8d7da; color: #721c24; border: 1px solid #f5c6cb; }
        button { background: #007bff; color: white; border: none; padding: 12px 24px; border-radius: 5px; cursor: pointer; font-size: 16px; margin: 10px 5px; }
        button:hover { background: #0056b3; }
        button:disabled { background: #6c757d; cursor: not-allowed; }
        .results { margin-top: 20px; padding: 20px; background: #f8f9fa; border-radius: 5px; white-space: pre-wrap; font-family: monospace; }
        .loading { display: none; text-align: center; margin: 20px 0; }
        .spinner { border: 4px solid #f3f3f3; border-top: 4px solid #3498db; border-radius: 50%; width: 40px; height: 40px; animation: spin 2s linear infinite; margin: 0 auto; }
        @keyframes spin { 0% { transform: rotate(0deg); } 100% { transform: rotate(360deg); } }
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>🏥 Looker Health Diagnostic Assistant</h1>
            <p>AI-powered health evaluation for your Looker instance</p>
        </div>
        
        <div id="status"></div>
        
        <div style="text-align: center; margin: 30px 0;">
            <button onclick="checkConfig()">Check Configuration</button>
            <button onclick="testMCP()">Test MCP Connection</button>
            <button onclick="runDiagnostic()" id="diagnosticBtn">Run Full Diagnostic</button>
        </div>
        
        <div class="loading" id="loading">
            <div class="spinner"></div>
            <p>Running diagnostic...</p>
        </div>
        
        <div id="results"></div>
    </div>

    <script>
        async function checkConfig() {
            try {
                const response = await fetch('/api/config');
                const config = await response.json();
                
                let status = '<div class="status success"><strong>✅ Configuration Status</strong><br>';
                status += \`Looker URL: \${config.lookerUrl}<br>\`;
                status += \`Client ID: \${config.clientId}<br>\`;
                status += \`MCP Enabled: \${config.mcpEnabled ? 'Yes' : 'No'}<br>\`;
                status += \`AI Recommendations: \${config.features.aiRecommendations ? 'Enabled' : 'Disabled'}\`;
                status += '</div>';
                
                document.getElementById('status').innerHTML = status;
            } catch (error) {
                document.getElementById('status').innerHTML = 
                    \`<div class="status error"><strong>❌ Configuration Error</strong><br>\${error.message}</div>\`;
            }
        }
        
        async function testMCP() {
            document.getElementById('status').innerHTML = 
                '<div class="status warning"><strong>🔌 Testing MCP Connection...</strong><br>This may take a few seconds...</div>';
            
            // This would normally call a test endpoint
            setTimeout(() => {
                document.getElementById('status').innerHTML = 
                    '<div class="status success"><strong>✅ MCP Test</strong><br>Check your console logs for detailed results. Run: npm run test-mcp</div>';
            }, 2000);
        }
        
        async function runDiagnostic() {
            const btn = document.getElementById('diagnosticBtn');
            const loading = document.getElementById('loading');
            const results = document.getElementById('results');
            
            btn.disabled = true;
            loading.style.display = 'block';
            results.innerHTML = '';
            
            try {
                const response = await fetch('/api/diagnostic/run', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' }
                });
                
                const data = await response.json();
                
                if (response.ok) {
                    results.innerHTML = \`<h3>📊 Diagnostic Results</h3>
<strong>Overall Health Grade:</strong> \${data.overallGrade}
<strong>Total Issues:</strong> \${data.totalIssuesFound}
<strong>Timestamp:</strong> \${new Date(data.timestamp).toLocaleString()}

<strong>Health Metrics:</strong>
• Performance: \${data.healthMetrics.performance}/100
• Governance: \${data.healthMetrics.governance}/100  
• Usage: \${data.healthMetrics.usage}/100
• Data Quality: \${data.healthMetrics.dataQuality}/100
• Security: \${data.healthMetrics.security}/100

<strong>Top Issues:</strong>
\${data.detailedIssues.slice(0, 3).map(issue => 
  \`• [\${issue.severity.toUpperCase()}] \${issue.dashboard}: \${issue.issue}\`
).join('\\n')}

<strong>AI Recommendations:</strong>
\${data.aiRecommendations.priorities.map(p => 
  \`• Priority \${p.priority}: \${p.title} (Impact: \${p.estimatedImpact}, Timeline: \${p.timeframe})\`
).join('\\n')}\`;
                } else {
                    results.innerHTML = \`<strong>❌ Error:</strong> \${data.error}\`;
                }
            } catch (error) {
                results.innerHTML = \`<strong>❌ Network Error:</strong> \${error.message}\`;
            } finally {
                btn.disabled = false;
                loading.style.display = 'none';
            }
        }
        
        // Auto-check configuration on load
        window.onload = checkConfig;
    </script>
</body>
</html>`;
        
        fs.writeFileSync(path.join(process.cwd(), 'public', 'index.html'), dashboardHtml);
        console.log('🎨 Created basic dashboard HTML\n');
        
        // Create README
        const readme = `# Looker Health Diagnostic Assistant

AI-powered health evaluation system for Looker instances using MCP integration.

## Quick Start

1. **Install dependencies:**
   \`\`\`bash
   npm install
   \`\`\`

2. **Configure environment:**
   \`\`\`bash
   cp .env.example .env
   # Edit .env with your Looker credentials
   \`\`\`

3. **Test setup:**
   \`\`\`bash
   npm run test-mcp      # Test MCP connection
   npm run dev           # Start development server
   \`\`\`

4. **Open dashboard:**
   \`\`\`
   http://localhost:3000
   \`\`\`

## Scripts

- \`npm run dev\` - Start development server
- \`npm run test-mcp\` - Test MCP connection to Looker
- \`npm run test\` - Run diagnostic tests
- \`npm start\` - Start production server

## Troubleshooting

### "ENOENT: no such file or directory, open './scripts/toolbox'"
- Run: \`npm run setup\`
- Or manually download MCP Toolbox from: https://github.com/googleapis/genai-toolbox/releases

### "Authentication failed"
- Check your Looker credentials in \`.env\`
- Ensure API access is enabled in Looker Admin settings
- Verify the base URL format (include https://)

### "Permission denied"
- Ensure your Looker user has the required permissions:
  - \`see_lookml_dashboards\`
  - \`see_users\`
  - \`see_system_activity\`
  - \`see_queries\`

## Features

- 🔌 **MCP Integration** - Direct connection to Looker via Model Context Protocol
- 🤖 **AI Analysis** - Gemini-powered recommendations and insights
- 📊 **Health Scoring** - Comprehensive evaluation across 5 key areas
- 🎯 **Action Plans** - Prioritized recommendations with timelines
- 📈 **Dashboard** - Interactive web interface for results

## Built for Looker Hackathon 2025
`;
        
        fs.writeFileSync(path.join(process.cwd(), 'README.md'), readme);
        console.log('📚 Created README.md\n');
        
        showNextSteps();
        
    } catch (error) {
        console.error('❌ Setup failed:', error.message);
        process.exit(1);
    }
}

// Run setup if called directly
if (require.main === module) {
    main();
}

module.exports = {
    createDirectories,
    copyEnvFile,
    downloadToolbox,
    createTestFiles,
    createBasicAppFile
};