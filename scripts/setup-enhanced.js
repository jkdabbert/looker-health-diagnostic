// #!/usr/bin/env node

// /**
//  * Enhanced Setup Script for Looker Health Diagnostic Assistant v2.0
//  * Sets up pagination, AI analysis, and enhanced MCP integration
//  */

// const fs = require('fs');
// const path = require('path');
// const https = require('https');
// const { exec, spawn } = require('child_process');
// const os = require('os');

// console.log('🚀 Setting up Enhanced Looker Health Diagnostic Assistant v2.0...\n');

// // Enhanced configuration
// const TOOLBOX_VERSION = 'latest';
// const PROJECT_DIRS = [
//     'src',
//     'src/components', 
//     'src/utils',
//     'src/config',
//     'public',
//     'scripts',
//     'test',
//     'test/integration',
//     'data',
//     'logs',
//     'docs'
// ];

// function createEnhancedDirectories() {
//     console.log('📁 Creating enhanced project directories...');
    
//     PROJECT_DIRS.forEach(dir => {
//         const dirPath = path.join(process.cwd(), dir);
//         if (!fs.existsSync(dirPath)) {
//             fs.mkdirSync(dirPath, { recursive: true });
//             console.log(`   ✅ Created ${dir}`);
//         } else {
//             console.log(`   ⭐ ${dir} already exists`);
//         }
//     });
    
//     console.log('');
// }

// function createEnhancedEnvFile() {
//     console.log('🔧 Setting up enhanced environment configuration...');
    
//     const envExample = path.join(process.cwd(), '.env.example');
//     const envFile = path.join(process.cwd(), '.env');
    
//     if (!fs.existsSync(envFile)) {
//         if (fs.existsSync(envExample)) {
//             fs.copyFileSync(envExample, envFile);
//             console.log('   ✅ Created .env from .env.example');
//         } else {
//             // Create basic .env with enhanced defaults
//             const defaultEnv = `# Looker Health Diagnostic Assistant v2.0 - Enhanced Configuration

// # Demo Mode (perfect for hackathons and testing)
// USE_MOCK_DATA=true

// # Server Configuration
// PORT=3000
// NODE_ENV=development

// # Looker Configuration (fill these in for real data)
// LOOKER_BASE_URL=
// LOOKER_CLIENT_ID=
// LOOKER_CLIENT_SECRET=
// LOOKER_VERIFY_SSL=true

// # AI Configuration (get your key from https://makersuite.google.com/app/apikey)
// GEMINI_API_KEY=

// # Enhanced Features
// ENABLE_AI_RECOMMENDATIONS=true
// ENABLE_PERFORMANCE_MONITORING=true
// ENABLE_USAGE_ANALYTICS=true
// ENABLE_QUERY_ANALYSIS=true

// # Pagination Settings
// PAGINATION_BATCH_SIZE=25
// MAX_DASHBOARDS=0

// # MCP Configuration
// MCP_TIMEOUT=15000
// MCP_BATCH_SIZE=25
// MCP_MAX_RETRIES=3

// # Development
// LOG_LEVEL=info
// MCP_DEBUG=false
// DETAILED_ERRORS=true
// `;
//             fs.writeFileSync(envFile, defaultEnv);
//             console.log('   ✅ Created .env with enhanced defaults');
//         }
//         console.log('   ⚠️  Please edit .env with your actual configuration');
//     } else {
//         console.log('   ⭐ .env already exists');
//     }
    
//     console.log('');
// }

// async function downloadMCPToolbox() {
//     return new Promise((resolve, reject) => {
//         console.log('📥 Downloading enhanced MCP Toolbox...');
        
//         const toolboxPath = path.join(process.cwd(), 'scripts', 'toolbox');
        
//         if (fs.existsSync(toolboxPath)) {
//             console.log('   ⭐ MCP Toolbox already exists');
//             resolve();
//             return;
//         }
        
//         const url = getToolboxUrl();
//         console.log(`   🔍 Downloading from: ${url}`);
        
//         const file = fs.createWriteStream(toolboxPath);
        
//         https.get(url, (response) => {
//             if (response.statusCode === 302 || response.statusCode === 301) {
//                 https.get(response.headers.location, (redirectResponse) => {
//                     redirectResponse.pipe(file);
                    
//                     file.on('finish', () => {
//                         file.close();
//                         fs.chmodSync(toolboxPath, '755');
//                         console.log('   ✅ Toolbox downloaded and made executable');
//                         resolve();
//                     });
//                 });
//             } else {
//                 response.pipe(file);
                
//                 file.on('finish', () => {
//                     file.close();
//                     fs.chmodSync(toolboxPath, '755');
//                     console.log('   ✅ Toolbox downloaded and made executable');
//                     resolve();
//                 });
//             }
//         }).on('error', (err) => {
//             fs.unlink(toolboxPath, () => {});
//             reject(err);
//         });
//     });
// }

// function getToolboxUrl() {
//     const platform = os.platform();
//     const arch = os.arch();
    
//     let filename;
    
//     if (platform === 'darwin') {
//         filename = arch === 'arm64' ? 'toolbox-darwin-arm64' : 'toolbox-darwin-amd64';
//     } else if (platform === 'linux') {
//         filename = arch === 'arm64' ? 'toolbox-linux-arm64' : 'toolbox-linux-amd64';
//     } else if (platform === 'win32') {
//         filename = 'toolbox-windows-amd64.exe';
//     } else {
//         throw new Error(`Unsupported platform: ${platform}`);
//     }
    
//     return `https://github.com/googleapis/genai-toolbox/releases/latest/download/${filename}`;
// }

// function createEnhancedTestFiles() {
//     console.log('🧪 Creating enhanced test files...');
    
//     // Create pagination test script
//     const paginationTest = `#!/usr/bin/env node

// const { LookerHealthDiagnostic } = require('../src/diagnostic-engine');
// require('dotenv').config();

// async function testPagination() {
//     console.log('📄 Testing MCP Pagination...');
    
//     const config = {
//         lookerUrl: process.env.LOOKER_BASE_URL || 'demo',
//         clientId: process.env.LOOKER_CLIENT_ID || 'demo',
//         clientSecret: process.env.LOOKER_CLIENT_SECRET || 'demo'
//     };

//     const diagnostic = new LookerHealthDiagnostic(config);

//     try {
//         console.log('Testing pagination with mock data...');
//         const data = await diagnostic.fetchAllDashboardsWithPagination();
        
//         console.log(\`✅ Pagination test completed:\`);
//         console.log(\`   Dashboards: \${data.dashboards.length}\`);
//         console.log(\`   Looks: \${data.looks?.length || 0}\`);
//         console.log(\`   MCP Calls: \${data.systemMetrics.mcpMetrics.callsPerformed}\`);
//         console.log(\`   Connection: \${data.systemMetrics.connectionType}\`);
        
//     } catch (error) {
//         console.error('❌ Pagination test failed:', error.message);
//     }
// }

// if (require.main === module) {
//     testPagination();
// }
// `;
    
//     fs.writeFileSync(path.join(process.cwd(), 'test', 'test-mcp-pagination.js'), paginationTest);
//     fs.chmodSync(path.join(process.cwd(), 'test', 'test-mcp-pagination.js'), '755');
    
//     // Create integration test
//     const integrationTest = `const request = require('supertest');
// const app = require('../src/app');

// describe('Enhanced Looker Health Diagnostic API', () => {
//     test('GET /api/health should return enhanced status', async () => {
//         const response = await request(app).get('/api/health');
//         expect(response.status).toBe(200);
//         expect(response.body.features.pagination).toBe(true);
//         expect(response.body.features.aiAnalysis).toBeDefined();
//     });

//     test('GET /api/config should return enhanced configuration', async () => {
//         const response = await request(app).get('/api/config');
//         expect(response.status).toBe(200);
//         expect(response.body.features.paginatedFetching).toBe(true);
//         expect(response.body.features.mcpTools).toBeDefined();
//     });

//     test('POST /api/diagnostic/run should complete with pagination', async () => {
//         const response = await request(app).post('/api/diagnostic/run');
//         expect(response.status).toBe(200);
//         expect(response.body.enhancedFeatures.paginationEnabled).toBe(true);
//     });
// });
// `;
    
//     fs.writeFileSync(path.join(process.cwd(), 'test', 'integration', 'api.test.js'), integrationTest);
    
//     console.log('   ✅ Created enhanced test files');
//     console.log('');
// }

// function createValidationScript() {
//     console.log('🔍 Creating environment validation script...');
    
//     const validationScript = `#!/usr/bin/env node

// require('dotenv').config();

// function validateEnvironment() {
//     console.log('🔍 Validating Enhanced Configuration...');
//     console.log('=====================================\\n');

//     const checks = [];
    
//     // Basic configuration
//     checks.push({
//         name: 'Node.js Version',
//         status: process.version >= 'v18.0.0' ? 'PASS' : 'FAIL',
//         value: process.version,
//         required: 'v18.0.0+'
//     });
    
//     // Environment variables
//     const envVars = [
//         { key: 'PORT', required: false, default: '3000' },
//         { key: 'USE_MOCK_DATA', required: false, default: 'true' },
//         { key: 'LOOKER_BASE_URL', required: false, note: 'Required for live data' },
//         { key: 'LOOKER_CLIENT_ID', required: false, note: 'Required for live data' },
//         { key: 'LOOKER_CLIENT_SECRET', required: false, note: 'Required for live data' },
//         { key: 'GEMINI_API_KEY', required: false, note: 'Required for AI features' },
//         { key: 'PAGINATION_BATCH_SIZE', required: false, default: '25' },
//         { key: 'ENABLE_AI_RECOMMENDATIONS', required: false, default: 'true' }
//     ];
    
//     envVars.forEach(env => {
//         const value = process.env[env.key];
//         const status = value ? 'SET' : (env.required ? 'MISSING' : 'DEFAULT');
        
//         checks.push({
//             name: env.key,
//             status: status,
//             value: value || env.default || 'undefined',
//             note: env.note
//         });
//     });
    
//     // File checks
//     const fs = require('fs');
//     const path = require('path');
    
//     const files = [
//         'scripts/toolbox',
//         'src/diagnostic-engine.js',
//         'src/app.js',
//         '.env'
//     ];
    
//     files.forEach(file => {
//         const exists = fs.existsSync(path.join(process.cwd(), file));
//         checks.push({
//             name: \`File: \${file}\`,
//             status: exists ? 'EXISTS' : 'MISSING',
//             value: exists ? 'Found' : 'Not found'
//         });
//     });
    
//     // Display results
//     checks.forEach(check => {
//         const statusIcon = {
//             'PASS': '✅',
//             'FAIL': '❌',
//             'SET': '✅',
//             'MISSING': '❌',
//             'DEFAULT': '⚠️',
//             'EXISTS': '✅'
//         }[check.status] || '❓';
        
//         console.log(\`\${statusIcon} \${check.name}: \${check.value}\${check.note ? \` (\${check.note})\` : ''}\`);
//     });
    
//     console.log('\\n📋 Summary:');
//     const issues = checks.filter(c => c.status === 'FAIL' || c.status === 'MISSING');
    
//     if (issues.length === 0) {
//         console.log('✅ All checks passed! Ready for enhanced diagnostics.');
//     } else {
//         console.log(\`⚠️  \${issues.length} issues found:\`);
//         issues.forEach(issue => {
//             console.log(\`   • \${issue.name}: \${issue.value}\`);
//         });
//     }
    
//     console.log('\\n🚀 Next Steps:');
//     console.log('   1. npm install (install dependencies)');
//     console.log('   2. npm run test-mcp (test MCP connection)');
//     console.log('   3. npm run test-ai (test AI analysis)');
//     console.log('   4. npm run dev (start server)');
// }

// if (require.main === module) {
//     validateEnvironment();
// }

// module.exports = { validateEnvironment };
// `;
    
//     fs.writeFileSync(path.join(process.cwd(), 'scripts', 'validate-env.js'), validationScript);
//     fs.chmodSync(path.join(process.cwd(), 'scripts', 'validate-env.js'), '755');
    
//     console.log('   ✅ Created validation script');
//     console.log('');
// }

// function createEnhancedDocumentation() {
//     console.log('📚 Creating enhanced documentation...');
    
//     const readme = `# Looker Health Diagnostic Assistant v2.0 - Enhanced Edition

// 🤖 AI-powered health evaluation system for Looker instances with advanced MCP integration, pagination support, and intelligent query analysis.

// ## 🚀 New in v2.0

// - **Pagination Support**: Fetch all 336+ dashboards via multiple MCP calls
// - **AI Query Analysis**: Gemini-powered analysis of slow queries with LookML optimization suggestions
// - **Enhanced MCP Tools**: Support for get_dashboards, get_looks, and query operations
// - **Intelligent Recommendations**: AI-generated action plans with performance insights
// - **Advanced Mock Data**: 336 simulated dashboards for comprehensive testing

// ## ⚡ Quick Start

// \`\`\`bash
// # 1. Install dependencies
// npm install

// # 2. Set up configuration (creates .env with defaults)
// npm run setup-enhanced

// # 3. Test in demo mode (uses 336 mock dashboards)
// npm run demo

// # 4. Test AI features (requires Gemini API key)
// npm run demo:ai

// # 5. Validate configuration
// npm run validate-env
// \`\`\`

// ## 🔧 Configuration

// ### Essential Settings (.env)

// \`\`\`bash
// # Demo Mode (perfect for hackathons)
// USE_MOCK_DATA=true

// # AI Features (get key from https://makersuite.google.com/app/apikey)
// GEMINI_API_KEY=your_key_here

// # Real Looker Data (for production)
// LOOKER_BASE_URL=https://company.looker.com
// LOOKER_CLIENT_ID=your_client_id
// LOOKER_CLIENT_SECRET=your_client_secret

// # Enhanced Features
// ENABLE_AI_RECOMMENDATIONS=true
// PAGINATION_BATCH_SIZE=25
// \`\`\`

// ## 🧪 Testing

// \`\`\`bash
// # Test MCP connection and pagination
// npm run test-mcp

// # Test AI query analysis
// npm run test-ai

// # Test pagination with 336 dashboards
// npm run test:mcp-pagination

// # Run all tests
// npm test
// \`\`\`

// ## 🎯 API Endpoints

// ### Enhanced Endpoints

// - \`POST /api/diagnostic/run\` - Full diagnostic with AI analysis
// - \`POST /api/test-mcp\` - Test MCP connection and pagination
// - \`POST /api/analyze-query\` - AI analysis of specific queries
// - \`GET /api/pagination-status\` - Check pagination capabilities

// ### Request Examples

// \`\`\`javascript
// // Analyze specific query
// fetch('/api/analyze-query', {
//   method: 'POST',
//   headers: { 'Content-Type': 'application/json' },
//   body: JSON.stringify({
//     queryId: 'slow_query_123',
//     queryText: 'SELECT * FROM large_table...',
//     runtime: 8.5
//   })
// });
// \`\`\`

// ## 🤖 AI Features

// ### Query Analysis
// - Identifies performance bottlenecks in SQL
// - Suggests specific optimizations (indexing, query structure)
// - Provides LookML improvement recommendations
// - Estimates performance gains

// ### Smart Recommendations
// - Prioritized action plans based on severity and impact
// - Strategic initiatives for long-term improvements
// - Query optimization roadmaps
// - LookML best practices

// ## 📊 Pagination Details

// The system automatically handles pagination to fetch all dashboards:
// - **Batch Size**: 25 dashboards per MCP call (configurable)
// - **Total Capacity**: Tested with 336+ dashboards
// - **Error Handling**: Graceful fallback on MCP failures
// - **Performance**: Optimized with request delays and retry logic

// ## 🏗️ Architecture

// \`\`\`
// ┌─── Enhanced Diagnostic Engine ───┐
// │  ├─ Pagination Manager           │
// │  ├─ AI Query Analyzer           │
// │  ├─ Multi-tool MCP Client       │
// │  └─ Smart Recommendation Engine │
// └─────────────────────────────────┘
//          ▼
// ┌─── MCP Integration Layer ───┐
// │  ├─ get_dashboards (paginated) │
// │  ├─ get_looks                  │
// │  ├─ query (performance data)   │
// │  └─ Error handling & retries   │
// └───────────────────────────────┘
//          ▼
// ┌─── AI Analysis Layer ───┐
// │  ├─ Gemini API         │
// │  ├─ Local fallback     │
// │  ├─ Query optimization │
// │  └─ LookML suggestions │
// └───────────────────────┘
// \`\`\`

// ## 🎭 Demo Mode Features

// Perfect for hackathon presentations:
// - 336 realistic mock dashboards
// - 50 sample looks
// - 20 slow queries for AI analysis
// - Simulated MCP pagination calls
// - Full feature demonstration without Looker credentials

// ## 🔍 Troubleshooting

// ### Common Issues

// **"MCP pagination failed"**
// - Check MCP toolbox installation: \`ls scripts/toolbox\`
// - Verify permissions: \`chmod +x scripts/toolbox\`
// - Test connection: \`npm run test-mcp\`

// **"AI analysis disabled"**
// - Add Gemini API key to .env: \`GEMINI_API_KEY=your_key\`
// - Test AI features: \`npm run test-ai\`

// **"Authentication failed"**
// - Verify Looker credentials in .env
// - Check API permissions in Looker Admin
// - Test with demo mode: \`USE_MOCK_DATA=true\`

// ### Performance Optimization

// - Adjust batch size: \`PAGINATION_BATCH_SIZE=20\`
// - Limit dashboard count: \`MAX_DASHBOARDS=100\`
// - Enable MCP debugging: \`MCP_DEBUG=true\`

// ## 🏆 Hackathon Ready

// This enhanced version is optimized for hackathon demonstrations:

// 1. **Quick Demo Setup**: \`npm run demo\` for instant results
// 2. **AI Showcase**: Full AI analysis with mock data
// 3. **Pagination Demo**: Shows handling of 336+ dashboards
// 4. **Professional UI**: React dashboard with modern design
// 5. **Zero Config**: Works out of the box with mock data

// ## 🔗 Links

// - [Gemini API Setup](https://makersuite.google.com/app/apikey)
// - [MCP Toolbox](https://github.com/googleapis/genai-toolbox)
// - [Looker API Documentation](https://developers.looker.com/api/)

// ## 📝 License

// MIT License - Built for Looker Hackathon 2025
// `;
    
//     fs.writeFileSync(path.join(process.cwd(), 'README.md'), readme);
    
//     console.log('   ✅ Created enhanced README.md');
//     console.log('');
// }

// function displayEnhancedNextSteps() {
//     console.log('🎯 Enhanced Setup Complete! Next Steps:\\n');
    
//     console.log('1. 🔑 Configure AI Features:');
//     console.log('   • Get Gemini API key: https://makersuite.google.com/app/apikey');
//     console.log('   • Add to .env: GEMINI_API_KEY=your_key_here\\n');
    
//     console.log('2. 🧪 Test Enhanced Features:');
//     console.log('   npm run validate-env     # Check configuration');
//     console.log('   npm run test-mcp         # Test MCP pagination');
//     console.log('   npm run test-ai          # Test AI analysis');
//     console.log('   npm run demo             # Start with mock data\\n');
    
//     console.log('3. 🚀 Run Enhanced Diagnostic:');
//     console.log('   npm run dev              # Start server');
//     console.log('   Open http://localhost:3000 (basic UI)');
//     console.log('   Open http://localhost:3000/dashboard (React UI)\\n');
    
//     console.log('4. 📊 API Testing:');
//     console.log('   POST /api/diagnostic/run      # Full enhanced diagnostic');
//     console.log('   POST /api/test-mcp            # Test pagination');
//     console.log('   POST /api/analyze-query       # AI query analysis\\n');
    
//     console.log('5. 🏆 For Hackathon Demo:');
//     console.log('   • USE_MOCK_DATA=true (336 dashboards ready)');
//     console.log('   • Add GEMINI_API_KEY for AI features');
//     console.log('   • Run npm run demo:ai for full showcase\\n');
    
//     console.log('📈 Enhanced Features Ready:');
//     console.log('   ✅ Pagination (336+ dashboards)');
//     console.log('   ✅ AI Query Analysis (Gemini-powered)');
//     console.log('   ✅ Multiple MCP Tools');
//     console.log('   ✅ Smart Recommendations');
//     console.log('   ✅ Enhanced Mock Data\\n');
    
//     console.log('💡 Pro Tips:');
//     console.log('   • Start with demo mode for immediate results');
//     console.log('   • Add real Looker credentials gradually');
//     console.log('   • Use React dashboard for best presentation');
//     console.log('   • Check logs for MCP debugging info\\n');
    
//     console.log('🆘 Need Help?');
//     console.log('   • Check README.md for detailed docs');
//     console.log('   • Run npm run validate-env for diagnostics');
//     console.log('   • Enable MCP_DEBUG=true for verbose logging');
// }

// async function main() {
//     try {
//         console.log('🛠️  Enhanced Looker Health Diagnostic Setup v2.0\\n');
        
//         createEnhancedDirectories();
//         createEnhancedEnvFile();
        
//         await downloadMCPToolbox();
//         createEnhancedTestFiles();
//         createValidationScript();
//         createEnhancedDocumentation();
        
//         displayEnhancedNextSteps();
        
//     } catch (error) {
//         console.error('❌ Enhanced setup failed:', error.message);
//         process.exit(1);
//     }
// }

// // Run setup if called directly
// if (require.main === module) {
//     main();
// }

// module.exports = {
//     createEnhancedDirectories,
//     createEnhancedEnvFile,
//     downloadMCPToolbox,
//     createEnhancedTestFiles,
//     createValidationScript,
//     createEnhancedDocumentation
// };