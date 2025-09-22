// src/connectors/looker-api-connector.js
// Direct Looker API integration for LookML files and additional data

class LookerAPIConnector {
    constructor(config) {
        this.config = config;
        this.accessToken = null;
        this.tokenExpiry = null;
        this.axios = require('axios');
        this.baseUrl = config.lookerUrl?.replace(/\/$/, '');
    }

    /**
     * Initialize connection and get access token
     */
    async initialize() {
        try {
            console.log('Initializing Looker API connector...');
            
            if (!this.config.lookerUrl || !this.config.clientId || !this.config.clientSecret) {
                throw new Error('Missing required Looker API configuration');
            }

            await this.refreshAccessToken();
            console.log('Looker API connector initialized successfully');
            return true;
            
        } catch (error) {
            console.error('Looker API connector initialization failed:', error);
            return false;
        }
    }

    /**
     * Get or refresh access token
     */
    async refreshAccessToken() {
        try {
            const loginUrl = `${this.baseUrl}/api/4.0/login`;
            console.log(`Getting Looker access token from: ${loginUrl}`);
            
            const formData = new URLSearchParams();
            formData.append('client_id', this.config.clientId);
            formData.append('client_secret', this.config.clientSecret);
            
            const response = await this.axios.post(loginUrl, formData, {
                headers: {
                    'Content-Type': 'application/x-www-form-urlencoded'
                },
                timeout: 15000
            });
            
            this.accessToken = response.data.access_token;
            this.tokenExpiry = Date.now() + (response.data.expires_in * 1000);
            
            console.log('Looker access token obtained successfully');
            return this.accessToken;
            
        } catch (error) {
            console.error('Failed to get Looker access token:', {
                status: error.response?.status,
                statusText: error.response?.statusText,
                message: error.message
            });
            throw error;
        }
    }

    /**
     * Ensure we have a valid access token
     */
    async ensureValidToken() {
        if (!this.accessToken || Date.now() >= (this.tokenExpiry - 60000)) { // Refresh 1 minute before expiry
            await this.refreshAccessToken();
        }
        return this.accessToken;
    }

    /**
     * Make authenticated API request with retry logic
     */
    async makeApiRequest(endpoint, method = 'GET', data = null, params = null, timeout = 15000, retries = 2) {
        await this.ensureValidToken();
        
        for (let attempt = 1; attempt <= retries + 1; attempt++) {
            try {
                const config = {
                    method: method,
                    url: `${this.baseUrl}/api/4.0${endpoint}`,
                    headers: {
                        'Authorization': `Bearer ${this.accessToken}`,
                        'Content-Type': 'application/json'
                    },
                    timeout: timeout
                };

                if (params && method === 'GET') {
                    config.params = params;
                } else if (data && method !== 'GET') {
                    config.data = data;
                }

                const response = await this.axios(config);
                return response.data;

            } catch (error) {
                console.log(`API request attempt ${attempt} failed:`, error.response?.status || error.message);
                
                if (error.response?.status === 401 && attempt === 1) {
                    // Token might be expired, refresh and retry
                    await this.refreshAccessToken();
                    continue;
                }
                
                if (attempt <= retries && (error.code === 'ECONNABORTED' || error.response?.status >= 500)) {
                    console.log(`Retrying in ${attempt * 2} seconds...`);
                    await new Promise(resolve => setTimeout(resolve, attempt * 2000));
                    continue;
                }
                
                throw error;
            }
        }
    }

    /**
     * Get all projects
     */
    async getAllProjects() {
        try {
            console.log('Fetching all Looker projects...');
            const projects = await this.makeApiRequest('/projects', 'GET', null, null, 30000);
            console.log(`Found ${projects.length} projects`);
            return projects;
        } catch (error) {
            console.error('Failed to get projects:', error.message);
            return [];
        }
    }

    /**
     * Get files from a specific project
     */
    async getProjectFiles(projectName) {
        try {
            console.log(`Fetching files for project: ${projectName}`);
            const files = await this.makeApiRequest(`/projects/${projectName}/files`, 'GET', null, null, 25000);
            console.log(`Found ${files.length} files in ${projectName}`);
            return files;
        } catch (error) {
            console.error(`Failed to get files for project ${projectName}:`, error.message);
            return [];
        }
    }

    /**
     * Get content of a specific file
     */
    async getFileContent(projectName, filePath) {
        try {
            const content = await this.makeApiRequest(
                '/projects/${projectName}/files/file',
                'GET',
                null,
                { file_path: filePath },
                15000
            );
            return content;
        } catch (error) {
            console.error(`Failed to get content for ${filePath}:`, error.response?.status || error.message);
            return null;
        }
    }

    /**
     * Get all LookML files across projects with enhanced filtering
     */
    async getAllLookMLFiles(maxProjects = 3, maxFilesPerProject = 5) {
        try {
            console.log('Fetching LookML files from all projects...');
            const projects = await this.getAllProjects();
            const allLookMLFiles = [];

            // Process limited number of projects to avoid timeouts
            for (const project of projects.slice(0, maxProjects)) {
                try {
                    const files = await this.getProjectFiles(project.name);
                    
                    // Enhanced LookML file detection
                    const lookmlFiles = files.filter(file => this.isLookMLFile(file));
                    
                    console.log(`Found ${lookmlFiles.length} LookML files in ${project.name}:`);
                    lookmlFiles.slice(0, 5).forEach(file => {
                        console.log(`  - ${file.name}`);
                    });

                    // Get content for promising LookML files
                    for (const file of lookmlFiles.slice(0, maxFilesPerProject)) {
                        try {
                            const content = await this.getFileContent(project.name, file.name);
                            
                            if (content && this.validateLookMLContent(content)) {
                                allLookMLFiles.push({
                                    fileName: file.name,
                                    project: project.name,
                                    content: content,
                                    type: this.determineLookMLType(file.name),
                                    size: file.size || 0,
                                    lastModified: file.modified_at
                                });
                                
                                console.log(`Successfully retrieved: ${file.name}`);
                            }
                            
                        } catch (fileError) {
                            console.log(`Could not get content for ${file.name}:`, fileError.message);
                        }
                        
                        // Rate limiting
                        await new Promise(resolve => setTimeout(resolve, 1000));
                    }
                    
                } catch (projectError) {
                    console.log(`Could not process project ${project.name}:`, projectError.message);
                }
                
                // Project-level rate limiting
                await new Promise(resolve => setTimeout(resolve, 2000));
            }

            console.log(`Successfully retrieved ${allLookMLFiles.length} LookML files total`);
            return allLookMLFiles;

        } catch (error) {
            console.error('Error fetching LookML files:', error.message);
            return [];
        }
    }

    /**
     * Enhanced LookML file detection
     */
    isLookMLFile(file) {
        if (file.type !== 'file') return false;
        
        const fileName = file.name.toLowerCase();
        const fileExtension = fileName.split('.').pop();
        
        // Direct LookML extensions
        if (fileName.endsWith('.lkml') || 
            fileName.endsWith('.view.lkml') || 
            fileName.endsWith('.explore.lkml') || 
            fileName.endsWith('.model.lkml') ||
            fileName.endsWith('.dashboard.lkml')) {
            return true;
        }
        
        // Files that might be LookML but with non-standard naming
        if (fileExtension === 'lookml' || fileExtension === 'view' || fileExtension === 'model') {
            return true;
        }
        
        // Check for LookML-like naming patterns
        if (fileName.includes('.view.') || 
            fileName.includes('.explore.') || 
            fileName.includes('.model.') ||
            fileName.includes('lookml')) {
            return true;
        }
        
        return false;
    }

    /**
     * Validate that content is actually LookML
     */
    validateLookMLContent(content) {
        if (!content || typeof content !== 'string') return false;
        
        const contentStr = content.toLowerCase();
        const lookmlKeywords = [
            'view:', 'explore:', 'model:', 'dimension:', 'measure:',
            'sql_table_name:', 'derived_table:', 'join:', 'datagroup:'
        ];
        
        // Must contain at least 2 LookML keywords
        let keywordCount = 0;
        for (const keyword of lookmlKeywords) {
            if (contentStr.includes(keyword)) {
                keywordCount++;
                if (keywordCount >= 2) return true;
            }
        }
        
        return false;
    }

    /**
     * Determine LookML file type from filename
     */
    determineLookMLType(fileName) {
        const name = fileName.toLowerCase();
        
        if (name.includes('.view.') || name.endsWith('.view.lkml')) return 'view';
        if (name.includes('.explore.') || name.endsWith('.explore.lkml')) return 'explore';
        if (name.includes('.model.') || name.endsWith('.model.lkml')) return 'model';
        if (name.includes('.dashboard.') || name.endsWith('.dashboard.lkml')) return 'dashboard';
        if (name.includes('datagroup')) return 'datagroup';
        
        // Fallback: analyze content for type hints
        return 'unknown';
    }

    /**
     * Get user information
     */
    async getCurrentUser() {
        try {
            const user = await this.makeApiRequest('/user');
            return user;
        } catch (error) {
            console.error('Failed to get current user:', error.message);
            return null;
        }
    }

    /**
     * Get all models (alternative to MCP)
     */
    async getAllModels() {
        try {
            console.log('Fetching models via API...');
            const models = await this.makeApiRequest('/lookml_models');
            console.log(`Found ${models.length} models via API`);
            return models;
        } catch (error) {
            console.error('Failed to get models via API:', error.message);
            return [];
        }
    }

    /**
     * Get explores for a specific model (alternative to MCP)
     */
    async getModelExplores(modelName) {
        try {
            console.log(`Fetching explores for model ${modelName} via API...`);
            const model = await this.makeApiRequest(`/lookml_models/${modelName}`);
            return model.explores || [];
        } catch (error) {
            console.error(`Failed to get explores for model ${modelName}:`, error.message);
            return [];
        }
    }

    /**
     * Get dashboards
     */
    async getAllDashboards(fields = 'id,title,description,user_id') {
        try {
            console.log('Fetching dashboards via API...');
            const dashboards = await this.makeApiRequest('/dashboards', 'GET', null, { fields });
            console.log(`Found ${dashboards.length} dashboards`);
            return dashboards;
        } catch (error) {
            console.error('Failed to get dashboards:', error.message);
            return [];
        }
    }

    /**
     * Test API connection
     */
    async testConnection() {
        try {
            const user = await this.getCurrentUser();
            const models = await this.getAllModels();
            
            return {
                success: true,
                message: 'Looker API connection successful',
                user: user?.email || 'Unknown',
                modelsCount: models.length,
                tokenValid: !!this.accessToken
            };
        } catch (error) {
            return {
                success: false,
                message: `Looker API connection failed: ${error.message}`,
                details: {
                    hasToken: !!this.accessToken,
                    baseUrl: this.baseUrl,
                    error: error.response?.status || error.message
                }
            };
        }
    }

    /**
     * Get connection status
     */
    getStatus() {
        return {
            connected: !!this.accessToken,
            tokenExpiry: this.tokenExpiry,
            baseUrl: this.baseUrl,
            tokenValid: this.accessToken && Date.now() < (this.tokenExpiry - 60000)
        };
    }
}

module.exports = { LookerAPIConnector };