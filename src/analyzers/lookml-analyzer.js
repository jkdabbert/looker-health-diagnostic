// src/analyzers/lookml-analyzer.js
// LookML parsing and analysis extracted from diagnostic-engine.js

class LookMLAnalyzer {
    constructor(config = {}) {
        this.config = config;
    }

    /**
     * Analyze LookML files for structure and optimization opportunities
     */
    async analyzeLookMLFiles(files) {
        const analyses = [];
        
        for (const file of files) {
            const analysis = this.analyzeLookMLFile(file);
            analyses.push(analysis);
        }

        return {
            totalFiles: analyses.length,
            analyses: analyses,
            summary: this.generateLookMLSummary(analyses)
        };
    }

    /**
     * Analyze individual LookML file
     */
    analyzeLookMLFile(file) {
        return {
            fileName: file.fileName,
            project: file.project,
            type: file.type,
            
            // Structure analysis
            structure: {
                joins: file.joins || [],
                dimensions: file.dimensions || [],
                measures: file.measures || [],
                explores: file.explores || []
            },
            
            // Complexity metrics
            complexity: this.calculateLookMLComplexity(file),
            
            // Issues identification
            issues: this.identifyLookMLIssues(file),
            
            // Optimization recommendations
            recommendations: this.generateLookMLRecommendations(file)
        };
    }

    /**
     * Parse LookML content from string
     */
    parseLookMLContent(content, fileName, projectName) {
        try {
            const fileType = this.determineFileType(fileName);
            
            const parsed = {
                fileName: fileName,
                project: projectName,
                type: fileType,
                content: content,
                joins: [],
                dimensions: [],
                measures: [],
                explores: []
            };
            
            if (fileType === 'view') {
                parsed.joins = this.extractJoinsFromLookML(content);
                parsed.dimensions = this.extractDimensionsFromLookML(content);
                parsed.measures = this.extractMeasuresFromLookML(content);
            } else if (fileType === 'explore') {
                parsed.explores = this.extractExploresFromLookML(content);
                parsed.joins = this.extractExploreJoinsFromLookML(content);
            } else if (fileType === 'model') {
                parsed.explores = this.extractExploresFromLookML(content);
            }
            
            return parsed;
            
        } catch (error) {
            console.log(`Error parsing LookML file ${fileName}:`, error.message);
            return null;
        }
    }

    /**
     * Determine LookML file type
     */
    determineFileType(fileName) {
        if (fileName.endsWith('.view.lkml')) return 'view';
        if (fileName.endsWith('.explore.lkml')) return 'explore';
        if (fileName.endsWith('.model.lkml')) return 'model';
        if (fileName.endsWith('.dashboard.lkml')) return 'dashboard';
        return 'unknown';
    }

    /**
     * Detect if content is LookML
     */
    isLookMLContent(content) {
        if (!content || typeof content !== 'string') return false;
        
        const contentLower = content.toLowerCase();
        
        const lookmlIndicators = [
            'view:', 'explore:', 'model:', 'dashboard:',
            'dimension:', 'measure:', 'filter:',
            'sql_table_name:', 'derived_table:',
            'join:', 'relationship:', 'sql_on:',
            'type: string', 'type: number', 'type: date',
            'drill_fields:', 'label:', 'description:',
            '${TABLE}', 'sql:', 'html:',
            'datagroup:', 'connection:'
        ];
        
        let indicatorCount = 0;
        for (const indicator of lookmlIndicators) {
            if (contentLower.includes(indicator)) {
                indicatorCount++;
            }
        }
        
        return indicatorCount >= 3;
    }

    /**
     * Extract joins from LookML content
     */
    extractJoinsFromLookML(content) {
        const joins = [];
        const joinRegex = /join:\s*(\w+)\s*\{([^{}]*(?:\{[^{}]*\}[^{}]*)*)\}/g;
        let match;
        
        while ((match = joinRegex.exec(content)) !== null) {
            const joinName = match[1];
            const joinBlock = match[2];
            
            const join = {
                name: joinName,
                type: this.extractJoinType(joinBlock),
                relationship: this.extractJoinRelationship(joinBlock),
                sql_on: this.extractJoinSqlOn(joinBlock),
                foreign_key: this.extractJoinForeignKey(joinBlock)
            };
            
            joins.push(join);
        }
        
        return joins;
    }

    extractJoinType(joinBlock) {
        const typeMatch = joinBlock.match(/type:\s*(\w+)/);
        return typeMatch ? typeMatch[1] : 'left_outer';
    }

    extractJoinRelationship(joinBlock) {
        const relationshipMatch = joinBlock.match(/relationship:\s*(\w+)/);
        return relationshipMatch ? relationshipMatch[1] : 'many_to_one';
    }

    extractJoinSqlOn(joinBlock) {
        const sqlOnMatch = joinBlock.match(/sql_on:\s*([^;]+);/);
        return sqlOnMatch ? sqlOnMatch[1].trim() : '';
    }

    extractJoinForeignKey(joinBlock) {
        const foreignKeyMatch = joinBlock.match(/foreign_key:\s*([^}]+)/);
        return foreignKeyMatch ? foreignKeyMatch[1].trim() : '';
    }

    /**
     * Extract dimensions from LookML content
     */
    extractDimensionsFromLookML(content) {
        const dimensions = [];
        const dimensionRegex = /dimension:\s*(\w+)\s*\{([^{}]*(?:\{[^{}]*\}[^{}]*)*)\}/g;
        let match;
        
        while ((match = dimensionRegex.exec(content)) !== null) {
            const dimensionName = match[1];
            const dimensionBlock = match[2];
            
            dimensions.push({
                name: dimensionName,
                type: this.extractDimensionType(dimensionBlock),
                sql: this.extractDimensionSql(dimensionBlock),
                label: this.extractFieldLabel(dimensionBlock),
                description: this.extractFieldDescription(dimensionBlock)
            });
        }
        
        return dimensions;
    }

    extractDimensionType(dimensionBlock) {
        const typeMatch = dimensionBlock.match(/type:\s*(\w+)/);
        return typeMatch ? typeMatch[1] : 'string';
    }

    extractDimensionSql(dimensionBlock) {
        const sqlMatch = dimensionBlock.match(/sql:\s*([^;]+);/);
        return sqlMatch ? sqlMatch[1].trim() : '';
    }

    /**
     * Extract measures from LookML content
     */
    extractMeasuresFromLookML(content) {
        const measures = [];
        const measureRegex = /measure:\s*(\w+)\s*\{([^{}]*(?:\{[^{}]*\}[^{}]*)*)\}/g;
        let match;
        
        while ((match = measureRegex.exec(content)) !== null) {
            const measureName = match[1];
            const measureBlock = match[2];
            
            measures.push({
                name: measureName,
                type: this.extractMeasureType(measureBlock),
                sql: this.extractMeasureSql(measureBlock),
                label: this.extractFieldLabel(measureBlock),
                description: this.extractFieldDescription(measureBlock)
            });
        }
        
        return measures;
    }

    extractMeasureType(measureBlock) {
        const typeMatch = measureBlock.match(/type:\s*(\w+)/);
        return typeMatch ? typeMatch[1] : 'count';
    }

    extractMeasureSql(measureBlock) {
        const sqlMatch = measureBlock.match(/sql:\s*([^;]+);/);
        return sqlMatch ? sqlMatch[1].trim() : '';
    }

    extractFieldLabel(fieldBlock) {
        const labelMatch = fieldBlock.match(/label:\s*"([^"]+)"/);
        return labelMatch ? labelMatch[1] : '';
    }

    extractFieldDescription(fieldBlock) {
        const descMatch = fieldBlock.match(/description:\s*"([^"]+)"/);
        return descMatch ? descMatch[1] : '';
    }

    /**
     * Extract explores from LookML content
     */
    extractExploresFromLookML(content) {
        const explores = [];
        const exploreRegex = /explore:\s*(\w+)\s*\{([^{}]*(?:\{[^{}]*\}[^{}]*)*)\}/g;
        let match;
        
        while ((match = exploreRegex.exec(content)) !== null) {
            explores.push({
                name: match[1],
                content: match[2],
                label: this.extractExploreLabel(match[2]),
                description: this.extractExploreDescription(match[2])
            });
        }
        
        return explores;
    }

    extractExploreLabel(exploreBlock) {
        const labelMatch = exploreBlock.match(/label:\s*"([^"]+)"/);
        return labelMatch ? labelMatch[1] : '';
    }

    extractExploreDescription(exploreBlock) {
        const descMatch = exploreBlock.match(/description:\s*"([^"]+)"/);
        return descMatch ? descMatch[1] : '';
    }

    extractExploreJoinsFromLookML(content) {
        const joins = [];
        const joinRegex = /join:\s*(\w+)\s*\{([^{}]*(?:\{[^{}]*\}[^{}]*)*)\}/g;
        let match;
        
        while ((match = joinRegex.exec(content)) !== null) {
            joins.push({
                name: match[1],
                exploreLevel: true,
                content: match[2]
            });
        }
        
        return joins;
    }

    /**
     * Calculate LookML complexity score
     */
    calculateLookMLComplexity(file) {
        let complexity = 0;
        
        // Join complexity
        complexity += (file.joins?.length || 0) * 3;
        
        // Field complexity
        complexity += (file.dimensions?.length || 0) * 0.5;
        complexity += (file.measures?.length || 0) * 1;
        
        // Explore complexity
        complexity += (file.explores?.length || 0) * 2;
        
        // SQL complexity (if available)
        if (file.content) {
            complexity += this.calculateSQLComplexityInLookML(file.content);
        }
        
        return Math.min(complexity, 50); // Cap at 50
    }

    calculateSQLComplexityInLookML(content) {
        let sqlComplexity = 0;
        
        // Count complex SQL patterns
        sqlComplexity += (content.match(/CASE\s+WHEN/gi) || []).length * 2;
        sqlComplexity += (content.match(/COALESCE/gi) || []).length * 1;
        sqlComplexity += (content.match(/SUBSTRING/gi) || []).length * 1;
        sqlComplexity += (content.match(/REGEXP/gi) || []).length * 2;
        
        return sqlComplexity;
    }

    /**
     * Identify issues in LookML files
     */
    identifyLookMLIssues(file) {
        const issues = [];
        
        // Missing documentation
        if (file.type === 'view') {
            const dimensionsWithoutDesc = (file.dimensions || []).filter(d => !d.description).length;
            const measuresWithoutDesc = (file.measures || []).filter(m => !m.description).length;
            
            if (dimensionsWithoutDesc > 0) {
                issues.push({
                    type: 'documentation',
                    severity: 'low',
                    issue: `${dimensionsWithoutDesc} dimensions missing descriptions`,
                    recommendation: 'Add descriptions to improve user experience'
                });
            }
            
            if (measuresWithoutDesc > 0) {
                issues.push({
                    type: 'documentation',
                    severity: 'medium',
                    issue: `${measuresWithoutDesc} measures missing descriptions`,
                    recommendation: 'Add descriptions to measures for clarity'
                });
            }
        }
        
        // Complex joins
        const joinCount = (file.joins || []).length;
        if (joinCount > 5) {
            issues.push({
                type: 'performance',
                severity: 'high',
                issue: `View has ${joinCount} joins which may impact performance`,
                recommendation: 'Consider breaking into multiple views or using PDTs'
            });
        }
        
        // Large number of fields
        const fieldCount = (file.dimensions || []).length + (file.measures || []).length;
        if (fieldCount > 50) {
            issues.push({
                type: 'usability',
                severity: 'medium',
                issue: `View has ${fieldCount} fields which may overwhelm users`,
                recommendation: 'Consider organizing fields into groups or hiding less common ones'
            });
        }
        
        return issues;
    }

    /**
     * Generate LookML optimization recommendations
     */
    generateLookMLRecommendations(file) {
        const recommendations = [];
        
        // PDT recommendations
        if (file.type === 'view' && (file.joins || []).length > 3) {
            recommendations.push({
                type: 'pdt_creation',
                priority: 'high',
                action: `Create PDT for ${file.fileName} to pre-compute joins`,
                benefit: 'Eliminate runtime joins and improve query performance',
                effort: 'medium'
            });
        }
        
        // Aggregate table recommendations
        if (file.type === 'view' && (file.measures || []).length > 10) {
            recommendations.push({
                type: 'aggregate_table',
                priority: 'medium',
                action: 'Create aggregate table for common measure combinations',
                benefit: 'Faster aggregation queries',
                effort: 'high'
            });
        }
        
        // Documentation improvements
        const undocumentedFields = [
            ...(file.dimensions || []).filter(d => !d.description),
            ...(file.measures || []).filter(m => !m.description)
        ].length;
        
        if (undocumentedFields > 0) {
            recommendations.push({
                type: 'documentation',
                priority: 'low',
                action: `Add descriptions to ${undocumentedFields} fields`,
                benefit: 'Improved user experience and adoption',
                effort: 'low'
            });
        }
        
        return recommendations;
    }

    /**
     * Generate optimized LookML suggestions
     */
    generateOptimizedLookML(file) {
        if (file.type !== 'view' || !file.joins || file.joins.length === 0) {
            return null;
        }
        
        const pdtName = `pdt_${file.fileName.replace('.view.lkml', '')}_optimized`;
        
        return {
            pdtDefinition: this.generatePDTDefinition(file, pdtName),
            optimizedView: this.generateOptimizedView(file, pdtName),
            datagroup: this.generateDatagroupDefinition(file)
        };
    }

    generatePDTDefinition(file, pdtName) {
        const tableName = file.fileName.replace('.view.lkml', '');
        
        return `view: ${pdtName} {
  derived_table: {
    sql: SELECT 
      ${this.generateSelectClause(file)},
      COUNT(*) as record_count,
      MAX(updated_at) as last_updated
    FROM ${tableName} base
    ${this.generateJoinClauses(file.joins)}
    WHERE base.created_at >= DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)
    GROUP BY ${this.generateGroupByClause(file)} ;;
    
    datagroup_trigger: ${tableName}_datagroup
    distribution_style: even
    sortkeys: ["created_date"]
  }
  
  ${this.generateOptimizedDimensions(file)}
  ${this.generateOptimizedMeasures(file)}
}`;
    }

    generateSelectClause(file) {
        const keyDimensions = (file.dimensions || [])
            .filter(d => d.type !== 'string' || d.name.includes('id') || d.name.includes('key'))
            .slice(0, 5)
            .map(d => `base.${d.name}`)
            .join(', ');
        
        return keyDimensions || 'base.id, base.created_at';
    }

    generateJoinClauses(joins) {
        return joins.map(join => 
            `LEFT JOIN ${join.name} ON ${join.sql_on || `base.${join.name}_id = ${join.name}.id`}`
        ).join('\n    ');
    }

    generateGroupByClause(file) {
        const keyDimensions = (file.dimensions || [])
            .filter(d => d.type !== 'string' || d.name.includes('id') || d.name.includes('key'))
            .slice(0, 5)
            .map((d, i) => i + 1)
            .join(', ');
        
        return keyDimensions || '1, 2';
    }

    generateOptimizedDimensions(file) {
        return (file.dimensions || []).slice(0, 10).map(dim => `
  dimension: ${dim.name} {
    type: ${dim.type}
    sql: \${TABLE}.${dim.name} ;;
    ${dim.label ? `label: "${dim.label}"` : ''}
    ${dim.description ? `description: "${dim.description}"` : ''}
  }`).join('\n');
    }

    generateOptimizedMeasures(file) {
        return (file.measures || []).map(measure => `
  measure: ${measure.name} {
    type: ${measure.type}
    sql: \${TABLE}.${measure.name} ;;
    ${measure.label ? `label: "${measure.label}"` : ''}
    ${measure.description ? `description: "${measure.description}"` : ''}
  }`).join('\n');
    }

    generateOptimizedView(file, pdtName) {
        const baseName = file.fileName.replace('.view.lkml', '');
        
        return `explore: ${baseName}_optimized {
  view_name: ${pdtName}
  label: "${baseName} (Optimized)"
  description: "High-performance version with pre-computed joins and aggregations"
  
  always_filter: {
    filters: [${pdtName}.created_date: "30 days"]
  }
}`;
    }

    generateDatagroupDefinition(file) {
        const tableName = file.fileName.replace('.view.lkml', '');
        
        return `datagroup: ${tableName}_datagroup {
  sql_trigger: SELECT MAX(updated_at) FROM ${tableName} ;;
  max_cache_age: "4 hours"
  description: "Triggers when ${tableName} data is updated"
}`;
    }

    /**
     * Generate summary of LookML analysis
     */
    generateLookMLSummary(analyses) {
        const totalFiles = analyses.length;
        const byType = {
            view: analyses.filter(a => a.type === 'view').length,
            explore: analyses.filter(a => a.type === 'explore').length,
            model: analyses.filter(a => a.type === 'model').length,
            dashboard: analyses.filter(a => a.type === 'dashboard').length
        };

        const totalJoins = analyses.reduce((sum, a) => sum + (a.structure.joins.length || 0), 0);
        const totalDimensions = analyses.reduce((sum, a) => sum + (a.structure.dimensions.length || 0), 0);
        const totalMeasures = analyses.reduce((sum, a) => sum + (a.structure.measures.length || 0), 0);

        const avgComplexity = totalFiles > 0 
            ? analyses.reduce((sum, a) => sum + a.complexity, 0) / totalFiles 
            : 0;

        const highComplexityFiles = analyses.filter(a => a.complexity > 30).length;
        const totalIssues = analyses.reduce((sum, a) => sum + a.issues.length, 0);

        return {
            totalFiles,
            fileTypes: byType,
            structure: {
                totalJoins,
                totalDimensions,
                totalMeasures,
                avgFieldsPerFile: totalFiles > 0 ? Math.round((totalDimensions + totalMeasures) / totalFiles) : 0
            },
            complexity: {
                average: Math.round(avgComplexity * 10) / 10,
                highComplexityFiles,
                distribution: {
                    low: analyses.filter(a => a.complexity < 15).length,
                    medium: analyses.filter(a => a.complexity >= 15 && a.complexity < 30).length,
                    high: analyses.filter(a => a.complexity >= 30).length
                }
            },
            issues: {
                total: totalIssues,
                byType: this.groupIssuesByType(analyses),
                filesWithIssues: analyses.filter(a => a.issues.length > 0).length
            },
            recommendations: {
                total: analyses.reduce((sum, a) => sum + a.recommendations.length, 0),
                pdtCandidates: analyses.filter(a => 
                    a.recommendations.some(r => r.type === 'pdt_creation')
                ).length
            }
        };
    }

    groupIssuesByType(analyses) {
        const issueTypes = {};
        
        analyses.forEach(analysis => {
            analysis.issues.forEach(issue => {
                if (!issueTypes[issue.type]) {
                    issueTypes[issue.type] = 0;
                }
                issueTypes[issue.type]++;
            });
        });

        return issueTypes;
    }
}

module.exports = { LookMLAnalyzer };