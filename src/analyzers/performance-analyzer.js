// src/analyzers/performance-analyzer.js
// FIXED: Error accessing performanceRisk.level property

class PerformanceAnalyzer {
    constructor(config = {}) {
        this.config = config;
    }

    analyzeExplores(explores) {
        const analyses = explores.map(explore => this.analyzeExplorePerformance(explore));
        
        return {
            totalExplores: explores.length,
            analyses: analyses,
            summary: this.generatePerformanceSummary(analyses),
            recommendations: this.generatePerformanceRecommendations(analyses)
        };
    }

    analyzeExplorePerformance(explore) {
        return {
            exploreId: `${explore.model}.${explore.name}`,
            name: explore.name,
            model: explore.model,
            label: explore.label,
            
            // Performance metrics
            complexity: this.calculateComplexity(explore),
            joinCount: explore.joinCount || 0,
            fieldCount: explore.fieldCount || 0,
            
            // Performance assessment
            performanceRisk: this.assessPerformanceRisk(explore),
            optimizationPotential: this.assessOptimizationPotential(explore),
            
            // Issues and recommendations
            issues: explore.potentialIssues || [],
            recommendations: this.generateExploreRecommendations(explore),
            
            // Sample queries for testing
            sampleQueries: explore.sampleQueries || []
        };
    }

    calculateComplexity(explore) {
        let complexity = 10; // Base complexity
        
        // Add complexity based on joins
        complexity += (explore.joinCount || 0) * 3;
        
        // Add complexity based on field count
        complexity += Math.floor((explore.fieldCount || 0) / 10) * 2;
        
        // Add complexity based on explore name patterns
        const name = explore.name.toLowerCase();
        if (name.includes('ticket') || name.includes('customer')) complexity += 5;
        if (name.includes('revenue') || name.includes('financial')) complexity += 7;
        if (name.includes('analysis') || name.includes('report')) complexity += 3;
        
        // Add complexity based on potential issues
        complexity += (explore.potentialIssues?.length || 0) * 2;
        
        return Math.min(50, complexity); // Cap at 50
    }

    assessPerformanceRisk(explore) {
        const complexity = this.calculateComplexity(explore);
        const joinCount = explore.joinCount || 0;
        const issueCount = (explore.potentialIssues || []).length;
        
        let riskScore = 0;
        
        // Complexity risk
        if (complexity > 35) riskScore += 30;
        else if (complexity > 25) riskScore += 20;
        else if (complexity > 15) riskScore += 10;
        
        // Join risk
        if (joinCount > 5) riskScore += 25;
        else if (joinCount > 3) riskScore += 15;
        else if (joinCount > 1) riskScore += 5;
        
        // Issue risk
        riskScore += issueCount * 10;
        
        if (riskScore >= 40) return { level: 'high', score: riskScore };
        if (riskScore >= 20) return { level: 'medium', score: riskScore };
        return { level: 'low', score: riskScore };
    }

    assessOptimizationPotential(explore) {
        const complexity = this.calculateComplexity(explore);
        const hasIssues = (explore.potentialIssues || []).length > 0;
        const joinCount = explore.joinCount || 0;
        
        let potential = 0;
        
        if (complexity > 25) potential += 30;
        if (joinCount > 3) potential += 25;
        if (hasIssues) potential += 20;
        
        const opportunities = [];
        
        if (joinCount > 3) {
            opportunities.push('PDT creation for join elimination');
            potential += 20;
        }
        
        if (complexity > 30) {
            opportunities.push('Aggregate table implementation');
            potential += 15;
        }
        
        if (hasIssues) {
            opportunities.push('Query optimization');
            potential += 10;
        }
        
        return {
            score: Math.min(100, potential),
            level: potential >= 50 ? 'high' : potential >= 25 ? 'medium' : 'low',
            opportunities: opportunities,
            estimatedImprovement: this.estimatePerformanceImprovement(potential)
        };
    }

    estimatePerformanceImprovement(potential) {
        if (potential >= 70) return '70-90%';
        if (potential >= 50) return '50-75%';
        if (potential >= 25) return '30-60%';
        return '10-40%';
    }

    generateExploreRecommendations(explore) {
        const recommendations = [];
        const complexity = this.calculateComplexity(explore);
        const joinCount = explore.joinCount || 0;
        
        // PDT recommendations
        if (complexity > 25 || joinCount > 3) {
            recommendations.push({
                type: 'pdt_implementation',
                priority: complexity > 35 ? 'critical' : 'high',
                title: `Create PDT for ${explore.name}`,
                description: 'Pre-compute common aggregations and eliminate runtime joins',
                estimatedImprovement: '60-85% performance improvement',
                effort: 'medium',
                timeframe: '1-2 weeks'
            });
        }
        
        // Caching recommendations
        recommendations.push({
            type: 'caching',
            priority: 'low',
            title: 'Enable query result caching',
            description: 'Cache frequently used query results',
            estimatedImprovement: '50-90% improvement for repeated queries',
            effort: 'low',
            timeframe: '1 hour'
        });
        
        return recommendations;
    }

    calculateHealthMetrics(explores, slowQueries) {
        const metrics = {
            performance: 100,
            governance: 85,
            usage: 80,
            dataQuality: 90,
            security: 90
        };
        
        // Adjust performance based on slow queries
        if (slowQueries.length > 20) {
            metrics.performance -= 30;
        } else if (slowQueries.length > 10) {
            metrics.performance -= 20;
        } else if (slowQueries.length > 5) {
            metrics.performance -= 10;
        }
        
        // Adjust performance based on explore complexity
        if (explores.length > 0) {
            const avgComplexity = explores.reduce((sum, e) => sum + this.calculateComplexity(e), 0) / explores.length;
            
            if (avgComplexity > 30) {
                metrics.performance -= 25;
            } else if (avgComplexity > 25) {
                metrics.performance -= 15;
            } else if (avgComplexity > 20) {
                metrics.performance -= 10;
            }
        }
        
        return metrics;
    }

    generatePerformanceSummary(analyses) {
        const total = analyses.length;
        const highRisk = analyses.filter(a => a.performanceRisk.level === 'high').length;
        const mediumRisk = analyses.filter(a => a.performanceRisk.level === 'medium').length;
        const lowRisk = analyses.filter(a => a.performanceRisk.level === 'low').length;
        
        const highOptimization = analyses.filter(a => a.optimizationPotential.level === 'high').length;
        const avgComplexity = total > 0 
            ? analyses.reduce((sum, a) => sum + a.complexity, 0) / total 
            : 0;
        
        const totalRecommendations = analyses.reduce((sum, a) => sum + a.recommendations.length, 0);
        
        return {
            totalExplores: total,
            performanceRisk: {
                high: highRisk,
                medium: mediumRisk,
                low: lowRisk,
                distribution: {
                    high: total > 0 ? Math.round((highRisk / total) * 100) : 0,
                    medium: total > 0 ? Math.round((mediumRisk / total) * 100) : 0,
                    low: total > 0 ? Math.round((lowRisk / total) * 100) : 0
                }
            },
            optimizationOpportunities: {
                high: highOptimization,
                total: totalRecommendations,
                avgPerExplore: total > 0 ? Math.round(totalRecommendations / total * 10) / 10 : 0
            },
            complexity: {
                average: Math.round(avgComplexity * 10) / 10,
                distribution: {
                    low: analyses.filter(a => a.complexity < 15).length,
                    medium: analyses.filter(a => a.complexity >= 15 && a.complexity < 30).length,
                    high: analyses.filter(a => a.complexity >= 30).length
                }
            }
        };
    }

    generatePerformanceRecommendations(analyses) {
        const highRiskExplores = analyses.filter(a => a.performanceRisk && a.performanceRisk.level === 'high');
        const highOptimizationPotential = analyses.filter(a => a.optimizationPotential && a.optimizationPotential.level === 'high');
        
        const recommendations = [];
        
        // Critical performance issues
        if (highRiskExplores.length > 0) {
            recommendations.push({
                priority: 1,
                category: 'Critical Performance',
                title: `Address ${highRiskExplores.length} High-Risk Explores`,
                description: `Explores with high performance risk require immediate attention`,
                explores: highRiskExplores.slice(0, 5).map(e => e.exploreId),
                estimatedImpact: 'High',
                timeframe: '1-2 weeks'
            });
        }
        
        // High optimization potential
        if (highOptimizationPotential.length > 0) {
            recommendations.push({
                priority: 2,
                category: 'Optimization Opportunities', 
                title: `Optimize ${highOptimizationPotential.length} High-Potential Explores`,
                description: `Significant performance gains available through PDTs and aggregation`,
                explores: highOptimizationPotential.slice(0, 5).map(e => e.exploreId),
                estimatedImpact: 'High',
                timeframe: '2-4 weeks'
            });
        }
        
        return recommendations;
    }

    // FIXED: Added null checks for performanceRisk property
    calculatePerformanceGrade(analyses, slowQueries) {
        const avgComplexity = analyses.length > 0 
            ? analyses.reduce((sum, a) => sum + a.complexity, 0) / analyses.length 
            : 0;
        
        // FIXED: Added null check for performanceRisk.level
        const highRiskCount = analyses.filter(a => a.performanceRisk && a.performanceRisk.level === 'high').length;
        const slowQueryCount = slowQueries.length;
        
        let gradeScore = 100;
        
        // Deduct for complexity
        if (avgComplexity > 30) gradeScore -= 25;
        else if (avgComplexity > 25) gradeScore -= 15;
        else if (avgComplexity > 20) gradeScore -= 10;
        
        // Deduct for high risk explores
        gradeScore -= highRiskCount * 5;
        
        // Deduct for slow queries
        if (slowQueryCount > 20) gradeScore -= 20;
        else if (slowQueryCount > 10) gradeScore -= 10;
        
        // Convert to letter grade
        if (gradeScore >= 90) return 'A';
        if (gradeScore >= 80) return 'B';
        if (gradeScore >= 70) return 'C';
        if (gradeScore >= 60) return 'D';
        return 'F';
    }
}

module.exports = { PerformanceAnalyzer };