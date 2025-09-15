import React, { useState } from 'react';

const EnhancedAIRecommendations = ({ diagnosticResults }) => {
  const [selectedTab, setSelectedTab] = useState('priorities');
  const [analysisModal, setAnalysisModal] = useState(null);
  const [loadingAnalysis, setLoadingAnalysis] = useState(false);

  // Mock data - replace with your actual diagnostic results
  const mockResults = diagnosticResults || {
    aiRecommendations: {
      priorities: [
        {
          priority: 1,
          title: "Address High-Severity Performance Issues",
          description: "Multiple dashboards exhibit slow loading times (over 3 seconds), significantly impacting user experience and productivity.",
          estimatedImpact: "High",
          estimatedEffort: "High", 
          timeframe: "2-4 weeks",
          includesQueryOptimization: true,
          relatedDashboards: [
            { id: "dash_001", title: "Executive KPI Dashboard", url: "https://company.looker.com/dashboards/001" },
            { id: "dash_004", title: "Financial Reporting Suite", url: "https://company.looker.com/dashboards/004" }
          ],
          slowQueries: [
            { id: "query_1", runtime: 8.5, dashboard: "Executive KPI Dashboard" },
            { id: "query_5", runtime: 12.3, dashboard: "Financial Reporting Suite" }
          ]
        },
        {
          priority: 2,
          title: "Resolve Critical LookML Issues",
          description: "Numerous dashboards have identified LookML issues including deprecated field usage and missing dimension descriptions.",
          estimatedImpact: "High",
          estimatedEffort: "Medium",
          timeframe: "1-2 weeks",
          includesQueryOptimization: false,
          relatedDashboards: [
            { id: "dash_002", title: "Sales Performance Analytics", url: "https://company.looker.com/dashboards/002" },
            { id: "dash_003", title: "Marketing Campaign ROI", url: "https://company.looker.com/dashboards/003" }
          ],
          lookmlFiles: [
            { name: "sales_metrics.view", issues: ["deprecated_field_usage", "missing_descriptions"] },
            { name: "marketing_roi.dashboard", issues: ["missing_dimension_description"] }
          ]
        },
        {
          priority: 3,
          title: "Reduce Dashboard Tile Count",
          description: "Many dashboards have a high number of tiles (over 11), making them cumbersome to use and potentially impacting performance.",
          estimatedImpact: "Medium",
          estimatedEffort: "Medium",
          timeframe: "2-3 weeks",
          includesQueryOptimization: false,
          relatedDashboards: [
            { id: "dash_001", title: "Executive KPI Dashboard", url: "https://company.looker.com/dashboards/001", tileCount: 15 },
            { id: "dash_004", title: "Financial Reporting Suite", url: "https://company.looker.com/dashboards/004", tileCount: 20 }
          ]
        }
      ]
    }
  };

  const analyzeQueries = async (priority) => {
    if (!priority.slowQueries || priority.slowQueries.length === 0) return;
    
    setLoadingAnalysis(true);
    try {
      // Call your API endpoint for query analysis
      const response = await fetch('/api/analyze-queries', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ 
          queries: priority.slowQueries,
          dashboards: priority.relatedDashboards 
        })
      });
      
      const analysis = await response.json();
      setAnalysisModal({
        type: 'query',
        title: `Query Analysis - ${priority.title}`,
        content: analysis
      });
    } catch (error) {
      setAnalysisModal({
        type: 'error',
        title: 'Analysis Failed',
        content: { error: 'Failed to analyze queries. Please try again.' }
      });
    } finally {
      setLoadingAnalysis(false);
    }
  };

  const analyzeLookML = async (priority) => {
    if (!priority.lookmlFiles || priority.lookmlFiles.length === 0) return;
    
    setLoadingAnalysis(true);
    try {
      const response = await fetch('/api/analyze-lookml', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ 
          files: priority.lookmlFiles,
          dashboards: priority.relatedDashboards 
        })
      });
      
      const analysis = await response.json();
      setAnalysisModal({
        type: 'lookml',
        title: `LookML Analysis - ${priority.title}`,
        content: analysis
      });
    } catch (error) {
      setAnalysisModal({
        type: 'error',
        title: 'Analysis Failed',
        content: { error: 'Failed to analyze LookML. Please try again.' }
      });
    } finally {
      setLoadingAnalysis(false);
    }
  };

  const PriorityCard = ({ priority }) => (
    <div className="border-l-4 border-purple-500 pl-6 mb-8">
      <div className="flex items-start justify-between mb-3">
        <div className="flex-1">
          <div className="flex items-center space-x-2 mb-2">
            <span className="bg-purple-100 text-purple-800 text-xs font-medium px-2 py-1 rounded-full">
              Priority #{priority.priority}
            </span>
          </div>
          <h4 className="text-lg font-semibold text-gray-800 mb-2">{priority.title}</h4>
          <p className="text-gray-600 mb-3">{priority.description}</p>
        </div>
        
        {/* Action Buttons */}
        <div className="flex flex-col space-y-2 ml-4">
          {priority.includesQueryOptimization && priority.slowQueries && (
            <button
              onClick={() => analyzeQueries(priority)}
              disabled={loadingAnalysis}
              className="bg-blue-500 hover:bg-blue-600 text-white text-xs px-3 py-1 rounded flex items-center space-x-1 disabled:opacity-50"
            >
              <span>🔍</span>
              <span>Query AI</span>
            </button>
          )}
          
          {priority.lookmlFiles && (
            <button
              onClick={() => analyzeLookML(priority)}
              disabled={loadingAnalysis}
              className="bg-green-500 hover:bg-green-600 text-white text-xs px-3 py-1 rounded flex items-center space-x-1 disabled:opacity-50"
            >
              <span>🏗️</span>
              <span>LookML AI</span>
            </button>
          )}
          
          {priority.relatedDashboards && (
            <div className="relative">
              <select 
                className="bg-gray-500 hover:bg-gray-600 text-white text-xs px-2 py-1 rounded cursor-pointer"
                onChange={(e) => {
                  if (e.target.value) {
                    window.open(e.target.value, '_blank');
                    e.target.value = '';
                  }
                }}
                defaultValue=""
              >
                <option value="" disabled>📊 Dashboards</option>
                {priority.relatedDashboards.map(dashboard => (
                  <option key={dashboard.id} value={dashboard.url}>
                    {dashboard.title} {dashboard.tileCount ? `(${dashboard.tileCount} tiles)` : ''}
                  </option>
                ))}
              </select>
            </div>
          )}
        </div>
      </div>
      
      {/* Impact/Effort/Timeline Grid */}
      <div className="grid grid-cols-1 md:grid-cols-3 gap-4 text-sm">
        <div className="bg-green-50 p-3 rounded">
          <span className="font-medium text-green-800">Impact:</span>
          <span className="ml-1 text-green-700">{priority.estimatedImpact}</span>
        </div>
        <div className="bg-blue-50 p-3 rounded">
          <span className="font-medium text-blue-800">Effort:</span>
          <span className="ml-1 text-blue-700">{priority.estimatedEffort}</span>
        </div>
        <div className="bg-orange-50 p-3 rounded">
          <span className="font-medium text-orange-800">Timeline:</span>
          <span className="ml-1 text-orange-700">{priority.timeframe}</span>
        </div>
      </div>
      
      {/* Related Items Summary */}
      {(priority.slowQueries || priority.lookmlFiles || priority.relatedDashboards) && (
        <div className="mt-4 p-3 bg-gray-50 rounded text-sm">
          <div className="flex flex-wrap gap-4">
            {priority.slowQueries && (
              <span className="text-gray-600">
                🐌 {priority.slowQueries.length} slow queries
              </span>
            )}
            {priority.lookmlFiles && (
              <span className="text-gray-600">
                🏗️ {priority.lookmlFiles.length} LookML files
              </span>
            )}
            {priority.relatedDashboards && (
              <span className="text-gray-600">
                📊 {priority.relatedDashboards.length} dashboards affected
              </span>
            )}
          </div>
        </div>
      )}
    </div>
  );

  const AnalysisModal = () => {
    if (!analysisModal) return null;

    return (
      <div className="fixed inset-0 bg-black bg-opacity-50 flex items-center justify-center z-50 p-4">
        <div className="bg-white rounded-lg max-w-4xl w-full max-h-[80vh] overflow-hidden">
          <div className="flex items-center justify-between p-6 border-b">
            <h2 className="text-xl font-semibold">{analysisModal.title}</h2>
            <button 
              onClick={() => setAnalysisModal(null)}
              className="text-gray-400 hover:text-gray-600 text-2xl"
            >
              ×
            </button>
          </div>
          
          <div className="p-6 overflow-y-auto max-h-[60vh]">
            {analysisModal.type === 'error' ? (
              <div className="text-red-600 text-center py-8">
                {analysisModal.content.error}
              </div>
            ) : analysisModal.type === 'query' ? (
              <QueryAnalysisContent analysis={analysisModal.content} />
            ) : (
              <LookMLAnalysisContent analysis={analysisModal.content} />
            )}
          </div>
          
          <div className="flex justify-end p-6 border-t space-x-3">
            <button 
              onClick={() => setAnalysisModal(null)}
              className="px-4 py-2 bg-gray-300 text-gray-700 rounded hover:bg-gray-400"
            >
              Close
            </button>
            <button 
              onClick={() => {
                // Export or save functionality
                navigator.clipboard.writeText(JSON.stringify(analysisModal.content, null, 2));
              }}
              className="px-4 py-2 bg-blue-500 text-white rounded hover:bg-blue-600"
            >
              Copy Analysis
            </button>
          </div>
        </div>
      </div>
    );
  };

  const QueryAnalysisContent = ({ analysis }) => (
    <div className="space-y-6">
      <div className="bg-blue-50 p-4 rounded-lg">
        <h3 className="font-semibold text-blue-800 mb-2">🔍 Query Performance Analysis</h3>
        <p className="text-blue-700">AI-powered analysis of slow-running queries with optimization recommendations.</p>
      </div>
      
      {/* Mock analysis results */}
      <div className="space-y-4">
        <div className="border rounded-lg p-4">
          <div className="flex justify-between items-start mb-3">
            <h4 className="font-medium">Query: large_sales_analysis</h4>
            <span className="bg-red-100 text-red-800 text-xs px-2 py-1 rounded">8.5s runtime</span>
          </div>
          <div className="space-y-3">
            <div>
              <h5 className="font-medium text-sm text-gray-700">Issues Identified:</h5>
              <ul className="text-sm text-gray-600 ml-4 list-disc">
                <li>Missing index on join column (customer_id)</li>
                <li>Inefficient WHERE clause ordering</li>
                <li>SELECT * fetching unnecessary columns</li>
              </ul>
            </div>
            <div>
              <h5 className="font-medium text-sm text-gray-700">AI Recommendations:</h5>
              <ul className="text-sm text-green-700 ml-4 list-disc">
                <li>Add composite index on (customer_id, date_created)</li>
                <li>Specify only required columns in SELECT</li>
                <li>Reorder WHERE conditions by selectivity</li>
              </ul>
            </div>
            <div className="bg-green-50 p-3 rounded text-sm">
              <strong>Expected Improvement:</strong> 60-75% faster execution
            </div>
          </div>
        </div>
      </div>
    </div>
  );

  const LookMLAnalysisContent = ({ analysis }) => (
    <div className="space-y-6">
      <div className="bg-green-50 p-4 rounded-lg">
        <h3 className="font-semibold text-green-800 mb-2">🏗️ LookML Code Analysis</h3>
        <p className="text-green-700">AI-powered analysis of LookML files with best practice recommendations.</p>
      </div>
      
      <div className="space-y-4">
        <div className="border rounded-lg p-4">
          <div className="flex justify-between items-start mb-3">
            <h4 className="font-medium">sales_metrics.view</h4>
            <span className="bg-yellow-100 text-yellow-800 text-xs px-2 py-1 rounded">3 issues</span>
          </div>
          <div className="space-y-3">
            <div>
              <h5 className="font-medium text-sm text-gray-700">Issues Found:</h5>
              <ul className="text-sm text-gray-600 ml-4 list-disc">
                <li>Missing description for revenue_total dimension</li>
                <li>Deprecated sql_table_name syntax</li>
                <li>Inefficient derived table logic</li>
              </ul>
            </div>
            <div>
              <h5 className="font-medium text-sm text-gray-700">Recommended Fixes:</h5>
              <ul className="text-sm text-blue-700 ml-4 list-disc">
                <li>Add comprehensive dimension descriptions</li>
                <li>Update to modern LookML syntax</li>
                <li>Implement incremental PDT for better performance</li>
              </ul>
            </div>
            <div className="bg-blue-50 p-3 rounded text-sm">
              <strong>Best Practice Score:</strong> 7/10 (Good, with room for improvement)
            </div>
          </div>
        </div>
      </div>
    </div>
  );

  return (
    <div className="space-y-8">
      <div className="bg-white rounded-lg shadow">
        <div className="px-6 py-4 border-b border-gray-200">
          <h3 className="text-lg font-semibold text-gray-800 flex items-center space-x-2">
            <span className="text-purple-500">🎯</span>
            <span>Priority Actions (AI Generated)</span>
          </h3>
          <p className="text-sm text-gray-600 mt-1">
            Click action buttons to analyze queries or LookML with AI, or select dashboards to view in Looker.
          </p>
        </div>
        <div className="p-6 space-y-6">
          {mockResults.aiRecommendations.priorities.map((priority, index) => (
            <PriorityCard key={index} priority={priority} />
          ))}
        </div>
      </div>

      {loadingAnalysis && (
        <div className="fixed inset-0 bg-black bg-opacity-25 flex items-center justify-center z-40">
          <div className="bg-white p-6 rounded-lg shadow-lg flex items-center space-x-3">
            <div className="animate-spin rounded-full h-6 w-6 border-b-2 border-blue-500"></div>
            <span>Running AI Analysis...</span>
          </div>
        </div>
      )}

      <AnalysisModal />
    </div>
  );
};

export default EnhancedAIRecommendations;