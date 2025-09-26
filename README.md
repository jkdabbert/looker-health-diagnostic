# Looker Health Diagnostic Assistant

A comprehensive diagnostic tool for analyzing Looker instance performance, identifying slow queries, and providing AI-powered optimization recommendations.

## Features

- **Two-Phase Diagnostic Workflow**: Fast scanning followed by detailed AI analysis
- **AI-Powered Query Analysis**: Uses Gemini AI to analyze SQL and provide optimization recommendations
- **SQL Optimization Suggestions**: Shows before/after SQL comparisons with specific improvements
- **LookML Code Generation**: Generates PDT (Persistent Derived Table) implementations
- **API Testing Dashboard**: Test and debug Looker API calls and MCP tools
- **Interactive Dashboard**: Modern React-based interface with tabbed navigation

## Project Structure

```
looker-health-diagnostic/
├── dashboard.html          # Main HTML file - entry point
├── components.js           # React components and UI elements
├── main-app.js            # Application logic and main components
└── README.md              # This file
```

## File Breakdown

### dashboard.html (Main Entry Point)
- HTML structure with React, Babel, and Tailwind CSS imports
- Minimal file that loads the JavaScript components
- Entry point for the application

### components.js (UI Components)
- Icon components for the interface
- SQLComparisonCard component for displaying AI analysis results
- Helper functions for severity colors and visual indicators
- Reusable UI elements

### main-app.js (Application Logic)
- TwoPhaseDiagnosticTab component for the main workflow
- APITestingTab component for testing Looker APIs
- Main LookerHealthDashboard component
- Application state management and initialization

## Installation & Setup

### Prerequisites
- A web server (local or remote) to serve the files
- Access to a Looker instance with API credentials
- Backend API endpoints for diagnostic functionality

### Quick Start
1. Clone or download all three files to the same directory
2. Ensure your backend API is running with the required endpoints
3. Open `dashboard.html` in a web browser
4. The application will load automatically

### Backend API Requirements
The frontend expects these API endpoints to be available:

```
POST /api/diagnostic/scan              # Fast performance scan
POST /api/diagnostic/analyze-batch     # AI analysis of selected queries
POST /api/diagnostic/run              # Full diagnostic run
POST /api/test-mcp-tool               # MCP tool testing
POST /api/test-api                    # Direct API testing
```

## Usage

### Two-Phase Diagnostic Workflow

#### Phase 1: Fast Scan
1. Click "Run Fast Scan" to quickly identify slow queries
2. Review the summary cards showing overall performance metrics
3. Examine the list of slow queries with runtime categorization

#### Phase 2: AI Analysis
1. Select queries from the scan results using checkboxes
2. Use quick selection buttons (Top 3, Top 5, Critical Only)
3. Click "Analyze Selected" to run AI-powered analysis
4. Review detailed results for each query including:
   - Issues identified with severity levels
   - Optimization recommendations
   - SQL comparison (original vs optimized)
   - Generated LookML code for PDTs

### API Testing
1. Navigate to the "API Testing" tab
2. Choose between MCP Tool Call or Direct API Call
3. Use predefined test buttons or create custom tests
4. Enter JSON arguments and execute tests
5. Review response data and timing information

## AI Analysis Features

### Issue Detection
- **Performance Issues**: Identifies slow operations and inefficient patterns
- **Severity Levels**: Critical, High, Medium, Low with color coding
- **Specific Recommendations**: Actionable fixes for each issue

### SQL Optimization
- **Before/After Comparison**: Side-by-side view of original and optimized SQL
- **Optimization Comments**: Inline explanations of improvements
- **Performance Estimates**: Expected runtime improvements

### LookML Generation
- **PDT Implementation**: Complete view files for persistent derived tables
- **Implementation Steps**: Step-by-step deployment instructions
- **Performance Gains**: Expected 85-95% speed improvements

## Configuration

### Backend Integration
Update the API endpoints in the JavaScript files to match your backend:

```javascript
// In main-app.js
const response = await fetch('/api/diagnostic/scan', {
  method: 'POST',
  headers: { 'Content-Type': 'application/json' }
});
```

### Styling Customization
The interface uses Tailwind CSS. Modify classes in the components to customize:
- Color schemes
- Layout spacing
- Component sizing
- Visual indicators

## Browser Compatibility

- **Recommended**: Chrome, Firefox, Safari, Edge (latest versions)
- **Requirements**: ES6+ support, React 18 compatibility
- **Mobile**: Responsive design works on tablets and mobile devices

## Development

### Adding New Components
1. Add component to `components.js`
2. Import and use in `main-app.js`
3. Update navigation if needed

### Extending API Testing
Add new predefined tests to the `predefinedTests` object in `APITestingTab`:

```javascript
new_test: {
  testType: 'mcp',
  toolName: 'your_tool_name',
  arguments: JSON.stringify({...})
}
```

## Troubleshooting

### Common Issues

**Dashboard doesn't load**
- Check that all three files are in the same directory
- Verify web server is serving static files correctly
- Check browser console for JavaScript errors

**API calls fail**
- Verify backend API endpoints are running
- Check CORS configuration on the backend
- Confirm API URLs match your backend setup

**AI analysis not showing results**
- Check browser console for error messages
- Verify the backend AI integration is working
- Confirm the analysis response format matches expected structure

### Debug Mode
Enable console logging by opening browser developer tools. The application logs:
- API responses
- Analysis results
- Error messages
- Component state changes

## Contributing

### Code Style
- Use consistent indentation (2 spaces)
- Add comments for complex logic
- Follow React functional component patterns
- Use descriptive variable names

### Testing
- Test all API endpoints manually
- Verify UI responsiveness across devices
- Check error handling for edge cases
- Validate AI analysis display with various response formats

## License

This project is developed for the Looker Hackathon 2025.

## Support

For issues or questions:
1. Check the tro