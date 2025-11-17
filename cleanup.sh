#!/bin/bash

echo "🧹 Cleaning up repository for public sharing..."

# Update package.json with your info
echo "📝 Updating package.json..."
# (You'll need to do this manually with your actual info)

# Complete README
echo "📚 Completing README.md..."
echo -e "\n2. Run integration tests\n3. Open issues on GitHub" >> README.md

# Remove test API keys or sensitive data
echo "🔐 Checking for sensitive data..."
grep -r "api[_-]key" . --exclude-dir=node_modules || echo "No hardcoded API keys found ✅"

echo "✅ Cleanup complete! Review changes before committing."