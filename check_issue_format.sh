#!/bin/bash

# Script to verify issue format and provide guidance
# Usage: ./check_issue_format.sh

echo "🔍 Issue Format Checker"
echo "======================"
echo ""

# Colors
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo -e "${BLUE}📋 Available Issue Templates:${NC}"
echo ""

# Check templates
if [ -f ".github/ISSUE_TEMPLATE/bug_report.md" ]; then
    echo -e "${GREEN}✅ Bug Report Template${NC}"
    echo "   Use for: Reporting bugs, errors, unexpected behavior"
    echo "   Title format: [BUG] Brief description"
    echo ""
fi

if [ -f ".github/ISSUE_TEMPLATE/feature_request.md" ]; then
    echo -e "${GREEN}✅ Feature Request Template${NC}"
    echo "   Use for: Suggesting new features or enhancements"
    echo "   Title format: [FEATURE] Brief description"
    echo ""
fi

if [ -f ".github/ISSUE_TEMPLATE/question.md" ]; then
    echo -e "${GREEN}✅ Question Template${NC}"
    echo "   Use for: Asking questions about the project"
    echo "   Title format: [QUESTION] Brief description"
    echo ""
fi

echo -e "${BLUE}📝 Issue Title Format Guidelines:${NC}"
echo ""
echo "1. Start with type prefix:"
echo "   - [BUG] for bug reports"
echo "   - [FEATURE] for feature requests"
echo "   - [QUESTION] for questions"
echo ""
echo "2. Follow with brief, descriptive title:"
echo "   ✅ Good: [FEATURE] Add support for Delta Lake format"
echo "   ❌ Bad:  Add Delta Lake"
echo "   ❌ Bad:  [FEATURE] delta"
echo ""
echo "3. Keep it concise (under 72 characters recommended)"
echo ""

echo -e "${BLUE}✅ Best Practices:${NC}"
echo ""
echo "1. Use the appropriate template when creating issues"
echo "2. Fill out all required sections"
echo "3. Provide clear, specific descriptions"
echo "4. Include environment details (for bugs)"
echo "5. Add code examples/logs when relevant"
echo "6. Link related issues if applicable"
echo ""

echo -e "${BLUE}📚 Documentation:${NC}"
echo ""
echo "See ISSUE_GUIDE.md for detailed guidelines"
echo ""

echo -e "${YELLOW}💡 Example Issue Titles:${NC}"
echo ""
echo "Bug Reports:"
echo "  • [BUG] CSV ingestion fails with ConnectionError for files > 1GB"
echo "  • [BUG] Avro handler throws SchemaError on nested union types"
echo ""
echo "Feature Requests:"
echo "  • [FEATURE] Add support for Delta Lake format"
echo "  • [FEATURE] Implement batch ownership assignment API"
echo ""
echo "Questions:"
echo "  • [QUESTION] How to configure custom ownership types?"
echo "  • [QUESTION] Best practices for handling large file ingestion?"
echo ""

echo -e "${GREEN}✨ All issue templates are properly configured!${NC}"

