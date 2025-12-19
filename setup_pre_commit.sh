#!/bin/bash

# Setup script for pre-commit hooks
# Usage: ./setup_pre_commit.sh

set -e

echo "🚀 Setting up pre-commit hooks for Lumos Framework..."
echo ""

# Check if Python is available
if ! command -v python3 &> /dev/null; then
    echo "❌ Python 3 is not installed. Please install Python 3.8+ first."
    exit 1
fi

# Check Python version (simplified check)
python_major=$(python3 --version | cut -d' ' -f2 | cut -d'.' -f1)
python_minor=$(python3 --version | cut -d' ' -f2 | cut -d'.' -f2)

if [ "$python_major" -lt 3 ] || ([ "$python_major" -eq 3 ] && [ "$python_minor" -lt 8 ]); then
    echo "❌ Python 3.8+ is required. Found: $(python3 --version)"
    exit 1
fi

echo "✅ Python version: $(python3 --version)"
echo ""

# Install development dependencies
echo "📦 Installing development dependencies..."
pip install -r requirements-dev.txt

echo ""
echo "🔧 Installing pre-commit hooks..."

# Install pre-commit hooks
pre-commit install

echo ""
echo "✅ Pre-commit hooks installed successfully!"
echo ""
echo "📋 Running initial check on all files..."
pre-commit run --all-files || true

echo ""
echo "✨ Setup complete!"
echo ""
echo "📖 Next steps:"
echo "   1. Create a branch linked to an issue: git checkout -b feature/19-your-feature-name"
echo "   2. Make your changes"
echo "   3. Commit: git commit -m 'feat(scope): your message (#19)'"
echo "   4. Pre-commit hooks will run automatically before each commit"
echo ""
echo "💡 Tips:"
echo "   - Run 'pre-commit run --all-files' to check all files manually"
echo "   - See SETUP_PRE_COMMIT.md for detailed documentation"
echo ""

