#!/bin/bash

# Test script to verify code quality standards
set -e

echo "🧪 Testing Code Quality Standards"
echo "=================================="
echo ""

# Colors
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

PASSED=0
FAILED=0

test_check() {
    if [ $? -eq 0 ]; then
        echo -e "${GREEN}✅ PASS:${NC} $1"
        ((PASSED++))
    else
        echo -e "${RED}❌ FAIL:${NC} $1"
        ((FAILED++))
    fi
}

# Test 1: Check if .pre-commit-config.yaml exists
echo "1. Checking configuration files..."
[ -f ".pre-commit-config.yaml" ] && test_check ".pre-commit-config.yaml exists" || echo -e "${RED}❌ FAIL:${NC} .pre-commit-config.yaml missing"

# Test 2: Check if pyproject.toml exists
[ -f "pyproject.toml" ] && test_check "pyproject.toml exists" || echo -e "${RED}❌ FAIL:${NC} pyproject.toml missing"

# Test 3: Check if GitHub workflows exist
[ -f ".github/workflows/ci.yml" ] && test_check "CI workflow exists" || echo -e "${RED}❌ FAIL:${NC} CI workflow missing"
[ -f ".github/workflows/pre-commit-checks.yml" ] && test_check "Pre-commit checks workflow exists" || echo -e "${RED}❌ FAIL:${NC} Pre-commit checks workflow missing"
[ -f ".github/dependabot.yml" ] && test_check "Dependabot config exists" || echo -e "${RED}❌ FAIL:${NC} Dependabot config missing"

echo ""
echo "2. Testing branch naming validation..."

# Test branch naming logic
test_branch() {
    local branch=$1
    local expected=$2
    if [[ "$branch" =~ ^(main|master|develop)$ ]] || [[ "$branch" =~ ^(feature/|fix/|docs/|chore/|refactor/|test/).+$ ]]; then
        if [ "$expected" = "valid" ]; then
            test_check "Branch '$branch' correctly validated as valid"
        else
            echo -e "${RED}❌ FAIL:${NC} Branch '$branch' should be invalid but was accepted"
            ((FAILED++))
        fi
    else
        if [ "$expected" = "invalid" ]; then
            test_check "Branch '$branch' correctly rejected as invalid"
        else
            echo -e "${RED}❌ FAIL:${NC} Branch '$branch' should be valid but was rejected"
            ((FAILED++))
        fi
    fi
}

# Test valid branches
test_branch "feature/new-handler" "valid"
test_branch "fix/bug-123" "valid"
test_branch "docs/update-readme" "valid"
test_branch "chore/update-deps" "valid"
test_branch "refactor/code-cleanup" "valid"
test_branch "test/add-tests" "valid"
test_branch "main" "valid"
test_branch "develop" "valid"

# Test invalid branches
test_branch "my-branch" "invalid"
test_branch "update-code" "invalid"
test_branch "patch-1" "invalid"
test_branch "hotfix" "invalid"

echo ""
echo "3. Testing commit message format validation..."

# Test commit message format
test_commit_msg() {
    local msg=$1
    local expected=$2
    # Check if message matches conventional commits format: type(scope): subject
    # Pattern: starts with type, optional scope in parens, colon, space, subject (min 10 chars)
    local pattern="^(feat|fix|docs|style|refactor|test|chore)"
    if [[ "$msg" =~ $pattern ]]; then
        # Check if it has proper format with colon and space, and subject is at least 10 chars
        if [[ "$msg" =~ :[[:space:]] ]] && [[ ${#msg} -ge 10 ]]; then
            # Extract subject part (after colon and space)
            local subject="${msg#*: }"
            # Subject should be at least 10 characters
            if [[ ${#subject} -ge 10 ]]; then
                local is_valid=true
            else
                local is_valid=false
            fi
        else
            local is_valid=false
        fi
    else
        local is_valid=false
    fi
    
    if [ "$is_valid" = "true" ]; then
        if [ "$expected" = "valid" ]; then
            test_check "Commit message correctly validated: '$msg'"
        else
            echo -e "${RED}❌ FAIL:${NC} Commit message should be invalid: '$msg'"
            ((FAILED++))
        fi
    else
        if [ "$expected" = "invalid" ]; then
            test_check "Commit message correctly rejected: '$msg'"
        else
            echo -e "${RED}❌ FAIL:${NC} Commit message should be valid: '$msg'"
            ((FAILED++))
        fi
    fi
}

# Test valid commit messages
test_commit_msg "feat(ingestion): add PostgreSQL handler" "valid"
test_commit_msg "fix(datahub): handle connection timeout" "valid"
test_commit_msg "docs(readme): update installation guide" "valid"
test_commit_msg "style: auto-format code" "valid"
test_commit_msg "refactor(common): improve error handling" "valid"
test_commit_msg "test(handlers): add unit tests" "valid"
test_commit_msg "chore: update dependencies" "valid"

# Test invalid commit messages
test_commit_msg "Update code" "invalid"
test_commit_msg "Fixed bug" "invalid"
test_commit_msg "Changes" "invalid"
test_commit_msg "feat: short" "invalid"  # Too short

echo ""
echo "4. Checking file structure..."

# Check if required directories exist
[ -d ".github/workflows" ] && test_check ".github/workflows directory exists" || echo -e "${RED}❌ FAIL:${NC} .github/workflows missing"
[ -d ".github/ISSUE_TEMPLATE" ] && test_check ".github/ISSUE_TEMPLATE directory exists" || echo -e "${RED}❌ FAIL:${NC} .github/ISSUE_TEMPLATE missing"

# Check if documentation files exist
[ -f "CONTRIBUTING.md" ] && test_check "CONTRIBUTING.md exists" || echo -e "${RED}❌ FAIL:${NC} CONTRIBUTING.md missing"
[ -f "SETUP_PRE_COMMIT.md" ] && test_check "SETUP_PRE_COMMIT.md exists" || echo -e "${RED}❌ FAIL:${NC} SETUP_PRE_COMMIT.md missing"
[ -f "CODE_QUALITY_STANDARDS.md" ] && test_check "CODE_QUALITY_STANDARDS.md exists" || echo -e "${RED}❌ FAIL:${NC} CODE_QUALITY_STANDARDS.md missing"
[ -f "CHANGELOG.md" ] && test_check "CHANGELOG.md exists" || echo -e "${RED}❌ FAIL:${NC} CHANGELOG.md missing"

echo ""
echo "5. Validating configuration file syntax..."

# Check YAML syntax (basic check - look for common errors)
if grep -q "repos:" .pre-commit-config.yaml 2>/dev/null; then
    test_check ".pre-commit-config.yaml has valid structure"
else
    echo -e "${RED}❌ FAIL:${NC} .pre-commit-config.yaml structure issue"
    ((FAILED++))
fi

# Check if pyproject.toml has required sections
if grep -q "\[tool.black\]" pyproject.toml 2>/dev/null && grep -q "\[tool.ruff\]" pyproject.toml 2>/dev/null; then
    test_check "pyproject.toml has required tool configurations"
else
    echo -e "${RED}❌ FAIL:${NC} pyproject.toml missing required sections"
    ((FAILED++))
fi

# Check if workflows have required structure
if grep -q "name:" .github/workflows/ci.yml 2>/dev/null && grep -q "on:" .github/workflows/ci.yml 2>/dev/null; then
    test_check "CI workflow has valid structure"
else
    echo -e "${RED}❌ FAIL:${NC} CI workflow structure issue"
    ((FAILED++))
fi

echo ""
echo "6. Checking current branch..."
current_branch=$(git branch --show-current 2>/dev/null || echo "")
if [ -n "$current_branch" ]; then
    if [[ "$current_branch" =~ ^(main|master|develop)$ ]] || [[ "$current_branch" =~ ^(feature/|fix/|docs/|chore/|refactor/|test/).+$ ]]; then
        echo -e "${GREEN}✅ Current branch '$current_branch' follows naming convention${NC}"
        ((PASSED++))
    else
        echo -e "${YELLOW}⚠️  Current branch '$current_branch' does NOT follow naming convention${NC}"
        echo "   Should start with: feature/, fix/, docs/, chore/, refactor/, or test/"
        ((FAILED++))
    fi
else
    echo -e "${YELLOW}⚠️  Could not determine current branch${NC}"
fi

echo ""
echo "=================================="
echo "📊 Test Results:"
echo -e "${GREEN}✅ Passed: $PASSED${NC}"
if [ $FAILED -gt 0 ]; then
    echo -e "${RED}❌ Failed: $FAILED${NC}"
    exit 1
else
    echo -e "${GREEN}✅ All tests passed!${NC}"
    exit 0
fi

