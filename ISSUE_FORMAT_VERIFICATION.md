# Issue Format Verification

## ✅ Your Issue #9 Format Check

Based on the title you provided:
**`[FEATURE] Create and verify the developer commit convention and feature branch convention based on best practices`**

### Title Analysis:
- ✅ **Prefix**: `[FEATURE]` - Correct format
- ✅ **Description**: Clear and descriptive
- ✅ **Length**: Reasonable (under 100 characters)

### Expected Issue Body Structure:

For a Feature Request, the issue should include:

```markdown
## Feature Description
[Clear description of what you want]

## Use Case
[Why this is needed / problem it solves]

## Proposed Solution
[How it should work]

## Alternatives Considered
[Other options you thought about]

## Additional Context
[Any other relevant information]

## Implementation Notes
[Ideas for how to implement]

## Related Issues
[Links to related issues]
```

## 📋 Issue Template Checklist

When creating issues, ensure:

### For Feature Requests:
- [ ] Title starts with `[FEATURE]`
- [ ] Feature Description is clear and specific
- [ ] Use Case explains the problem/need
- [ ] Proposed Solution describes how it should work
- [ ] Alternatives Considered section is filled (even if "None")
- [ ] Additional Context provided if needed
- [ ] Implementation Notes included (if you have ideas)
- [ ] Related Issues linked (if applicable)

### For Bug Reports:
- [ ] Title starts with `[BUG]`
- [ ] Bug Description is clear
- [ ] Steps to Reproduce are numbered and specific
- [ ] Expected vs Actual Behavior are described
- [ ] Environment details included (OS, Python version, etc.)
- [ ] Error Messages/Logs provided
- [ ] Configuration shared (if relevant, without secrets)

### For Questions:
- [ ] Title starts with `[QUESTION]`
- [ ] Question is clear and specific
- [ ] Context provided
- [ ] What you've tried is documented
- [ ] Related documentation checked

## 🎯 Best Practices

1. **Be Specific**: Vague issues are hard to address
   - ❌ "Improve documentation"
   - ✅ "Add installation guide for Windows users"

2. **Provide Context**: Help maintainers understand the need
   - Include use cases, examples, or scenarios

3. **Search First**: Check if similar issues exist
   - Avoid duplicates
   - Reference related issues

4. **Use Templates**: Always use the appropriate template
   - GitHub will auto-populate the format
   - Ensures consistency

5. **Keep Updated**: Update issues as you learn more
   - Add new information
   - Close if resolved elsewhere

## 📝 Example: Properly Formatted Feature Request

**Title**: `[FEATURE] Add pre-commit hooks for code quality`

**Body**:
```markdown
## Feature Description
Add pre-commit hooks to automatically check code quality, formatting, 
and commit message format before commits are made.

## Use Case
Currently, code quality checks are manual and easy to forget. This leads 
to inconsistent code style and commit messages. Automated pre-commit 
hooks would ensure all code meets quality standards before it's committed.

## Proposed Solution
1. Add `.pre-commit-config.yaml` with hooks for:
   - Black code formatting
   - Ruff linting
   - Commit message format validation
   - Branch naming validation

2. Create setup script for easy installation
3. Document in CONTRIBUTING.md

## Alternatives Considered
- GitHub Actions only (but this catches issues too late)
- Manual checklist (but easy to forget)
- Pre-commit hooks provide the best balance

## Additional Context
This aligns with best practices used by FastAPI, Pydantic, and other 
top Python projects.

## Implementation Notes
- Use pre-commit framework (standard in Python ecosystem)
- Configure Ruff to replace Flake8 (faster, modern)
- Add branch naming validation script
- Create setup documentation

## Related Issues
None
```

## 🔍 How to Verify Your Issue

1. **Check Title Format**:
   - Starts with `[FEATURE]`, `[BUG]`, or `[QUESTION]`
   - Is descriptive and specific
   - Under 100 characters

2. **Check Body Completeness**:
   - All template sections are filled
   - Information is clear and actionable
   - Code examples are formatted properly

3. **Check for Common Issues**:
   - No sensitive information (API keys, passwords)
   - Code is properly formatted in code blocks
   - Links are working
   - Related issues are referenced

## 📚 Resources

- **Issue Guide**: See `ISSUE_GUIDE.md` for comprehensive guidelines
- **Templates**: Located in `.github/ISSUE_TEMPLATE/`
- **Contributing Guide**: See `CONTRIBUTING.md` for contribution process

## ✅ Quick Reference

| Issue Type | Title Prefix | Template File |
|------------|--------------|---------------|
| Bug Report | `[BUG]` | `bug_report.md` |
| Feature Request | `[FEATURE]` | `feature_request.md` |
| Question | `[QUESTION]` | `question.md` |

---

**Note**: Your issue #9 title format is correct! Make sure the body follows the feature request template structure for best results.

