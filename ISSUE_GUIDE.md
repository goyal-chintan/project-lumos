# Issue Creation Guide

This guide explains how to create proper issues for the Lumos Framework project.

## 📋 Issue Types

We have three main issue templates:

### 1. 🐛 Bug Report
Use for reporting bugs, errors, or unexpected behavior.

**Template**: `.github/ISSUE_TEMPLATE/bug_report.md`

**Required Information:**
- Clear bug description
- Steps to reproduce
- Expected vs actual behavior
- Environment details (OS, Python version, etc.)
- Error messages/logs
- Configuration (if applicable)

**Example Title**: `[BUG] Connection timeout when ingesting large CSV files`

### 2. ✨ Feature Request
Use for suggesting new features or enhancements.

**Template**: `.github/ISSUE_TEMPLATE/feature_request.md`

**Required Information:**
- Feature description
- Use case/problem it solves
- Proposed solution
- Alternatives considered
- Implementation notes (if any)

**Example Title**: `[FEATURE] Add support for Delta Lake format`

### 3. ❓ Question
Use for asking questions about the project.

**Template**: `.github/ISSUE_TEMPLATE/question.md`

**Required Information:**
- Clear question
- Context
- What you've tried
- Related documentation checked

**Example Title**: `[QUESTION] How to configure custom ownership types?`

## 🎯 Best Practices

### Issue Title Format
- **Bug**: `[BUG] Brief description`
- **Feature**: `[FEATURE] Brief description`
- **Docs** (recommended): `[DOCS] Brief description`
- **Chore** (recommended): `[CHORE] Brief description`
- **Refactor** (recommended): `[REFACTOR] Brief description`
- **Test** (recommended): `[TEST] Brief description`
- **Question**: `[QUESTION] Brief description`

### Writing Good Issues

1. **Be Specific**
   - ❌ Bad: "It doesn't work"
   - ✅ Good: "CSV ingestion fails with 'ConnectionError' when processing files > 1GB"

2. **Provide Context**
   - Include environment details
   - Share relevant configuration (remove secrets!)
   - Provide error messages/logs

3. **Include Steps to Reproduce** (for bugs)
   - Numbered list of steps
   - Expected vs actual behavior
   - Minimal reproducible example if possible

4. **Search First**
   - Check if the issue already exists
   - Look at closed issues for similar problems
   - Review documentation

5. **Use Labels Appropriately**
   - Labels are automatically applied based on template
   - Additional labels can be added by maintainers

## 🔗 Linking Branches and PRs to Issues

We require all development work to be traceable back to an issue.

### Branch Naming (Required)

Branch names must include the issue number:

- **Format**: `<type>/<issue-number>-<short-kebab-case>`
- **Examples**:
  - `feature/19-link-issues-to-prs`
  - `fix/15-ingestion-status-message`
  - `docs/8-add-demo-screenshots`

### PR Description (Required)

Your PR description must reference the same issue number as your branch, using one of:

- `Closes #<issue-number>`
- `Fixes #<issue-number>`
- `Resolves #<issue-number>`
- `Refs #<issue-number>`

This is validated in CI, and PRs that don’t link an issue will fail checks.

## 📝 Issue Template Structure

### Bug Report Template
```markdown
## Bug Description
[Clear description]

## Steps to Reproduce
1. Step one
2. Step two
3. See error

## Expected Behavior
[What should happen]

## Actual Behavior
[What actually happens]

## Environment
- OS: [e.g., macOS 13.0]
- Python Version: [e.g., 3.9.0]
- Framework Version: [e.g., v1.0.0]

## Error Messages/Logs
[Paste logs here]
```

### Feature Request Template
```markdown
## Feature Description
[Clear description]

## Use Case
[Problem it solves]

## Proposed Solution
[How it should work]

## Alternatives Considered
[Other options]

## Implementation Notes
[Ideas for implementation]
```

## 🔍 Checking Your Issue

Before submitting, ensure:

- [ ] Title follows the format: `[TYPE] Description`
- [ ] All required sections are filled out
- [ ] Code examples are formatted properly
- [ ] No sensitive information is included
- [ ] Related issues are linked (if applicable)
- [ ] Labels are appropriate (auto-applied from template)

## 📚 Related Resources

- [CONTRIBUTING.md](CONTRIBUTING.md) - Contribution guidelines
- [CODE_QUALITY_STANDARDS.md](CODE_QUALITY_STANDARDS.md) - Code quality standards
- [README.md](README.md) - Project overview

## 🚫 What NOT to Include

- ❌ Personal information
- ❌ API keys or secrets
- ❌ Large code dumps (use gists instead)
- ❌ Vague descriptions
- ❌ Multiple unrelated issues in one

## ✅ Example: Good Bug Report

**Title**: `[BUG] Avro handler fails on nested union types`

**Description**:
```markdown
## Bug Description
When processing Avro files with nested union types, the handler throws 
a `SchemaError` instead of properly parsing the nested structure.

## Steps to Reproduce
1. Create an Avro file with nested union: `{"type": ["null", {"type": "record", ...}]}`
2. Run: `python framework_cli.py ingest:avro_config.json`
3. See error: `SchemaError: Cannot parse nested union type`

## Expected Behavior
The handler should correctly parse and extract schema from nested union types.

## Actual Behavior
Handler throws `SchemaError` and ingestion fails.

## Environment
- OS: macOS 14.0
- Python Version: 3.10.5
- Framework Version: v0.1.0
- DataHub Version: v0.10.0

## Error Messages/Logs
```
ERROR: SchemaError: Cannot parse nested union type
  File "feature/ingestion/handlers/avro.py", line 45
    raise SchemaError("Cannot parse nested union type")
```

## Additional Context
This works fine with simple union types, but fails with nested structures.
```

## 💡 Tips

1. **Use Code Blocks**: Format code, logs, and configs properly
2. **Attach Files**: Use GitHub's file attachment for configs/logs
3. **Link Related Issues**: Reference related issues with `#issue-number`
4. **Be Patient**: Maintainers will respond when they can
5. **Update Issues**: If you find more information, update the issue

## 🔗 Issue Templates Location

All templates are in `.github/ISSUE_TEMPLATE/`:
- `bug_report.md` - Bug reports
- `feature_request.md` - Feature requests
- `question.md` - Questions
- `config.yml` - Template configuration

---

**Need Help?** Check the [Documentation](README.md) or open a [Discussion](https://github.com/goyal-chintan/project-lumos/discussions).

