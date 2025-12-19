import unittest

from scripts.conventions import (
    extract_issue_refs,
    validate_branch_name,
    validate_conventional_subject,
    validate_pr_body_links_issue,
)


class TestConventions(unittest.TestCase):
    def test_validate_branch_name_allows_default_branches(self) -> None:
        for branch in ("main", "master", "develop"):
            with self.subTest(branch=branch):
                res = validate_branch_name(branch)
                self.assertTrue(res.ok, res.message)

    def test_validate_branch_name_allows_dependabot_branches(self) -> None:
        res = validate_branch_name("dependabot/pip/requests-2.32.3")
        self.assertTrue(res.ok, res.message)

    def test_validate_branch_name_allows_valid_format(self) -> None:
        for branch in (
            "feature/19-link-issues-to-prs",
            "fix/15-ingestion-status-message",
            "docs/8-update-readme",
            "chore/16-bump-upload-artifact",
            "refactor/21-cleanup",
            "test/17-add-tests",
        ):
            with self.subTest(branch=branch):
                res = validate_branch_name(branch)
                self.assertTrue(res.ok, res.message)

    def test_validate_branch_name_rejects_missing_issue_number(self) -> None:
        for branch in ("feature/new-handler", "fix/bug-123", "docs/update-readme"):
            with self.subTest(branch=branch):
                res = validate_branch_name(branch)
                self.assertFalse(res.ok)

    def test_validate_branch_name_rejects_invalid_slug_chars(self) -> None:
        res = validate_branch_name("feature/19_bad_slug")
        self.assertFalse(res.ok)

    def test_validate_conventional_subject_accepts_valid(self) -> None:
        res = validate_conventional_subject("chore(conventions): enforce issue linking (#19)")
        self.assertTrue(res.ok, res.message)

    def test_validate_conventional_subject_rejects_invalid(self) -> None:
        res = validate_conventional_subject("Update code")
        self.assertFalse(res.ok)

    def test_extract_issue_refs(self) -> None:
        refs = extract_issue_refs("Closes #19. Related: owner/repo#20. Not: abc#1.")
        self.assertEqual(refs, {19, 20})

    def test_pr_body_links_issue_success(self) -> None:
        res = validate_pr_body_links_issue(
            "This implements the checks.\n\nCloses #19\n",
            branch_name="feature/19-link-issues-to-prs",
            actor="goyal-chintan",
        )
        self.assertTrue(res.ok, res.message)

    def test_pr_body_links_issue_rejects_missing_ref(self) -> None:
        res = validate_pr_body_links_issue(
            "This implements the checks.\n",
            branch_name="feature/19-link-issues-to-prs",
            actor="goyal-chintan",
        )
        self.assertFalse(res.ok)

    def test_pr_body_links_issue_rejects_wrong_issue(self) -> None:
        res = validate_pr_body_links_issue(
            "Closes #18\n",
            branch_name="feature/19-link-issues-to-prs",
            actor="goyal-chintan",
        )
        self.assertFalse(res.ok)

    def test_pr_body_links_issue_exempts_dependabot(self) -> None:
        res = validate_pr_body_links_issue(
            "",
            branch_name="dependabot/pip/requests-2.32.3",
            actor="dependabot[bot]",
        )
        self.assertTrue(res.ok, res.message)


