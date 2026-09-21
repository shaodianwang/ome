#!/usr/bin/env python3
import tempfile
import unittest
from pathlib import Path

import check_test_layout


def repo(files):
    """Build a throwaway repo whose workload tree holds `files`.

    Keys are paths under pkg/controller/v1beta1/workload; values are file
    bodies. Returns the TemporaryDirectory (the caller keeps it alive).
    """
    tmp = tempfile.TemporaryDirectory()
    root = Path(tmp.name) / check_test_layout.WORKLOAD_ROOT
    for rel, body in files.items():
        path = root / rel
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(body)
    return tmp


FUNC = "package p\n\nfunc Do() {}\n"
DECL_ONLY = "package p\n\ntype T struct{}\n\nconst X = 1\n"
TEST = "package p\n\nimport \"testing\"\n\nfunc TestDo(t *testing.T) {}\n"
EXTERNAL_TEST = "package p_test\n\nimport \"testing\"\n\nfunc TestDoAsAConsumer(t *testing.T) {}\n"


class LayoutTest(unittest.TestCase):
    def violations(self, files):
        tmp = repo(files)
        self.addCleanup(tmp.cleanup)
        return check_test_layout.check_layout(tmp.name)

    def test_one_test_file_per_production_file_passes(self):
        self.assertEqual(self.violations({"do.go": FUNC, "do_test.go": TEST}), [])

    def test_production_file_with_no_test_file_fails(self):
        (violation,) = self.violations({"do.go": FUNC})
        self.assertIn("do.go", violation)
        self.assertIn("no do_test.go", violation)

    def test_declaration_only_file_is_exempt(self):
        self.assertEqual(self.violations({"kinds.go": DECL_ONLY}), [])

    def test_test_file_naming_no_production_file_fails(self):
        (violation,) = self.violations(
            {"do.go": FUNC, "do_test.go": TEST, "corrective_recovery_test.go": TEST}
        )
        self.assertIn("corrective_recovery_test.go", violation)
        self.assertIn("names no production file", violation)

    def test_forced_internal_and_external_split_is_allowed(self):
        self.assertEqual(
            self.violations(
                {"do.go": FUNC, "do_test.go": TEST, "do_external_test.go": EXTERNAL_TEST}
            ),
            [],
        )

    def test_a_third_test_file_for_one_production_file_fails(self):
        (violation,) = self.violations(
            {
                "do.go": FUNC,
                "do_test.go": TEST,
                "do_external_test.go": EXTERNAL_TEST,
                "do_extra_test.go": TEST,
            }
        )
        self.assertIn("do_extra_test.go", violation)

    def test_helpers_files_are_exempt_and_do_not_satisfy_anything(self):
        self.assertEqual(
            self.violations(
                {
                    "do.go": FUNC,
                    "do_test.go": TEST,
                    "helpers_test.go": TEST,
                    "helpers_external_test.go": EXTERNAL_TEST,
                }
            ),
            [],
        )
        (violation,) = self.violations({"do.go": FUNC, "helpers_test.go": TEST})
        self.assertIn("no do_test.go", violation)

    def test_a_production_file_named_external_owns_its_own_test(self):
        self.assertEqual(
            self.violations({"do_external.go": FUNC, "do_external_test.go": TEST}), []
        )

    def test_each_package_is_checked_on_its_own(self):
        violations = self.violations(
            {"do.go": FUNC, "do_test.go": TEST, "ops/run.go": FUNC}
        )
        self.assertEqual(len(violations), 1)
        self.assertIn("ops", violations[0])

    def test_a_missing_tree_is_reported_rather_than_passing_silently(self):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.assertTrue(check_test_layout.check_layout(tmp.name))


ROW_WRITE = (
    "package p\n\nfunc write() {\n\t_ = func(s *types.InstanceStatus) bool { return true }\n}\n"
)
ROW_TYPE_DECL = (
    "package p\n\ntype Input struct {\n"
    "\tMutateInstance func(idx int32, mutate func(s *InstanceStatus) bool) error\n}\n"
)


class RowWriterTest(unittest.TestCase):
    def violations(self, files):
        tmp = repo(files)
        self.addCleanup(tmp.cleanup)
        return check_test_layout.check_row_writers(tmp.name)

    def test_a_row_writer_package_may_author_the_closure(self):
        self.assertEqual(self.violations({"status/decision.go": ROW_WRITE}), [])
        self.assertEqual(self.violations({"ops/create.go": ROW_WRITE}), [])
        self.assertEqual(self.violations({"reconcile.go": ROW_WRITE}), [])

    def test_a_reading_package_may_not(self):
        (violation,) = self.violations({"evidence/crashloop.go": ROW_WRITE})
        self.assertIn("crashloop.go", violation)
        self.assertIn("row-mutation", violation)

    def test_declaring_the_callback_type_is_not_authoring_one(self):
        self.assertEqual(self.violations({"types/input.go": ROW_TYPE_DECL}), [])

    def test_a_subpackage_of_a_writer_is_checked(self):
        self.assertTrue(self.violations({"ops/inner/x.go": ROW_WRITE}))


if __name__ == "__main__":
    unittest.main()
