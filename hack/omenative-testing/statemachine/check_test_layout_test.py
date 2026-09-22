#!/usr/bin/env python3
import tempfile
import unittest
from pathlib import Path

import check_test_layout


def repo(files, root=check_test_layout.WORKLOAD_ROOT):
    """Build a throwaway repo whose tree under `root` holds `files`.

    Keys are paths under root (the workload tree by default); values are
    file bodies. Returns the TemporaryDirectory (the caller keeps it alive).
    """
    tmp = tempfile.TemporaryDirectory()
    base = Path(tmp.name) / root
    base.mkdir(parents=True, exist_ok=True)
    for rel, body in files.items():
        path = base / rel
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(body)
    return tmp


FUNC = "package p\n\nfunc Do() {}\n"
DECL_ONLY = "package p\n\ntype T struct{}\n\nconst X = 1\n"
TEST = "package p\n\nimport \"testing\"\n\nfunc TestDo(t *testing.T) {}\n"
EXTERNAL_TEST = "package p_test\n\nimport \"testing\"\n\nfunc TestDoAsAConsumer(t *testing.T) {}\n"

WORKLOAD_ONLY = (check_test_layout.ROOTS[0],)
ADAPTER_ONLY = (check_test_layout.ROOTS[1],)


class LayoutTest(unittest.TestCase):
    def violations(self, files):
        tmp = repo(files)
        self.addCleanup(tmp.cleanup)
        return check_test_layout.check_layout(tmp.name, WORKLOAD_ONLY)

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
        self.assertTrue(check_test_layout.check_layout(tmp.name, WORKLOAD_ONLY))


class AdapterLayoutTest(unittest.TestCase):
    """The adapter tree is held to the same layout rule as the engine."""

    def violations(self, files):
        tmp = repo(files, check_test_layout.ADAPTER_ROOT)
        self.addCleanup(tmp.cleanup)
        return check_test_layout.check_layout(tmp.name, ADAPTER_ONLY)

    def test_one_test_file_per_production_file_passes(self):
        self.assertEqual(
            self.violations(
                {"reconciler.go": FUNC, "reconciler_test.go": TEST, "helpers_test.go": TEST}
            ),
            [],
        )

    def test_a_scenario_named_test_file_fails(self):
        (violation,) = self.violations(
            {"reconciler.go": FUNC, "reconciler_test.go": TEST, "gang_loss_restart_test.go": TEST}
        )
        self.assertIn(check_test_layout.ADAPTER_ROOT, violation)
        self.assertIn("gang_loss_restart_test.go", violation)
        self.assertIn("names no production file", violation)

    def test_a_second_test_file_for_one_production_file_fails(self):
        (violation,) = self.violations(
            {"status.go": FUNC, "status_test.go": TEST, "status_contract_test.go": TEST}
        )
        self.assertIn("status_contract_test.go", violation)

    def test_a_production_file_without_a_test_file_fails(self):
        (violation,) = self.violations({"manager.go": FUNC})
        self.assertIn("no manager_test.go", violation)

    def test_both_trees_are_checked_by_default(self):
        tmp = repo({"do.go": FUNC, "do_test.go": TEST})
        self.addCleanup(tmp.cleanup)
        violations = check_test_layout.check_layout(tmp.name)
        self.assertEqual(len(violations), 1)
        self.assertIn(check_test_layout.ADAPTER_ROOT, violations[0])
        self.assertIn("no such directory", violations[0])


ROW_WRITE = (
    "package p\n\nfunc write() {\n\t_ = func(s *types.InstanceStatus) bool { return true }\n}\n"
)
ROW_WRITE_MULTILINE = (
    "package p\n\nfunc write() {\n\t_ = func(\n\t\ts *types.InstanceStatus,\n\t) bool {\n"
    "\t\treturn true\n\t}\n}\n"
)
ROW_TYPE_DECL = (
    "package p\n\ntype Input struct {\n"
    "\tMutateInstance func(idx int32, mutate func(s *InstanceStatus) bool) error\n}\n"
)
ROW_CALLBACK_PARAMETER = (
    "package p\n\nfunc (s *Store) MutateInstance(idx int32, mutate func(*InstanceStatus) bool) error {\n"
    "\treturn nil\n}\n"
)
ROW_NOOP_PREFLIGHT = (
    "package p\n\nfunc confirm() {\n\t_ = func(*types.InstanceStatus) bool { return false }\n}\n"
)


class RowWriterTest(unittest.TestCase):
    def violations(self, files):
        tmp = repo(files)
        self.addCleanup(tmp.cleanup)
        return check_test_layout.check_row_writers(tmp.name, WORKLOAD_ONLY)

    def test_the_status_package_may_author_the_closure(self):
        self.assertEqual(self.violations({"status/decision.go": ROW_WRITE}), [])
        self.assertEqual(self.violations({"status/create.go": ROW_WRITE_MULTILINE}), [])

    def test_the_reconcile_root_may_not(self):
        (violation,) = self.violations({"reconcile.go": ROW_WRITE})
        self.assertIn("reconcile.go", violation)
        self.assertIn("row-mutation", violation)

    def test_an_operation_pass_may_not(self):
        (violation,) = self.violations({"ops/create.go": ROW_WRITE})
        self.assertIn("create.go", violation)
        self.assertIn("row-mutation", violation)

    def test_the_escalation_pass_may_not(self):
        (violation,) = self.violations({"escalation/refresh.go": ROW_WRITE})
        self.assertIn("refresh.go", violation)
        self.assertIn("row-mutation", violation)

    def test_a_reading_package_may_not(self):
        (violation,) = self.violations({"evidence/crashloop.go": ROW_WRITE})
        self.assertIn("crashloop.go", violation)
        self.assertIn("row-mutation", violation)

    def test_a_closure_spread_over_lines_is_still_a_closure(self):
        (violation,) = self.violations({"ops/create.go": ROW_WRITE_MULTILINE})
        self.assertIn("create.go", violation)

    def test_declaring_the_callback_type_is_not_authoring_one(self):
        self.assertEqual(self.violations({"types/input.go": ROW_TYPE_DECL}), [])

    def test_taking_the_callback_as_a_parameter_is_not_authoring_one(self):
        self.assertEqual(self.violations({"replay/rowstore.go": ROW_CALLBACK_PARAMETER}), [])

    def test_a_closure_that_cannot_name_the_row_is_not_a_write(self):
        self.assertEqual(self.violations({"ops/migrate.go": ROW_NOOP_PREFLIGHT}), [])

    def test_a_subpackage_of_a_writer_is_checked(self):
        self.assertTrue(self.violations({"ops/inner/x.go": ROW_WRITE}))


class AdapterRowWriterTest(unittest.TestCase):
    """The adapter authors no row-mutation closure: its allow-list is empty,
    so the closure is a violation anywhere under it, its own status writer
    included."""

    def violations(self, files):
        tmp = repo(files, check_test_layout.ADAPTER_ROOT)
        self.addCleanup(tmp.cleanup)
        return check_test_layout.check_row_writers(tmp.name, ADAPTER_ONLY)

    def test_the_adapter_root_may_not(self):
        (violation,) = self.violations({"reset_instances.go": ROW_WRITE})
        self.assertIn("reset_instances.go", violation)
        self.assertIn("row-mutation", violation)
        self.assertIn("none under", violation)

    def test_the_adapters_status_writer_is_not_a_row_writer(self):
        (violation,) = self.violations({"status_writer.go": ROW_WRITE})
        self.assertIn("status_writer.go", violation)

    def test_taking_the_callback_as_a_parameter_is_still_not_authoring_one(self):
        self.assertEqual(self.violations({"convert.go": ROW_CALLBACK_PARAMETER}), [])

    def test_test_files_are_not_checked(self):
        self.assertEqual(self.violations({"convert_test.go": ROW_WRITE}), [])


if __name__ == "__main__":
    unittest.main()
