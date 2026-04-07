import importlib.util
import unittest
from pathlib import Path
from types import SimpleNamespace


MODULE_PATH = Path(__file__).resolve().parents[1] / "oci_backup_manager.py"
SPEC = importlib.util.spec_from_file_location("oci_backup_manager", MODULE_PATH)
MODULE = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(MODULE)


class PlanValidationTests(unittest.TestCase):
    def test_default_plan_is_valid(self):
        plan = MODULE.validate_and_normalize_plan({})
        self.assertEqual(plan["discovery"]["mode"], "defined_tag")
        self.assertEqual(plan["discovery"]["tag"]["namespace"], "Operations")
        self.assertTrue(plan["services"]["block_volumes"]["enabled"])

    def test_missing_tag_namespace_is_rejected(self):
        with self.assertRaises(MODULE.PlanValidationError):
            MODULE.validate_and_normalize_plan({
                "discovery": {
                    "tag": {
                        "namespace": "",
                        "key": "backup",
                        "value": "enabled",
                    }
                }
            })


class DiscoveryDedupTests(unittest.TestCase):
    def test_direct_and_derived_items_deduplicate(self):
        items = {}
        ordered_keys = []

        direct = MODULE.build_discovery_item(
            resource_type="BlockVolume",
            resource_id="vol-1",
            display_name="vol-a",
            compartment_id="compartment-a",
            region="us-ashburn-1",
            source_tag_path="Operations.backup=enabled",
            planned_action="create_block_volume_backup",
            is_direct_tag_match=True,
            reason="Direct tag",
        )
        derived = MODULE.build_discovery_item(
            resource_type="BlockVolume",
            resource_id="vol-1",
            display_name="vol-a",
            compartment_id="compartment-a",
            region="us-ashburn-1",
            source_tag_path="Operations.backup=enabled",
            planned_action="create_block_volume_backup",
            is_direct_tag_match=False,
            derived_from_type="Instance",
            derived_from_id="instance-1",
            reason="Derived from instance",
        )

        MODULE.add_discovered_item(items, ordered_keys, direct)
        MODULE.add_discovered_item(items, ordered_keys, derived)
        discovered = MODULE.discovered_items_to_list(items, ordered_keys)

        self.assertEqual(len(discovered), 1)
        self.assertTrue(discovered[0].is_direct_tag_match)
        self.assertIn("Direct tag", discovered[0].inclusion_reasons)
        self.assertIn("Derived from instance", discovered[0].inclusion_reasons)


class EligibilityTests(unittest.TestCase):
    def test_block_volume_eligibility_requires_available(self):
        self.assertTrue(MODULE.is_resource_backup_eligible("BlockVolume", "AVAILABLE"))
        self.assertFalse(MODULE.is_resource_backup_eligible("BlockVolume", "TERMINATED"))

    def test_skip_reason_mentions_required_state(self):
        reason = MODULE.build_skip_reason("BlockVolume", "TERMINATED", "direct")
        self.assertIn("AVAILABLE", reason)
        self.assertIn("TERMINATED", reason)

    def test_revalidation_marks_resource_as_skipped_when_state_changes(self):
        resource = MODULE.build_discovery_item(
            resource_type="BlockVolume",
            resource_id="vol-1",
            display_name="vol-a",
            compartment_id="compartment-a",
            region="us-ashburn-1",
            source_tag_path="Operations.backup=enabled",
            planned_action="create_block_volume_backup",
            is_direct_tag_match=True,
            lifecycle_state="AVAILABLE",
            is_backup_eligible=True,
        )

        original_fetch = MODULE.fetch_live_resource
        try:
            MODULE.fetch_live_resource = lambda config, signer, discovered: SimpleNamespace(lifecycle_state="TERMINATED")
            updated = MODULE.revalidate_resource_eligibility({}, None, resource)
        finally:
            MODULE.fetch_live_resource = original_fetch

        self.assertFalse(updated.is_backup_eligible)
        self.assertEqual(updated.lifecycle_state, "TERMINATED")
        self.assertIn("State changed before execution", updated.skip_reason)


if __name__ == "__main__":
    unittest.main()
