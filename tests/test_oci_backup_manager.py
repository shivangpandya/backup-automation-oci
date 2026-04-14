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
        self.assertTrue(plan["services"]["postgresql"]["enabled"])
        self.assertIn("dr", plan)
        self.assertIn("mysql_read_replicas", plan)

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
            planned_action="backup",
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
            planned_action="backup",
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
        self.assertEqual(discovered[0].planned_actions, ["backup"])


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
            planned_action="backup",
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


class ActionPlanningTests(unittest.TestCase):
    def test_mysql_actions_include_initial_dr_copy_and_read_replica(self):
        plan = MODULE.validate_and_normalize_plan({
            "dr": {"enabled": True, "target_region": "us-phoenix-1"},
            "mysql_read_replicas": {"enabled": True},
        })
        resource = MODULE.build_discovery_item(
            resource_type="MySQL",
            resource_id="mysql-1",
            display_name="mysql-a",
            compartment_id="compartment-a",
            region="us-ashburn-1",
            source_tag_path="Operations.backup=enabled",
            planned_action="backup",
            is_direct_tag_match=True,
        )
        discovery = {"resources": [resource], "skipped_resources": []}
        MODULE.enrich_discovery_with_plan_actions(discovery, plan)
        self.assertEqual(
            discovery["resources"][0].planned_actions,
            ["backup", "initial_dr_copy", "dr_config", "mysql_read_replica"],
        )

    def test_volume_actions_use_dr_config_without_initial_dr_copy(self):
        plan = MODULE.validate_and_normalize_plan({
            "dr": {"enabled": True, "target_region": "us-phoenix-1"},
        })
        resource = MODULE.build_discovery_item(
            resource_type="BlockVolume",
            resource_id="vol-1",
            display_name="vol-a",
            compartment_id="compartment-a",
            region="us-ashburn-1",
            source_tag_path="Operations.backup=enabled",
            planned_action="backup",
            is_direct_tag_match=True,
        )
        discovery = {"resources": [resource], "skipped_resources": []}
        MODULE.enrich_discovery_with_plan_actions(discovery, plan)
        self.assertEqual(discovery["resources"][0].planned_actions, ["backup", "dr_config"])


class PostgreSQLBackupTests(unittest.TestCase):
    def test_postgresql_backup_includes_compartment_id_and_resolves_async_backup(self):
        captured = {}

        class FakePsqlClient:
            def get_db_system(self, db_id):
                return SimpleNamespace(data=SimpleNamespace(compartment_id="ocid1.compartment.test", id=db_id))

            def create_backup(self, details):
                captured["details"] = details
                return SimpleNamespace(data=None, headers={"opc-work-request-id": "wr-1"})

            def get_work_request(self, work_request_id):
                return SimpleNamespace(data=SimpleNamespace(status="SUCCEEDED"))

            def list_backups(self, **kwargs):
                return None

            def get_backup(self, backup_id):
                return SimpleNamespace(data=SimpleNamespace(id=backup_id, display_name="pg-backup"))

        original_make_client = MODULE.make_client
        original_oci = MODULE.oci
        original_list_all_results = MODULE.list_all_results
        original_sleep = MODULE.time.sleep
        try:
            MODULE.make_client = lambda *args, **kwargs: FakePsqlClient()
            MODULE.list_all_results = lambda fn, **kwargs: [SimpleNamespace(id="backup-1", time_created="2026-04-14T00:00:00Z")]
            MODULE.time.sleep = lambda *_args, **_kwargs: None
            MODULE.oci = SimpleNamespace(
                psql=SimpleNamespace(
                    PostgresqlClient=object(),
                    models=SimpleNamespace(
                        CreateBackupDetails=lambda **kwargs: SimpleNamespace(**kwargs)
                    ),
                )
            )

            backup = MODULE.backup_database({}, None, "db-system-1", "postgresql", "pg-backup")
        finally:
            MODULE.make_client = original_make_client
            MODULE.list_all_results = original_list_all_results
            MODULE.time.sleep = original_sleep
            MODULE.oci = original_oci

        self.assertEqual(captured["details"].compartment_id, "ocid1.compartment.test")
        self.assertEqual(captured["details"].db_system_id, "db-system-1")
        self.assertEqual(captured["details"].display_name, "pg-backup")
        self.assertEqual(backup.id, "backup-1")


if __name__ == "__main__":
    unittest.main()
