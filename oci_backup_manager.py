#!/usr/bin/env python3
"""
oci_backup_manager.py
---------------------
A centralized OCI backup automation tool inspired by AWS Backup.

V2 additions:
- tag-driven discovery using OCI defined tags
- orchestration commands to list and run backups for tagged resources
- plan-file validation and execution

Requirements:
    pip install oci
Optional:
    pip install pyyaml

Auth:
    Uses ~/.oci/config by default. Set OCI_CONFIG_FILE / OCI_CONFIG_PROFILE
    env vars to override. Supports instance principal via --auth instance_principal.
"""

import argparse
import json
import logging
import os
import re
import sys
import time
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Dict, Iterable, List, Optional, Sequence

try:
    import oci
    from oci.auth.signers import InstancePrincipalsSecurityTokenSigner
    from oci.config import from_file, validate_config
except ImportError:  # pragma: no cover - handled at runtime
    oci = None
    InstancePrincipalsSecurityTokenSigner = None
    from_file = None
    validate_config = None

try:
    import yaml
except ImportError:  # pragma: no cover - YAML is optional
    yaml = None


CROSS_REGION = "us-phoenix-1"
DEFAULT_RETENTION_DAYS = 30
ARCHIVE_AFTER_DAYS = 90
LOG_FORMAT = "%(asctime)s [%(levelname)s] %(message)s"

DEFAULT_TAG_NAMESPACE = "Operations"
DEFAULT_TAG_KEY = "backup"
DEFAULT_TAG_VALUE = "enabled"
DEFAULT_POLICY_NAME = "oci-backup-manager-tagged-policy"
DEFAULT_OUTPUT = "table"
SUPPORTED_OUTPUTS = ("table", "json")
DEFAULT_DR_BUCKET_SUFFIX = "-dr"
DEFAULT_MYSQL_REPLICA_SUFFIX = "-read-replica"
DEFAULT_AUTOMATIC_BACKUP_RETENTION_DAYS = 7
POSTGRESQL_WORK_REQUEST_TIMEOUT_SECONDS = 300
POSTGRESQL_WORK_REQUEST_POLL_SECONDS = 5
SUPPORTED_DISCOVERED_TYPES = {
    "BlockVolume",
    "BootVolume",
    "Bucket",
    "AutonomousDatabase",
    "MySQL",
    "PostgreSQL",
    "Instance",
}

logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
log = logging.getLogger("oci_backup_manager")


class PlanValidationError(ValueError):
    """Raised when a plan file is malformed."""


@dataclass
class DiscoveredResource:
    resource_type: str
    resource_id: str
    display_name: str
    compartment_id: str
    region: str
    source_tag_path: str
    planned_action: str
    planned_actions: List[str]
    is_direct_tag_match: bool
    derived_from_type: Optional[str] = None
    derived_from_id: Optional[str] = None
    availability_domain: Optional[str] = None
    lifecycle_state: Optional[str] = None
    is_backup_eligible: bool = True
    skip_reason: Optional[str] = None
    executable: bool = True
    inclusion_reasons: List[str] = field(default_factory=list)

    def add_reason(self, reason: str) -> None:
        if reason not in self.inclusion_reasons:
            self.inclusion_reasons.append(reason)


def require_oci_sdk() -> None:
    if oci is None:
        raise RuntimeError(
            "The OCI Python SDK is not installed. Install it with `pip install oci` "
            "before running OCI operations."
        )


def utc_timestamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")


def json_default(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, DiscoveredResource):
        return asdict(value)
    if hasattr(value, "__dict__"):
        return value.__dict__
    return str(value)


def print_json(data: Any) -> None:
    print(json.dumps(data, indent=2, default=json_default))


def parse_bool(value: Any, default: bool = False) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        lowered = value.strip().lower()
        if lowered in {"1", "true", "yes", "y", "on"}:
            return True
        if lowered in {"0", "false", "no", "n", "off"}:
            return False
    return bool(value)


def ensure_dict(value: Any, field_name: str) -> Dict[str, Any]:
    if value is None:
        return {}
    if not isinstance(value, dict):
        raise PlanValidationError(f"`{field_name}` must be an object.")
    return value


def get_config_and_signer(auth_method: str, profile: str = "DEFAULT"):
    """Return (config dict, signer) for the requested auth method."""
    require_oci_sdk()

    if auth_method == "instance_principal":
        signer = InstancePrincipalsSecurityTokenSigner()
        config: Dict[str, Any] = {}
        log.info("Using instance principal authentication.")
        return config, signer

    cfg_file = os.environ.get("OCI_CONFIG_FILE", oci.config.DEFAULT_LOCATION)
    cfg_profile = os.environ.get("OCI_CONFIG_PROFILE", profile)
    config = from_file(file_location=cfg_file, profile_name=cfg_profile)
    validate_config(config)
    log.info("Loaded OCI config profile '%s' from %s", cfg_profile, cfg_file)
    return config, None


def get_region(config: Dict[str, Any], signer: Any) -> str:
    if config.get("region"):
        return config["region"]
    if signer is not None and getattr(signer, "region", None):
        return signer.region
    return ""


def get_tenancy_id(config: Dict[str, Any], signer: Any) -> str:
    if config.get("tenancy"):
        return config["tenancy"]
    if signer is not None and getattr(signer, "tenancy_id", None):
        return signer.tenancy_id
    return ""


def make_client(client_class, config, signer, region: Optional[str] = None):
    """Instantiate an OCI client, optionally overriding the region."""
    require_oci_sdk()
    effective_region = region or get_region(config, signer)
    kwargs: Dict[str, Any] = {"config": config}
    if signer:
        kwargs["signer"] = signer
    if effective_region:
        kwargs["config"] = {**config, "region": effective_region}
    return client_class(**kwargs)


def list_all_results(list_fn, **kwargs):
    require_oci_sdk()
    return oci.pagination.list_call_get_all_results(list_fn, **kwargs).data


def tag_path(namespace: str, key: str, value: str) -> str:
    return f"{namespace}.{key}={value}"


def get_defined_tag_value(resource: Any, namespace: str, key: str) -> Any:
    defined_tags = getattr(resource, "defined_tags", None) or {}
    namespace_tags = defined_tags.get(namespace) or {}
    return namespace_tags.get(key)


def resource_has_defined_tag(resource: Any, namespace: str, key: str, value: str) -> bool:
    return str(get_defined_tag_value(resource, namespace, key)) == value


def normalize_name(resource: Any, fallback_prefix: str) -> str:
    for attr in ("display_name", "name"):
        if getattr(resource, attr, None):
            return getattr(resource, attr)
    resource_id = getattr(resource, "id", "") or ""
    suffix = resource_id[-8:] if resource_id else "unknown"
    return f"{fallback_prefix}-{suffix}"


def get_resource_lifecycle_state(resource: Any, resource_type: str) -> str:
    if resource_type == "Bucket":
        return str(getattr(resource, "lifecycle_state", None) or "ACTIVE").upper()
    if resource_type in {"MySQL", "PostgreSQL"}:
        return str(getattr(resource, "lifecycle_state", None) or getattr(resource, "state", None) or "").upper()
    return str(getattr(resource, "lifecycle_state", None) or getattr(resource, "state", None) or "").upper()


def is_resource_backup_eligible(resource_type: str, lifecycle_state: str) -> bool:
    eligible_states = {
        "BlockVolume": {"AVAILABLE"},
        "BootVolume": {"AVAILABLE"},
        "Bucket": {"ACTIVE"},
        "AutonomousDatabase": {"AVAILABLE"},
        "MySQL": {"ACTIVE"},
        "PostgreSQL": {"ACTIVE"},
        "Instance": {"RUNNING", "STOPPED"},
    }
    return lifecycle_state in eligible_states.get(resource_type, set())


def build_skip_reason(resource_type: str, lifecycle_state: str, inclusion_type: str) -> str:
    if not lifecycle_state:
        return f"{inclusion_type.capitalize()} {resource_type} has no usable lifecycle state."
    if resource_type in {"BlockVolume", "BootVolume"}:
        return f"{inclusion_type.capitalize()} {resource_type} is in state {lifecycle_state}; backups require AVAILABLE."
    if resource_type == "Bucket":
        return f"{inclusion_type.capitalize()} Bucket is in state {lifecycle_state}; backups require ACTIVE."
    if resource_type == "AutonomousDatabase":
        return f"{inclusion_type.capitalize()} AutonomousDatabase is in state {lifecycle_state}; backups require AVAILABLE."
    if resource_type == "MySQL":
        return f"{inclusion_type.capitalize()} MySQL is in state {lifecycle_state}; backups require ACTIVE."
    if resource_type == "PostgreSQL":
        return f"{inclusion_type.capitalize()} PostgreSQL is in state {lifecycle_state}; backups require ACTIVE."
    if resource_type == "Instance":
        return f"{inclusion_type.capitalize()} Instance is in state {lifecycle_state}; only RUNNING or STOPPED instances can expand attached storage."
    return f"{inclusion_type.capitalize()} {resource_type} is in unsupported state {lifecycle_state}."


def first_or_none(values: Sequence[str]) -> str:
    return values[0] if values else ""


def aggregate_result_status(actions: Sequence[Dict[str, Any]]) -> str:
    if not actions:
        return "skipped"
    statuses = [action["status"] for action in actions]
    if any(status == "failed" for status in statuses):
        return "failed"
    if any(status == "succeeded" for status in statuses):
        return "succeeded"
    if any(status == "planned" for status in statuses):
        return "planned"
    if any(status == "source_only" for status in statuses):
        return "source_only"
    return "skipped"


def aggregate_result_detail(actions: Sequence[Dict[str, Any]]) -> str:
    if not actions:
        return ""
    parts = []
    for action in actions:
        detail = action.get("detail") or action.get("result_id") or ""
        parts.append(f"{action['type']}={action['status']}" + (f"({detail})" if detail else ""))
    return ", ".join(parts)


def extract_ad_ordinal(availability_domain: Optional[str]) -> Optional[str]:
    if not availability_domain:
        return None
    match = re.search(r"AD-(\d+)$", availability_domain)
    return match.group(1) if match else None


def find_matching_destination_ad(source_availability_domain: Optional[str], candidate_ads: Sequence[str]) -> Optional[str]:
    ordinal = extract_ad_ordinal(source_availability_domain)
    if not ordinal:
        return None
    suffix = f"AD-{ordinal}"
    for availability_domain in candidate_ads:
        if availability_domain.endswith(suffix):
            return availability_domain
    return None


def get_object_storage_namespace(config: Dict[str, Any], signer: Any) -> str:
    os_client = make_client(oci.object_storage.ObjectStorageClient, config, signer)
    return os_client.get_namespace().data


def get_compartment_scope(
    config: Dict[str, Any],
    signer: Any,
    root_compartment_id: str,
    include_subcompartments: bool,
) -> List[str]:
    if not include_subcompartments:
        return [root_compartment_id]

    identity_client = make_client(oci.identity.IdentityClient, config, signer)
    compartments = list_all_results(
        identity_client.list_compartments,
        compartment_id=root_compartment_id,
        compartment_id_in_subtree=True,
        access_level="ACCESSIBLE",
    )

    scope = [root_compartment_id]
    for compartment in compartments:
        if getattr(compartment, "lifecycle_state", "") == "ACTIVE":
            scope.append(compartment.id)
    return scope


def get_availability_domains(
    config: Dict[str, Any],
    signer: Any,
    scope_compartment_id: str,
) -> List[str]:
    identity_client = make_client(oci.identity.IdentityClient, config, signer)
    tenancy_id = get_tenancy_id(config, signer) or scope_compartment_id
    ads = identity_client.list_availability_domains(compartment_id=tenancy_id).data
    return [ad.name for ad in ads]


def build_discovery_item(
    resource_type: str,
    resource_id: str,
    display_name: str,
    compartment_id: str,
    region: str,
    source_tag_path: str,
    planned_action: str,
    is_direct_tag_match: bool,
    planned_actions: Optional[List[str]] = None,
    derived_from_type: Optional[str] = None,
    derived_from_id: Optional[str] = None,
    availability_domain: Optional[str] = None,
    lifecycle_state: Optional[str] = None,
    is_backup_eligible: bool = True,
    skip_reason: Optional[str] = None,
    executable: bool = True,
    reason: Optional[str] = None,
) -> DiscoveredResource:
    item = DiscoveredResource(
        resource_type=resource_type,
        resource_id=resource_id,
        display_name=display_name,
        compartment_id=compartment_id,
        region=region,
        source_tag_path=source_tag_path,
        planned_action=planned_action,
        planned_actions=planned_actions[:] if planned_actions else [planned_action],
        is_direct_tag_match=is_direct_tag_match,
        derived_from_type=derived_from_type,
        derived_from_id=derived_from_id,
        availability_domain=availability_domain,
        lifecycle_state=lifecycle_state,
        is_backup_eligible=is_backup_eligible,
        skip_reason=skip_reason,
        executable=executable,
    )
    if reason:
        item.add_reason(reason)
    return item


def merge_discovered_items(existing: DiscoveredResource, incoming: DiscoveredResource) -> None:
    existing.is_direct_tag_match = existing.is_direct_tag_match or incoming.is_direct_tag_match
    if not existing.derived_from_type and incoming.derived_from_type:
        existing.derived_from_type = incoming.derived_from_type
        existing.derived_from_id = incoming.derived_from_id
    if incoming.availability_domain and not existing.availability_domain:
        existing.availability_domain = incoming.availability_domain
    if incoming.lifecycle_state and not existing.lifecycle_state:
        existing.lifecycle_state = incoming.lifecycle_state
    existing.is_backup_eligible = existing.is_backup_eligible or incoming.is_backup_eligible
    if not existing.skip_reason and incoming.skip_reason:
        existing.skip_reason = incoming.skip_reason
    for action in incoming.planned_actions:
        if action not in existing.planned_actions:
            existing.planned_actions.append(action)
    existing.planned_action = first_or_none(existing.planned_actions)
    if incoming.source_tag_path and incoming.source_tag_path != existing.source_tag_path:
        existing.add_reason(f"Also matched via {incoming.source_tag_path}")
    for reason in incoming.inclusion_reasons:
        existing.add_reason(reason)


def add_discovered_item(
    items: Dict[tuple, DiscoveredResource],
    ordered_keys: List[tuple],
    item: DiscoveredResource,
) -> None:
    key = (item.resource_type, item.resource_id)
    if key in items:
        merge_discovered_items(items[key], item)
        return
    items[key] = item
    ordered_keys.append(key)


def discovered_items_to_list(
    items: Dict[tuple, DiscoveredResource],
    ordered_keys: List[tuple],
) -> List[DiscoveredResource]:
    return [items[key] for key in ordered_keys]


def discover_tagged_resources(
    config: Dict[str, Any],
    signer: Any,
    root_compartment_id: str,
    include_subcompartments: bool = False,
    namespace: str = DEFAULT_TAG_NAMESPACE,
    tag_key_name: str = DEFAULT_TAG_KEY,
    tag_value: str = DEFAULT_TAG_VALUE,
    expand_tagged_instances: bool = True,
) -> Dict[str, Any]:
    """
    Discover supported OCI resources carrying the configured defined tag.
    A tagged compute instance expands into its attached boot and block volumes.
    """
    require_oci_sdk()

    scope = get_compartment_scope(config, signer, root_compartment_id, include_subcompartments)
    region = get_region(config, signer)
    source_path = tag_path(namespace, tag_key_name, tag_value)

    compute_client = make_client(oci.core.ComputeClient, config, signer)
    block_client = make_client(oci.core.BlockstorageClient, config, signer)
    db_client = make_client(oci.database.DatabaseClient, config, signer)
    mysql_client = make_client(oci.mysql.DbSystemClient, config, signer)
    psql_client = make_client(oci.psql.PostgresqlClient, config, signer)
    os_client = make_client(oci.object_storage.ObjectStorageClient, config, signer)
    namespace_name = get_object_storage_namespace(config, signer)

    items: Dict[tuple, DiscoveredResource] = {}
    ordered_keys: List[tuple] = []
    skipped_items: Dict[tuple, DiscoveredResource] = {}
    skipped_ordered_keys: List[tuple] = []
    notes: List[str] = []

    availability_domains = get_availability_domains(config, signer, root_compartment_id)

    def add_item(item: DiscoveredResource) -> None:
        add_discovered_item(items, ordered_keys, item)

    def add_skipped_item(item: DiscoveredResource) -> None:
        add_discovered_item(skipped_items, skipped_ordered_keys, item)

    for compartment_id in scope:
        volumes = list_all_results(block_client.list_volumes, compartment_id=compartment_id)
        for volume in volumes:
            if resource_has_defined_tag(volume, namespace, tag_key_name, tag_value):
                lifecycle_state = get_resource_lifecycle_state(volume, "BlockVolume")
                is_eligible = is_resource_backup_eligible("BlockVolume", lifecycle_state)
                add_item(
                    build_discovery_item(
                        resource_type="BlockVolume",
                        resource_id=volume.id,
                        display_name=normalize_name(volume, "block-volume"),
                        compartment_id=compartment_id,
                        region=region,
                        source_tag_path=source_path,
                        planned_action="backup",
                        is_direct_tag_match=True,
                        lifecycle_state=lifecycle_state,
                        is_backup_eligible=is_eligible,
                        skip_reason=None if is_eligible else build_skip_reason("BlockVolume", lifecycle_state, "direct"),
                        reason=f"Directly tagged with {source_path}",
                    )
                ) if is_eligible else add_skipped_item(
                    build_discovery_item(
                        resource_type="BlockVolume",
                        resource_id=volume.id,
                        display_name=normalize_name(volume, "block-volume"),
                        compartment_id=compartment_id,
                        region=region,
                        source_tag_path=source_path,
                        planned_action="backup",
                        is_direct_tag_match=True,
                        lifecycle_state=lifecycle_state,
                        is_backup_eligible=False,
                        skip_reason=build_skip_reason("BlockVolume", lifecycle_state, "direct"),
                        executable=False,
                        reason=f"Directly tagged with {source_path}",
                    )
                )

        for availability_domain in availability_domains:
            boot_volumes = list_all_results(
                block_client.list_boot_volumes,
                compartment_id=compartment_id,
                availability_domain=availability_domain,
            )
            for boot_volume in boot_volumes:
                if resource_has_defined_tag(boot_volume, namespace, tag_key_name, tag_value):
                    lifecycle_state = get_resource_lifecycle_state(boot_volume, "BootVolume")
                    is_eligible = is_resource_backup_eligible("BootVolume", lifecycle_state)
                    add_item(
                        build_discovery_item(
                            resource_type="BootVolume",
                            resource_id=boot_volume.id,
                            display_name=normalize_name(boot_volume, "boot-volume"),
                            compartment_id=compartment_id,
                            region=region,
                            source_tag_path=source_path,
                            planned_action="backup",
                            is_direct_tag_match=True,
                            availability_domain=availability_domain,
                            lifecycle_state=lifecycle_state,
                            is_backup_eligible=is_eligible,
                            skip_reason=None if is_eligible else build_skip_reason("BootVolume", lifecycle_state, "direct"),
                            reason=f"Directly tagged with {source_path}",
                        )
                    ) if is_eligible else add_skipped_item(
                        build_discovery_item(
                            resource_type="BootVolume",
                            resource_id=boot_volume.id,
                            display_name=normalize_name(boot_volume, "boot-volume"),
                            compartment_id=compartment_id,
                            region=region,
                            source_tag_path=source_path,
                            planned_action="backup",
                            is_direct_tag_match=True,
                            availability_domain=availability_domain,
                            lifecycle_state=lifecycle_state,
                            is_backup_eligible=False,
                            skip_reason=build_skip_reason("BootVolume", lifecycle_state, "direct"),
                            executable=False,
                            reason=f"Directly tagged with {source_path}",
                        )
                    )

        databases = list_all_results(
            db_client.list_autonomous_databases,
            compartment_id=compartment_id,
        )
        for database in databases:
            if resource_has_defined_tag(database, namespace, tag_key_name, tag_value):
                lifecycle_state = get_resource_lifecycle_state(database, "AutonomousDatabase")
                is_eligible = is_resource_backup_eligible("AutonomousDatabase", lifecycle_state)
                add_item(
                    build_discovery_item(
                        resource_type="AutonomousDatabase",
                        resource_id=database.id,
                        display_name=normalize_name(database, "autonomous-db"),
                        compartment_id=compartment_id,
                        region=region,
                        source_tag_path=source_path,
                        planned_action="backup",
                        is_direct_tag_match=True,
                        lifecycle_state=lifecycle_state,
                        is_backup_eligible=is_eligible,
                        skip_reason=None if is_eligible else build_skip_reason("AutonomousDatabase", lifecycle_state, "direct"),
                        reason=f"Directly tagged with {source_path}",
                    )
                ) if is_eligible else add_skipped_item(
                    build_discovery_item(
                        resource_type="AutonomousDatabase",
                        resource_id=database.id,
                        display_name=normalize_name(database, "autonomous-db"),
                        compartment_id=compartment_id,
                        region=region,
                        source_tag_path=source_path,
                        planned_action="backup",
                        is_direct_tag_match=True,
                        lifecycle_state=lifecycle_state,
                        is_backup_eligible=False,
                        skip_reason=build_skip_reason("AutonomousDatabase", lifecycle_state, "direct"),
                        executable=False,
                        reason=f"Directly tagged with {source_path}",
                    )
                )

        mysql_db_systems = list_all_results(
            mysql_client.list_db_systems,
            compartment_id=compartment_id,
        )
        for db_system in mysql_db_systems:
            if resource_has_defined_tag(db_system, namespace, tag_key_name, tag_value):
                lifecycle_state = get_resource_lifecycle_state(db_system, "MySQL")
                is_eligible = is_resource_backup_eligible("MySQL", lifecycle_state)
                add_item(
                    build_discovery_item(
                        resource_type="MySQL",
                        resource_id=db_system.id,
                        display_name=normalize_name(db_system, "mysql"),
                        compartment_id=compartment_id,
                        region=region,
                        source_tag_path=source_path,
                        planned_action="backup",
                        is_direct_tag_match=True,
                        lifecycle_state=lifecycle_state,
                        is_backup_eligible=is_eligible,
                        skip_reason=None if is_eligible else build_skip_reason("MySQL", lifecycle_state, "direct"),
                        reason=f"Directly tagged with {source_path}",
                    )
                ) if is_eligible else add_skipped_item(
                    build_discovery_item(
                        resource_type="MySQL",
                        resource_id=db_system.id,
                        display_name=normalize_name(db_system, "mysql"),
                        compartment_id=compartment_id,
                        region=region,
                        source_tag_path=source_path,
                        planned_action="backup",
                        is_direct_tag_match=True,
                        lifecycle_state=lifecycle_state,
                        is_backup_eligible=False,
                        skip_reason=build_skip_reason("MySQL", lifecycle_state, "direct"),
                        executable=False,
                        reason=f"Directly tagged with {source_path}",
                    )
                )

        bucket_summaries = list_all_results(
            os_client.list_buckets,
            namespace_name=namespace_name,
            compartment_id=compartment_id,
        )
        for bucket_summary in bucket_summaries:
            bucket = os_client.get_bucket(namespace_name, bucket_summary.name).data
            if resource_has_defined_tag(bucket, namespace, tag_key_name, tag_value):
                lifecycle_state = get_resource_lifecycle_state(bucket, "Bucket")
                is_eligible = is_resource_backup_eligible("Bucket", lifecycle_state)
                add_item(
                    build_discovery_item(
                        resource_type="Bucket",
                        resource_id=getattr(bucket, "id", bucket.name),
                        display_name=bucket.name,
                        compartment_id=compartment_id,
                        region=region,
                        source_tag_path=source_path,
                        planned_action="backup",
                        is_direct_tag_match=True,
                        lifecycle_state=lifecycle_state,
                        is_backup_eligible=is_eligible,
                        skip_reason=None if is_eligible else build_skip_reason("Bucket", lifecycle_state, "direct"),
                        reason=f"Directly tagged with {source_path}",
                    )
                ) if is_eligible else add_skipped_item(
                    build_discovery_item(
                        resource_type="Bucket",
                        resource_id=getattr(bucket, "id", bucket.name),
                        display_name=bucket.name,
                        compartment_id=compartment_id,
                        region=region,
                        source_tag_path=source_path,
                        planned_action="backup",
                        is_direct_tag_match=True,
                        lifecycle_state=lifecycle_state,
                        is_backup_eligible=False,
                        skip_reason=build_skip_reason("Bucket", lifecycle_state, "direct"),
                        executable=False,
                        reason=f"Directly tagged with {source_path}",
                    )
                )

        postgresql_db_systems = list_all_results(
            psql_client.list_db_systems,
            compartment_id=compartment_id,
        )
        for db_system_summary in postgresql_db_systems:
            try:
                db_system = psql_client.get_db_system(db_system_summary.id).data
            except Exception as exc:
                log.warning(
                    "Could not fetch PostgreSQL DB system details for %s during tag discovery: %s",
                    getattr(db_system_summary, "id", "<unknown>"),
                    exc,
                )
                db_system = db_system_summary

            if resource_has_defined_tag(db_system, namespace, tag_key_name, tag_value):
                lifecycle_state = get_resource_lifecycle_state(db_system, "PostgreSQL")
                is_eligible = is_resource_backup_eligible("PostgreSQL", lifecycle_state)
                target_fn = add_item if is_eligible else add_skipped_item
                target_fn(
                    build_discovery_item(
                        resource_type="PostgreSQL",
                        resource_id=db_system.id,
                        display_name=normalize_name(db_system, "postgresql"),
                        compartment_id=getattr(db_system, "compartment_id", compartment_id),
                        region=region,
                        source_tag_path=source_path,
                        planned_action="backup",
                        is_direct_tag_match=True,
                        lifecycle_state=lifecycle_state,
                        is_backup_eligible=is_eligible,
                        skip_reason=None if is_eligible else build_skip_reason("PostgreSQL", lifecycle_state, "direct"),
                        executable=is_eligible,
                        reason=f"Directly tagged with {source_path}",
                    )
                )

        instances = list_all_results(compute_client.list_instances, compartment_id=compartment_id)
        for instance in instances:
            if not resource_has_defined_tag(instance, namespace, tag_key_name, tag_value):
                continue

            instance_lifecycle_state = get_resource_lifecycle_state(instance, "Instance")
            instance_is_eligible = is_resource_backup_eligible("Instance", instance_lifecycle_state)

            instance_item = build_discovery_item(
                resource_type="Instance",
                resource_id=instance.id,
                display_name=normalize_name(instance, "instance"),
                compartment_id=compartment_id,
                region=region,
                source_tag_path=source_path,
                planned_action="source_only",
                is_direct_tag_match=True,
                availability_domain=getattr(instance, "availability_domain", None),
                lifecycle_state=instance_lifecycle_state,
                is_backup_eligible=instance_is_eligible,
                skip_reason=None if instance_is_eligible else build_skip_reason("Instance", instance_lifecycle_state, "direct"),
                executable=False,
                reason=f"Tagged instance used to derive attached storage from {source_path}",
            )
            if instance_is_eligible:
                add_item(instance_item)
            else:
                add_skipped_item(instance_item)

            if not expand_tagged_instances or not instance_is_eligible:
                continue

            instance_ad = getattr(instance, "availability_domain", None)
            boot_attachments = compute_client.list_boot_volume_attachments(
                availability_domain=instance_ad,
                compartment_id=compartment_id,
                instance_id=instance.id,
            ).data
            for attachment in boot_attachments:
                boot_id = getattr(attachment, "boot_volume_id", None)
                if not boot_id:
                    continue
                boot_volume = block_client.get_boot_volume(boot_id).data
                lifecycle_state = get_resource_lifecycle_state(boot_volume, "BootVolume")
                is_eligible = is_resource_backup_eligible("BootVolume", lifecycle_state)
                boot_item = build_discovery_item(
                    resource_type="BootVolume",
                    resource_id=boot_volume.id,
                    display_name=normalize_name(boot_volume, "boot-volume"),
                    compartment_id=boot_volume.compartment_id,
                    region=region,
                    source_tag_path=source_path,
                    planned_action="backup",
                    is_direct_tag_match=False,
                    derived_from_type="Instance",
                    derived_from_id=instance.id,
                    availability_domain=getattr(boot_volume, "availability_domain", instance_ad),
                    lifecycle_state=lifecycle_state,
                    is_backup_eligible=is_eligible,
                    skip_reason=None if is_eligible else build_skip_reason("BootVolume", lifecycle_state, "derived"),
                    executable=is_eligible,
                    reason=f"Derived from tagged instance {normalize_name(instance, 'instance')}",
                )
                if is_eligible:
                    add_item(boot_item)
                else:
                    add_skipped_item(boot_item)

            volume_attachments = list_all_results(
                compute_client.list_volume_attachments,
                compartment_id=compartment_id,
                instance_id=instance.id,
            )
            for attachment in volume_attachments:
                volume_id = getattr(attachment, "volume_id", None)
                if not volume_id:
                    continue
                volume = block_client.get_volume(volume_id).data
                lifecycle_state = get_resource_lifecycle_state(volume, "BlockVolume")
                is_eligible = is_resource_backup_eligible("BlockVolume", lifecycle_state)
                volume_item = build_discovery_item(
                    resource_type="BlockVolume",
                    resource_id=volume.id,
                    display_name=normalize_name(volume, "block-volume"),
                    compartment_id=volume.compartment_id,
                    region=region,
                    source_tag_path=source_path,
                    planned_action="backup",
                    is_direct_tag_match=False,
                    derived_from_type="Instance",
                    derived_from_id=instance.id,
                    availability_domain=getattr(volume, "availability_domain", None),
                    lifecycle_state=lifecycle_state,
                    is_backup_eligible=is_eligible,
                    skip_reason=None if is_eligible else build_skip_reason("BlockVolume", lifecycle_state, "derived"),
                    executable=is_eligible,
                    reason=f"Derived from tagged instance {normalize_name(instance, 'instance')}",
                )
                if is_eligible:
                    add_item(volume_item)
                else:
                    add_skipped_item(volume_item)

    if not discovered_items_to_list(items, ordered_keys):
        notes.append(f"No supported resources found with defined tag {source_path}.")

    return {
        "tag": {
            "namespace": namespace,
            "key": tag_key_name,
            "value": tag_value,
            "path": source_path,
        },
        "root_compartment_id": root_compartment_id,
        "include_subcompartments": include_subcompartments,
        "resources": discovered_items_to_list(items, ordered_keys),
        "skipped_resources": discovered_items_to_list(skipped_items, skipped_ordered_keys),
        "notes": notes,
    }


def group_resources(resources: Sequence[DiscoveredResource]) -> Dict[str, List[DiscoveredResource]]:
    grouped: Dict[str, List[DiscoveredResource]] = {}
    for resource in resources:
        grouped.setdefault(resource.resource_type, []).append(resource)
    return grouped


def is_dr_enabled_for_resource(plan: Dict[str, Any], resource_type: str) -> bool:
    dr_cfg = ensure_dict(plan.get("dr"), "dr")
    if not parse_bool(dr_cfg.get("enabled"), False):
        return False
    mapping = {
        "BlockVolume": "block_volumes",
        "BootVolume": "boot_volumes",
        "Bucket": "buckets",
        "MySQL": "mysql",
        "PostgreSQL": "postgresql",
    }
    service_key = mapping.get(resource_type)
    if not service_key:
        return False
    return parse_bool(ensure_dict(dr_cfg.get(service_key), f"dr.{service_key}").get("enabled"), True)


def build_planned_actions_for_resource(resource: DiscoveredResource, plan: Dict[str, Any]) -> List[str]:
    if resource.resource_type == "Instance":
        return ["source_only"]

    actions = ["backup"]
    if resource.resource_type in {"BlockVolume", "BootVolume", "Bucket", "MySQL", "PostgreSQL"} and is_dr_enabled_for_resource(plan, resource.resource_type):
        if resource.resource_type in {"Bucket", "MySQL", "PostgreSQL"}:
            actions.append("initial_dr_copy")
        actions.append("dr_config")
    if resource.resource_type == "MySQL" and parse_bool(ensure_dict(plan.get("mysql_read_replicas"), "mysql_read_replicas").get("enabled"), False):
        actions.append("mysql_read_replica")
    return actions


def enrich_discovery_with_plan_actions(discovery: Dict[str, Any], plan: Dict[str, Any]) -> Dict[str, Any]:
    for bucket in ("resources", "skipped_resources"):
        for resource in discovery.get(bucket, []):
            resource.planned_actions = build_planned_actions_for_resource(resource, plan)
            resource.planned_action = first_or_none(resource.planned_actions)
    return discovery


def print_discovery_table(resources: Sequence[DiscoveredResource]) -> None:
    grouped = group_resources(resources)
    if not grouped:
        print("No supported tagged resources found.")
        return

    for resource_type in sorted(grouped):
        print(f"\n[{resource_type}]")
        print(f"{'NAME':<28} {'STATE':<14} {'COMPARTMENT':<22} {'INCLUSION':<10} {'ACTIONS'}")
        print("-" * 132)
        for resource in grouped[resource_type]:
            inclusion = "direct" if resource.is_direct_tag_match else "derived"
            actions = ",".join(resource.planned_actions)
            name = resource.display_name[:26]
            state = (resource.lifecycle_state or "UNKNOWN")[:12]
            compartment = resource.compartment_id[:20]
            print(f"{name:<28} {state:<14} {compartment:<22} {inclusion:<10} {actions}")
            for reason in resource.inclusion_reasons:
                print(f"  reason: {reason}")


def print_skipped_discovery_table(resources: Sequence[DiscoveredResource]) -> None:
    if not resources:
        return

    grouped = group_resources(resources)
    print("\nSkipped tagged resources:")
    for resource_type in sorted(grouped):
        print(f"\n[{resource_type}]")
        print(f"{'NAME':<32} {'STATE':<14} {'INCLUSION':<14} {'REASON'}")
        print("-" * 120)
        for resource in grouped[resource_type]:
            inclusion = "direct" if resource.is_direct_tag_match else "derived"
            state = (resource.lifecycle_state or "UNKNOWN")[:12]
            reason = resource.skip_reason or "Not backup eligible."
            print(f"{resource.display_name[:30]:<32} {state:<14} {inclusion:<14} {reason}")


def print_run_results_table(results: Sequence[Dict[str, Any]]) -> None:
    if not results:
        print("No results to display.")
        return

    print(f"\n{'TYPE':<16} {'STATUS':<12} {'NAME':<28} {'ACTIONS'}")
    print("-" * 132)
    for result in results:
        detail = aggregate_result_detail(result.get("actions", [])) or result.get("detail") or result.get("result_id") or ""
        print(
            f"{result['resource_type']:<16} {result['status']:<12} "
            f"{result['display_name'][:26]:<28} {detail}"
        )


def create_backup_policy(config, signer, compartment_id: str, name: str,
                         retention_days: int = DEFAULT_RETENTION_DAYS):
    """Create an OCI volume backup policy with daily, weekly, and monthly schedules."""
    client = make_client(oci.core.BlockstorageClient, config, signer)

    schedules = [
        oci.core.models.VolumeBackupSchedule(
            backup_type=oci.core.models.VolumeBackupSchedule.BACKUP_TYPE_INCREMENTAL,
            period=oci.core.models.VolumeBackupSchedule.PERIOD_ONE_DAY,
            retention_seconds=7 * 86400,
            offset_seconds=0,
            time_zone=oci.core.models.VolumeBackupSchedule.TIME_ZONE_UTC,
        ),
        oci.core.models.VolumeBackupSchedule(
            backup_type=oci.core.models.VolumeBackupSchedule.BACKUP_TYPE_FULL,
            period=oci.core.models.VolumeBackupSchedule.PERIOD_ONE_WEEK,
            retention_seconds=4 * 7 * 86400,
            offset_seconds=0,
            time_zone=oci.core.models.VolumeBackupSchedule.TIME_ZONE_UTC,
        ),
        oci.core.models.VolumeBackupSchedule(
            backup_type=oci.core.models.VolumeBackupSchedule.BACKUP_TYPE_FULL,
            period=oci.core.models.VolumeBackupSchedule.PERIOD_ONE_MONTH,
            retention_seconds=retention_days * 86400,
            offset_seconds=0,
            time_zone=oci.core.models.VolumeBackupSchedule.TIME_ZONE_UTC,
        ),
    ]

    details = oci.core.models.CreateVolumeBackupPolicyDetails(
        compartment_id=compartment_id,
        display_name=name,
        schedules=schedules,
        defined_tags={},
        freeform_tags={"managed-by": "oci-backup-manager"},
    )

    resp = client.create_volume_backup_policy(details)
    policy = resp.data
    log.info("Created backup policy '%s' (OCID: %s)", policy.display_name, policy.id)
    return policy


def ensure_backup_policy(
    config: Dict[str, Any],
    signer: Any,
    compartment_id: str,
    policy_id: Optional[str],
    policy_name: str,
    retention_days: int,
):
    block_client = make_client(oci.core.BlockstorageClient, config, signer)

    if policy_id:
        return policy_id

    try:
        existing_policies = list_all_results(
            block_client.list_volume_backup_policies,
            compartment_id=compartment_id,
        )
    except Exception:
        existing_policies = []

    for policy in existing_policies:
        if getattr(policy, "display_name", None) == policy_name:
            log.info("Reusing volume backup policy '%s' (%s)", policy_name, policy.id)
            return policy.id

    policy = create_backup_policy(config, signer, compartment_id, policy_name, retention_days)
    return policy.id


def assign_policy_to_asset(
    config: Dict[str, Any],
    signer: Any,
    asset_id: str,
    policy_id: str,
) -> str:
    block_client = make_client(oci.core.BlockstorageClient, config, signer)
    try:
        assignment = block_client.create_volume_backup_policy_assignment(
            oci.core.models.CreateVolumeBackupPolicyAssignmentDetails(
                asset_id=asset_id,
                policy_id=policy_id,
            )
        ).data
        return getattr(assignment, "id", "created")
    except oci.exceptions.ServiceError as exc:
        if exc.status == 409:
            return "already-assigned"
        raise


def create_block_volume_backup(
    config: Dict[str, Any],
    signer: Any,
    volume_id: str,
    display_name: Optional[str] = None,
):
    block_client = make_client(oci.core.BlockstorageClient, config, signer)
    details = oci.core.models.CreateVolumeBackupDetails(
        volume_id=volume_id,
        display_name=display_name or f"block-backup-{utc_timestamp()}",
        type="INCREMENTAL",
        freeform_tags={"managed-by": "oci-backup-manager"},
    )
    return block_client.create_volume_backup(details).data


def create_boot_volume_backup(
    config: Dict[str, Any],
    signer: Any,
    boot_volume_id: str,
    display_name: Optional[str] = None,
):
    block_client = make_client(oci.core.BlockstorageClient, config, signer)
    details = oci.core.models.CreateBootVolumeBackupDetails(
        boot_volume_id=boot_volume_id,
        display_name=display_name or f"boot-backup-{utc_timestamp()}",
        type="INCREMENTAL",
        freeform_tags={"managed-by": "oci-backup-manager"},
    )
    return block_client.create_boot_volume_backup(details).data


def assign_policy_by_tag(config, signer, compartment_id: str,
                         tag_key: str, tag_value: str, policy_id: str):
    """
    Discover all block and boot volumes tagged with tag_key=tag_value
    and assign the backup policy to them.
    """
    block_client = make_client(oci.core.BlockstorageClient, config, signer)
    identity_client = make_client(oci.identity.IdentityClient, config, signer)

    assigned = 0

    volumes = list_all_results(
        block_client.list_volumes, compartment_id=compartment_id
    )
    for vol in volumes:
        tags = vol.freeform_tags or {}
        if tags.get(tag_key) == tag_value:
            try:
                block_client.create_volume_backup_policy_assignment(
                    oci.core.models.CreateVolumeBackupPolicyAssignmentDetails(
                        asset_id=vol.id, policy_id=policy_id
                    )
                )
                log.info("Assigned policy to block volume: %s (%s)", vol.display_name, vol.id)
                assigned += 1
            except oci.exceptions.ServiceError as exc:
                if exc.status == 409:
                    log.info("Policy already assigned to %s", vol.display_name)
                else:
                    log.warning("Failed to assign to %s: %s", vol.display_name, exc.message)

    ads = identity_client.list_availability_domains(compartment_id=compartment_id).data
    for ad in ads:
        boot_vols = list_all_results(
            block_client.list_boot_volumes,
            availability_domain=ad.name,
            compartment_id=compartment_id,
        )
        for bv in boot_vols:
            tags = bv.freeform_tags or {}
            if tags.get(tag_key) == tag_value:
                try:
                    block_client.create_volume_backup_policy_assignment(
                        oci.core.models.CreateVolumeBackupPolicyAssignmentDetails(
                            asset_id=bv.id, policy_id=policy_id
                        )
                    )
                    log.info("Assigned policy to boot volume: %s (%s)", bv.display_name, bv.id)
                    assigned += 1
                except oci.exceptions.ServiceError as exc:
                    if exc.status == 409:
                        log.info("Policy already assigned to %s", bv.display_name)
                    else:
                        log.warning("Failed to assign to %s: %s", bv.display_name, exc.message)

    log.info("Policy assigned to %d resource(s).", assigned)
    return assigned


def list_backups(config, signer, compartment_id: str):
    """List all volume backups, boot volume backups, and database backups in a compartment."""
    block_client = make_client(oci.core.BlockstorageClient, config, signer)

    results = []

    vol_backups = list_all_results(
        block_client.list_volume_backups, compartment_id=compartment_id
    )
    for backup in vol_backups:
        results.append({
            "type": "BlockVolume",
            "name": backup.display_name,
            "id": backup.id,
            "state": backup.lifecycle_state,
            "size_gb": backup.size_in_gbs,
            "time_created": str(backup.time_created),
            "source_id": backup.volume_id,
        })

    boot_backups = list_all_results(
        block_client.list_boot_volume_backups, compartment_id=compartment_id
    )
    for backup in boot_backups:
        results.append({
            "type": "BootVolume",
            "name": backup.display_name,
            "id": backup.id,
            "state": backup.lifecycle_state,
            "size_gb": backup.size_in_gbs,
            "time_created": str(backup.time_created),
            "source_id": backup.boot_volume_id,
        })

    try:
        db_client = make_client(oci.database.DatabaseClient, config, signer)
        adb_list = list_all_results(
            db_client.list_autonomous_databases, compartment_id=compartment_id
        )
        for adb in adb_list:
            adb_backups = list_all_results(
                db_client.list_autonomous_database_backups,
                autonomous_database_id=adb.id,
            )
            for backup in adb_backups:
                results.append({
                    "type": "AutonomousDB",
                    "name": backup.display_name,
                    "id": backup.id,
                    "state": backup.lifecycle_state,
                    "size_gb": backup.database_size_in_tbs * 1024 if backup.database_size_in_tbs else "N/A",
                    "time_created": str(backup.time_started),
                    "source_id": adb.id,
                })
    except Exception as exc:
        log.warning("Could not list Autonomous DB backups: %s", exc)

    try:
        mysql_backup_client = make_client(oci.mysql.DbBackupsClient, config, signer)
        mysql_backups = list_all_results(
            mysql_backup_client.list_backups, compartment_id=compartment_id
        )
        for backup in mysql_backups:
            results.append({
                "type": "MySQL",
                "name": backup.display_name,
                "id": backup.id,
                "state": backup.lifecycle_state,
                "size_gb": backup.backup_size_in_gbs,
                "time_created": str(backup.time_created),
                "source_id": backup.db_system_id,
            })
    except Exception as exc:
        log.warning("Could not list MySQL backups: %s", exc)

    try:
        psql_client = make_client(oci.psql.PostgresqlClient, config, signer)
        psql_backups = list_all_results(
            psql_client.list_backups, compartment_id=compartment_id
        )
        for backup in psql_backups:
            results.append({
                "type": "PostgreSQL",
                "name": normalize_name(backup, "postgresql-backup"),
                "id": backup.id,
                "state": get_resource_lifecycle_state(backup, "PostgreSQL"),
                "size_gb": getattr(backup, "size_in_gbs", "N/A"),
                "time_created": str(getattr(backup, "time_created", "")),
                "source_id": getattr(backup, "db_system_id", ""),
            })
    except Exception as exc:
        log.warning("Could not list PostgreSQL backups: %s", exc)

    print(f"\n{'TYPE':<16} {'STATE':<12} {'SIZE(GB)':<10} {'NAME':<40} {'CREATED':<28}")
    print("-" * 110)
    for row in results:
        print(f"{row['type']:<16} {row['state']:<12} {str(row['size_gb']):<10} {row['name'][:38]:<40} {row['time_created'][:25]}")
    print(f"\nTotal: {len(results)} backup(s) found.")
    return results


def copy_backup_cross_region(config, signer, backup_id: str,
                             resource_type: str, dest_region: str = CROSS_REGION,
                             display_name: Optional[str] = None):
    """Copy a block volume or boot volume backup to another region."""
    block_client = make_client(oci.core.BlockstorageClient, config, signer)
    name = display_name or f"xregion-copy-{utc_timestamp()}"

    if resource_type == "block_volume":
        details = oci.core.models.CopyVolumeBackupDetails(
            destination_region=dest_region,
            display_name=name,
        )
        copy = block_client.copy_volume_backup(backup_id, details).data
        log.info("Cross-region block volume copy initiated to %s (%s)", dest_region, copy.id)
        return copy

    if resource_type == "boot_volume":
        details = oci.core.models.CopyBootVolumeBackupDetails(
            destination_region=dest_region,
            display_name=name,
        )
        copy = block_client.copy_boot_volume_backup(backup_id, details).data
        log.info("Cross-region boot volume copy initiated to %s (%s)", dest_region, copy.id)
        return copy

    raise ValueError(f"Unsupported resource_type: {resource_type}. Use 'block_volume' or 'boot_volume'.")


def create_object_storage_backup(config, signer, namespace: str,
                                 source_bucket: str, dest_bucket: str,
                                 archive_after_days: int = ARCHIVE_AFTER_DAYS,
                                 dest_compartment_id: Optional[str] = None):
    """
    Copy all objects from source_bucket -> dest_bucket and apply a lifecycle
    rule to transition objects to Archive storage after N days.
    """
    os_client = make_client(oci.object_storage.ObjectStorageClient, config, signer)
    if not dest_compartment_id:
        dest_compartment_id = get_tenancy_id(config, signer)

    try:
        os_client.get_bucket(namespace, dest_bucket)
        log.info("Destination bucket '%s' already exists.", dest_bucket)
    except oci.exceptions.ServiceError as exc:
        if exc.status == 404:
            os_client.create_bucket(
                namespace,
                oci.object_storage.models.CreateBucketDetails(
                    name=dest_bucket,
                    compartment_id=dest_compartment_id,
                    storage_tier="Standard",
                    freeform_tags={"managed-by": "oci-backup-manager"},
                ),
            )
            log.info("Created destination bucket: %s", dest_bucket)
        else:
            raise

    lifecycle_rule = oci.object_storage.models.ObjectLifecycleRule(
        name="archive-old-backups",
        action="ARCHIVE",
        time_amount=archive_after_days,
        time_unit=oci.object_storage.models.ObjectLifecycleRule.TIME_UNIT_DAYS,
        is_enabled=True,
        target="objects",
    )
    os_client.put_object_lifecycle_policy(
        namespace,
        dest_bucket,
        oci.object_storage.models.PutObjectLifecyclePolicyDetails(items=[lifecycle_rule]),
    )

    objects = list_all_results(
        os_client.list_objects, namespace_name=namespace, bucket_name=source_bucket
    ).objects or []

    copied = 0
    for obj in objects:
        os_client.copy_object(
            namespace,
            source_bucket,
            oci.object_storage.models.CopyObjectDetails(
                source_object_name=obj.name,
                destination_region=get_region(config, signer),
                destination_namespace=namespace,
                destination_bucket=dest_bucket,
                destination_object_name=obj.name,
            ),
        )
        copied += 1

    log.info("Initiated copy of %d object(s) to '%s'.", copied, dest_bucket)
    return copied


def backup_database(config, signer, db_id: str, db_type: str,
                    display_name: Optional[str] = None):
    """Trigger an on-demand backup for Autonomous DB or MySQL HeatWave."""
    name = display_name or f"manual-backup-{utc_timestamp()}"

    if db_type == "autonomous":
        db_client = make_client(oci.database.DatabaseClient, config, signer)
        details = oci.database.models.CreateAutonomousDatabaseBackupDetails(
            autonomous_database_id=db_id,
            display_name=name,
            is_long_term_backup=False,
        )
        backup = db_client.create_autonomous_database_backup(details).data
        log.info("Autonomous DB backup triggered: %s (%s)", backup.display_name, backup.id)
        return backup

    if db_type == "mysql":
        mysql_client = make_client(oci.mysql.DbBackupsClient, config, signer)
        details = oci.mysql.models.CreateBackupDetails(
            display_name=name,
            backup_type=oci.mysql.models.CreateBackupDetails.BACKUP_TYPE_FULL,
            db_system_id=db_id,
            retention_in_days=DEFAULT_RETENTION_DAYS,
            freeform_tags={"managed-by": "oci-backup-manager"},
        )
        backup = mysql_client.create_backup(details).data
        log.info("MySQL backup triggered: %s (%s)", backup.display_name, backup.id)
        return backup

    if db_type == "postgresql":
        psql_client = make_client(oci.psql.PostgresqlClient, config, signer)
        db_system = psql_client.get_db_system(db_id).data
        details = oci.psql.models.CreateBackupDetails(
            compartment_id=getattr(db_system, "compartment_id", None),
            db_system_id=db_id,
            display_name=name,
        )
        response = psql_client.create_backup(details)
        backup = getattr(response, "data", None)
        if backup is None:
            work_request_id = getattr(response, "headers", {}).get("opc-work-request-id")
            backup = wait_for_postgresql_backup_resource(
                psql_client=psql_client,
                compartment_id=getattr(db_system, "compartment_id", None),
                db_system_id=db_id,
                display_name=name,
                work_request_id=work_request_id,
            )
            if backup is None:
                backup = SimpleNamespace(
                    id=None,
                    display_name=name,
                    work_request_id=work_request_id,
                    compartment_id=getattr(db_system, "compartment_id", None),
                )
        log.info(
            "PostgreSQL backup triggered: %s (%s)",
            normalize_name(backup, "postgresql-backup"),
            getattr(backup, "id", None) or getattr(backup, "work_request_id", "unknown"),
        )
        return backup

    raise ValueError(f"Unsupported db_type: {db_type}. Use 'autonomous', 'mysql', or 'postgresql'.")


def get_destination_availability_domains(config: Dict[str, Any], signer: Any, region: str) -> List[str]:
    identity_client = make_client(oci.identity.IdentityClient, config, signer, region=region)
    tenancy_id = get_tenancy_id(config, signer)
    return [ad.name for ad in identity_client.list_availability_domains(compartment_id=tenancy_id).data]


def ensure_bucket_exists(config: Dict[str, Any], signer: Any, namespace: str, bucket_name: str,
                         compartment_id: str, region: Optional[str] = None) -> None:
    client = make_client(oci.object_storage.ObjectStorageClient, config, signer, region=region)
    try:
        client.get_bucket(namespace, bucket_name)
        return
    except oci.exceptions.ServiceError as exc:
        if exc.status != 404:
            raise
    client.create_bucket(
        namespace,
        oci.object_storage.models.CreateBucketDetails(
            name=bucket_name,
            compartment_id=compartment_id,
            storage_tier="Standard",
            freeform_tags={"managed-by": "oci-backup-manager"},
        ),
    )


def configure_bucket_replication(config: Dict[str, Any], signer: Any, namespace: str, source_bucket: str,
                                 dest_bucket: str, dest_region: str) -> Dict[str, Any]:
    client = make_client(oci.object_storage.ObjectStorageClient, config, signer)
    for policy in get_bucket_replication_policies(config, signer, namespace, source_bucket):
        if getattr(policy, "destination_bucket_name", None) == dest_bucket and getattr(policy, "destination_region_name", None) == dest_region:
            return {"status": "skipped", "detail": "Replication policy already configured."}
    details = oci.object_storage.models.CreateReplicationPolicyDetails(
        name=f"{source_bucket}-to-{dest_region}",
        destination_region_name=dest_region,
        destination_bucket_name=dest_bucket,
    )
    policy = client.create_replication_policy(namespace, source_bucket, details).data
    return {"status": "succeeded", "detail": "Replication policy created.", "result_id": getattr(policy, "id", None)}


def get_bucket_replication_policies(config: Dict[str, Any], signer: Any, namespace: str, source_bucket: str) -> List[Any]:
    client = make_client(oci.object_storage.ObjectStorageClient, config, signer)
    try:
        return client.list_replication_policies(namespace, source_bucket).data or []
    except AttributeError:
        return []


def seed_bucket_initial_dr_copy(config: Dict[str, Any], signer: Any, namespace: str, source_bucket: str,
                                dest_bucket: str, dest_region: str) -> Dict[str, Any]:
    client = make_client(oci.object_storage.ObjectStorageClient, config, signer)
    objects = list_all_results(
        client.list_objects, namespace_name=namespace, bucket_name=source_bucket
    ).objects or []
    copied = 0
    for obj in objects:
        client.copy_object(
            namespace,
            source_bucket,
            oci.object_storage.models.CopyObjectDetails(
                source_object_name=obj.name,
                destination_region=dest_region,
                destination_namespace=namespace,
                destination_bucket=dest_bucket,
                destination_object_name=obj.name,
            ),
        )
        copied += 1
    return {"status": "succeeded", "detail": f"Seeded {copied} object(s) to DR bucket.", "result_id": dest_bucket}


def configure_volume_dr_replication(config: Dict[str, Any], signer: Any, resource: DiscoveredResource,
                                    target_region: str) -> Dict[str, Any]:
    destination_ads = get_destination_availability_domains(config, signer, target_region)
    target_ad = find_matching_destination_ad(resource.availability_domain, destination_ads)
    if not target_ad:
        return {"status": "skipped", "detail": "No matching destination availability domain for DR replication."}

    block_client = make_client(oci.core.BlockstorageClient, config, signer)
    replica_display_name = f"{resource.display_name}-{target_region}-replica"
    if resource.resource_type == "BlockVolume":
        details = oci.core.models.UpdateVolumeDetails(
            block_volume_replicas=[
                oci.core.models.BlockVolumeReplicaDetails(
                    availability_domain=target_ad,
                    display_name=replica_display_name,
                )
            ]
        )
        response = block_client.update_volume(resource.resource_id, details).data
    else:
        details = oci.core.models.UpdateBootVolumeDetails(
            boot_volume_replicas=[
                oci.core.models.BootVolumeReplicaDetails(
                    availability_domain=target_ad,
                    display_name=replica_display_name,
                )
            ]
        )
        response = block_client.update_boot_volume(resource.resource_id, details).data
    return {"status": "succeeded", "detail": f"Replication configured in {target_ad}.", "result_id": getattr(response, "id", None)}


def copy_mysql_backup_to_region(config: Dict[str, Any], signer: Any, backup: Any, dest_region: str,
                                retention_days: int) -> Dict[str, Any]:
    client = make_client(oci.mysql.DbBackupsClient, config, signer)
    details = oci.mysql.models.CopyBackupDetails(
        compartment_id=getattr(backup, "compartment_id", None),
        source_backup_id=backup.id,
        source_region=get_region(config, signer),
        display_name=f"{normalize_name(backup, 'mysql-backup')}-{dest_region}",
        backup_copy_retention_in_days=retention_days,
    )
    copied = client.copy_backup(details).data
    return {"status": "succeeded", "detail": f"Backup copied to {dest_region}.", "result_id": getattr(copied, "id", None)}


def configure_mysql_dr_backup_policy(config: Dict[str, Any], signer: Any, db_system_id: str,
                                     dest_region: str, retention_days: int) -> Dict[str, Any]:
    client = make_client(oci.mysql.DbSystemClient, config, signer)
    db_system = client.get_db_system(db_system_id).data
    existing_policy = getattr(db_system, "backup_policy", None)
    backup_policy = oci.mysql.models.UpdateBackupPolicyDetails(
        is_enabled=True,
        soft_delete=getattr(existing_policy, "soft_delete", None),
        retention_in_days=getattr(existing_policy, "retention_in_days", retention_days),
        window_start_time=getattr(existing_policy, "window_start_time", None),
        pitr_policy=getattr(existing_policy, "pitr_policy", None),
        copy_policies=[
            oci.mysql.models.CopyPolicy(
                copy_to_region=dest_region,
                backup_copy_retention_in_days=retention_days,
            )
        ],
    )
    updated = client.update_db_system(
        db_system_id,
        oci.mysql.models.UpdateDbSystemDetails(backup_policy=backup_policy),
    ).data
    return {"status": "succeeded", "detail": f"MySQL backup copy policy configured for {dest_region}.", "result_id": getattr(updated, "id", None)}


def is_mysql_read_replica_supported(config: Dict[str, Any], signer: Any, db_system: Any) -> Optional[str]:
    shape_name = str(getattr(db_system, "shape_name", "") or "")
    if ".FREE" in shape_name.upper():
        return "MySQL read replicas are not supported on Always Free shapes."

    numeric_parts = [int(part) for part in re.findall(r"\.(\d+)(?:\.|$)", shape_name)]
    if numeric_parts and max(numeric_parts) < 4 and "ECPU" not in shape_name.upper():
        return "MySQL read replicas require at least 4 OCPUs or 8 ECPUs."

    subnet_id = getattr(db_system, "subnet_id", None)
    if subnet_id:
        vcn_client = make_client(oci.core.VirtualNetworkClient, config, signer)
        subnet = vcn_client.get_subnet(subnet_id).data
        if getattr(subnet, "ipv6_cidr_block", None) or getattr(subnet, "ipv6cidr_blocks", None):
            return "MySQL read replicas are not supported on IPv6-enabled subnets."
    return None


def create_mysql_read_replicas(config: Dict[str, Any], signer: Any, db_system_id: str, plan: Dict[str, Any]) -> Dict[str, Any]:
    db_client = make_client(oci.mysql.DbSystemClient, config, signer)
    replica_client = make_client(oci.mysql.ReplicasClient, config, signer)
    db_system = db_client.get_db_system(db_system_id).data
    unsupported_reason = is_mysql_read_replica_supported(config, signer, db_system)
    if unsupported_reason:
        return {"status": "skipped", "detail": unsupported_reason}

    overrides_cfg = ensure_dict(plan["mysql_read_replicas"].get("overrides"), "mysql_read_replicas.overrides")
    replica_count = int(plan["mysql_read_replicas"].get("replica_count", 1))
    replica_ids: List[str] = []
    base_name = normalize_name(db_system, "mysql")
    suffix = overrides_cfg.get("display_name_suffix", DEFAULT_MYSQL_REPLICA_SUFFIX)
    for index in range(replica_count):
        display_name = f"{base_name}{suffix}"
        if replica_count > 1:
            display_name = f"{display_name}-{index + 1}"
        replica_details = oci.mysql.models.CreateReplicaDetails(
            db_system_id=db_system_id,
            display_name=display_name,
            replica_overrides=oci.mysql.models.ReplicaOverrides(
                mysql_version=overrides_cfg.get("mysql_version"),
                shape_name=overrides_cfg.get("shape_name"),
                configuration_id=overrides_cfg.get("configuration_id"),
            ),
        )
        replica = replica_client.create_replica(replica_details).data
        replica_ids.append(str(getattr(replica, "id", "")))
    return {
        "status": "succeeded",
        "detail": f"MySQL read replica requested ({replica_count}).",
        "result_id": ",".join(replica_ids),
    }


def configure_postgresql_dr(config: Dict[str, Any], signer: Any, db_system_id: str, dest_region: str,
                            retention_days: int) -> Dict[str, Any]:
    client = make_client(oci.psql.PostgresqlClient, config, signer)
    db_system = client.get_db_system(db_system_id).data
    unsupported_reason = get_postgresql_dr_skip_reason(db_system)
    if unsupported_reason:
        return {"status": "skipped", "detail": unsupported_reason}

    management_policy = getattr(db_system, "management_policy", None)
    backup_policy = getattr(management_policy, "backup_policy", None)
    backup_start = getattr(backup_policy, "backup_start", "02:00")
    retention = getattr(backup_policy, "retention_days", retention_days)
    maintenance_window_start = getattr(management_policy, "maintenance_window_start", None)
    updated = client.update_db_system(
        db_system_id,
        oci.psql.models.UpdateDbSystemDetails(
            management_policy=oci.psql.models.ManagementPolicyDetails(
                maintenance_window_start=maintenance_window_start,
                backup_policy=oci.psql.models.DailyBackupPolicy(
                    kind="DAILY",
                    backup_start=backup_start,
                    retention_days=retention,
                    copy_policy=oci.psql.models.BackupCopyPolicy(
                        compartment_id=getattr(db_system, "compartment_id", None),
                        retention_period=retention,
                        regions=[dest_region],
                    ),
                ),
            ),
        ),
    ).data
    return {"status": "succeeded", "detail": f"PostgreSQL backup copy policy configured for {dest_region}.", "result_id": getattr(updated, "id", None)}


def get_postgresql_dr_skip_reason(db_system: Any) -> Optional[str]:
    storage_details = getattr(db_system, "storage_details", None)
    data_storage_type = str(
        getattr(storage_details, "data_storage_type", "") or getattr(db_system, "data_storage_type", "") or ""
    ).upper()
    if data_storage_type and "REGIONAL" in data_storage_type:
        return "PostgreSQL backup copy DR requires availability-domain-specific data placement."
    return None


def copy_postgresql_backup_to_region(config: Dict[str, Any], signer: Any, backup: Any, dest_region: str,
                                     retention_days: int) -> Dict[str, Any]:
    client = make_client(oci.psql.PostgresqlClient, config, signer)
    response = client.backup_copy(
        backup.id,
        oci.psql.models.BackupCopyDetails(
            compartment_id=getattr(backup, "compartment_id", None),
            retention_period=retention_days,
            regions=[dest_region],
        ),
    )
    return {"status": "succeeded", "detail": f"Backup copied to {dest_region}.", "result_id": getattr(response, "opc_request_id", None)}


def wait_for_postgresql_backup_resource(
    psql_client: Any,
    compartment_id: str,
    db_system_id: str,
    display_name: str,
    work_request_id: Optional[str],
    timeout_seconds: int = POSTGRESQL_WORK_REQUEST_TIMEOUT_SECONDS,
    poll_seconds: int = POSTGRESQL_WORK_REQUEST_POLL_SECONDS,
) -> Optional[Any]:
    deadline = time.time() + timeout_seconds
    while time.time() < deadline:
        if work_request_id:
            work_request = psql_client.get_work_request(work_request_id).data
            work_request_status = str(getattr(work_request, "status", "") or "").upper()
            if work_request_status in {"FAILED", "CANCELED"}:
                raise RuntimeError(f"PostgreSQL backup work request {work_request_id} finished with status {work_request_status}.")

        backups = list_all_results(
            psql_client.list_backups,
            compartment_id=compartment_id,
            id=db_system_id,
            display_name=display_name,
        )
        if backups:
            backup_summary = sorted(
                backups,
                key=lambda item: str(getattr(item, "time_created", "") or ""),
                reverse=True,
            )[0]
            try:
                return psql_client.get_backup(backup_summary.id).data
            except Exception:
                return backup_summary

        time.sleep(poll_seconds)

    return None


def monitor_backups(config, signer, compartment_id: str,
                    topic_id: Optional[str] = None, poll_seconds: int = 30):
    """Poll block and boot volume backups and optionally publish failure alerts."""
    block_client = make_client(oci.core.BlockstorageClient, config, signer)
    ons_client = make_client(oci.ons.NotificationDataPlaneClient, config, signer) if topic_id else None

    def notify(subject: str, body: str):
        if ons_client and topic_id:
            ons_client.publish_message(
                topic_id,
                oci.ons.models.MessageDetails(title=subject, body=body),
            )
            log.info("Notification sent: %s", subject)

    log.info("Monitoring backups in compartment %s (polling every %ds)...", compartment_id, poll_seconds)

    seen_failed = set()

    while True:
        vol_backups = list_all_results(
            block_client.list_volume_backups, compartment_id=compartment_id
        )
        boot_backups = list_all_results(
            block_client.list_boot_volume_backups, compartment_id=compartment_id
        )

        all_backups = [(backup, "BlockVolume") for backup in vol_backups] + [
            (backup, "BootVolume") for backup in boot_backups
        ]

        now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
        print(f"\n[{now}] Backup Status Summary")
        print(f"{'TYPE':<14} {'STATE':<14} {'NAME'}")
        print("-" * 60)

        for backup, backup_type in all_backups:
            state = backup.lifecycle_state
            print(f"  {backup_type:<12} {state:<14} {backup.display_name}")

            if state == "FAULTY" and backup.id not in seen_failed:
                seen_failed.add(backup.id)
                notify(
                    f"OCI Backup FAILED: {backup.display_name}",
                    f"Type: {backup_type}\nOCID: {backup.id}\nState: {state}\nTime: {backup.time_created}",
                )

        time.sleep(poll_seconds)


def restore_backup(config, signer, backup_id: str, resource_type: str,
                   availability_domain: str, display_name: Optional[str] = None,
                   compartment_id: Optional[str] = None):
    """Restore a block volume or boot volume backup to a new volume."""
    block_client = make_client(oci.core.BlockstorageClient, config, signer)
    name = display_name or f"restored-{utc_timestamp()}"

    if resource_type == "block_volume":
        backup = block_client.get_volume_backup(backup_id).data
        details = oci.core.models.CreateVolumeDetails(
            availability_domain=availability_domain,
            compartment_id=compartment_id or backup.compartment_id,
            display_name=name,
            source_details=oci.core.models.VolumeSourceFromVolumeBackupDetails(
                type="volumeBackup",
                id=backup_id,
            ),
            freeform_tags={"restored-from": backup_id, "managed-by": "oci-backup-manager"},
        )
        vol = block_client.create_volume(details).data
        log.info("Block volume restore initiated: %s (%s)", vol.display_name, vol.id)
        return vol

    if resource_type == "boot_volume":
        backup = block_client.get_boot_volume_backup(backup_id).data
        details = oci.core.models.CreateBootVolumeDetails(
            availability_domain=availability_domain,
            compartment_id=compartment_id or backup.compartment_id,
            display_name=name,
            source_details=oci.core.models.BootVolumeSourceFromBootVolumeBackupDetails(
                type="bootVolumeBackup",
                id=backup_id,
            ),
            freeform_tags={"restored-from": backup_id, "managed-by": "oci-backup-manager"},
        )
        bv = block_client.create_boot_volume(details).data
        log.info("Boot volume restore initiated: %s (%s)", bv.display_name, bv.id)
        return bv

    raise ValueError(f"Unsupported resource_type: {resource_type}")


def default_plan_dict() -> Dict[str, Any]:
    return {
        "auth": {
            "method": "config_file",
            "profile": "DEFAULT",
        },
        "defaults": {
            "dest_region": CROSS_REGION,
            "archive_after_days": ARCHIVE_AFTER_DAYS,
            "automatic_backup_retention_days": DEFAULT_AUTOMATIC_BACKUP_RETENTION_DAYS,
            "volume_policy": {
                "name": DEFAULT_POLICY_NAME,
                "retention_days": DEFAULT_RETENTION_DAYS,
                "policy_id": None,
            },
        },
        "alerts": {
            "topic_id": None,
        },
        "discovery": {
            "mode": "defined_tag",
            "include_subcompartments": False,
            "expand_tagged_instances": True,
            "tag": {
                "namespace": DEFAULT_TAG_NAMESPACE,
                "key": DEFAULT_TAG_KEY,
                "value": DEFAULT_TAG_VALUE,
            },
        },
        "services": {
            "block_volumes": {
                "enabled": True,
            },
            "boot_volumes": {
                "enabled": True,
            },
            "object_storage": {
                "enabled": True,
            },
            "autonomous_db": {
                "enabled": True,
            },
            "mysql": {
                "enabled": True,
            },
            "postgresql": {
                "enabled": True,
            },
        },
        "dr": {
            "enabled": False,
            "target_region": CROSS_REGION,
            "block_volumes": {
                "enabled": True,
                "target_ad_strategy": "match_source_ordinal",
            },
            "boot_volumes": {
                "enabled": True,
                "target_ad_strategy": "match_source_ordinal",
            },
            "buckets": {
                "enabled": True,
                "destination_bucket_suffix": DEFAULT_DR_BUCKET_SUFFIX,
                "seed_existing_objects_on_first_enablement": True,
            },
            "mysql": {
                "enabled": True,
                "backup_copy_enabled": True,
                "immediate_copy_new_manual_backup": True,
            },
            "postgresql": {
                "enabled": True,
                "backup_copy_enabled": True,
                "immediate_copy_new_manual_backup": True,
            },
        },
        "mysql_read_replicas": {
            "enabled": False,
            "replica_count": 1,
            "overrides": {
                "display_name_suffix": DEFAULT_MYSQL_REPLICA_SUFFIX,
                "shape_name": None,
                "configuration_id": None,
                "mysql_version": None,
            },
        },
    }


def deep_merge(base: Dict[str, Any], overrides: Dict[str, Any]) -> Dict[str, Any]:
    merged = dict(base)
    for key, value in overrides.items():
        if isinstance(value, dict) and isinstance(merged.get(key), dict):
            merged[key] = deep_merge(merged[key], value)
        else:
            merged[key] = value
    return merged


def load_plan_file(plan_path: str) -> Dict[str, Any]:
    path = Path(plan_path)
    if not path.exists():
        raise FileNotFoundError(f"Plan file not found: {plan_path}")

    raw_text = path.read_text(encoding="utf-8")
    suffix = path.suffix.lower()

    if suffix == ".json":
        data = json.loads(raw_text)
    elif suffix in {".yaml", ".yml"}:
        if yaml is None:
            raise RuntimeError("PyYAML is required to read YAML plans. Install it with `pip install pyyaml`.")
        data = yaml.safe_load(raw_text)
    else:
        try:
            data = json.loads(raw_text)
        except json.JSONDecodeError:
            if yaml is None:
                raise RuntimeError("Unsupported plan format. Use JSON or install PyYAML for YAML plans.")
            data = yaml.safe_load(raw_text)

    if not isinstance(data, dict):
        raise PlanValidationError("Plan file must contain an object at the top level.")
    return data


def validate_and_normalize_plan(plan: Dict[str, Any]) -> Dict[str, Any]:
    merged = deep_merge(default_plan_dict(), plan)

    auth = ensure_dict(merged.get("auth"), "auth")
    defaults = ensure_dict(merged.get("defaults"), "defaults")
    alerts = ensure_dict(merged.get("alerts"), "alerts")
    discovery = ensure_dict(merged.get("discovery"), "discovery")
    services = ensure_dict(merged.get("services"), "services")
    dr = ensure_dict(merged.get("dr"), "dr")
    mysql_read_replicas = ensure_dict(merged.get("mysql_read_replicas"), "mysql_read_replicas")
    volume_policy = ensure_dict(defaults.get("volume_policy"), "defaults.volume_policy")
    tag = ensure_dict(discovery.get("tag"), "discovery.tag")

    if discovery.get("mode") != "defined_tag":
        raise PlanValidationError("Only `discovery.mode = defined_tag` is supported in v2.")

    for field_name in ("namespace", "key", "value"):
        if not str(tag.get(field_name, "")).strip():
            raise PlanValidationError(f"`discovery.tag.{field_name}` must be a non-empty string.")

    for service_key in ("block_volumes", "boot_volumes", "object_storage", "autonomous_db", "mysql", "postgresql"):
        service_cfg = ensure_dict(services.get(service_key), f"services.{service_key}")
        service_cfg["enabled"] = parse_bool(service_cfg.get("enabled"), True)
        services[service_key] = service_cfg

    legacy_volume_dr_enabled = any(
        parse_bool(services[service_key].get("copy_to_region_enabled"), False)
        for service_key in ("block_volumes", "boot_volumes")
    )

    dr["enabled"] = parse_bool(dr.get("enabled"), False)
    dr["target_region"] = dr.get("target_region") or defaults.get("dest_region") or CROSS_REGION
    for dr_key in ("block_volumes", "boot_volumes", "buckets", "mysql", "postgresql"):
        dr_cfg = ensure_dict(dr.get(dr_key), f"dr.{dr_key}")
        dr_cfg["enabled"] = parse_bool(dr_cfg.get("enabled"), True)
        dr[dr_key] = dr_cfg
    dr["buckets"]["destination_bucket_suffix"] = dr["buckets"].get("destination_bucket_suffix") or DEFAULT_DR_BUCKET_SUFFIX
    dr["buckets"]["seed_existing_objects_on_first_enablement"] = parse_bool(
        dr["buckets"].get("seed_existing_objects_on_first_enablement"), True
    )
    for db_key in ("mysql", "postgresql"):
        dr[db_key]["backup_copy_enabled"] = parse_bool(dr[db_key].get("backup_copy_enabled"), True)
        dr[db_key]["immediate_copy_new_manual_backup"] = parse_bool(dr[db_key].get("immediate_copy_new_manual_backup"), True)
    if legacy_volume_dr_enabled:
        dr["enabled"] = True
        dr["block_volumes"]["enabled"] = True
        dr["boot_volumes"]["enabled"] = True

    mysql_read_replicas["enabled"] = parse_bool(mysql_read_replicas.get("enabled"), False)
    mysql_read_replicas["replica_count"] = int(mysql_read_replicas.get("replica_count", 1))
    mysql_read_replicas["overrides"] = ensure_dict(mysql_read_replicas.get("overrides"), "mysql_read_replicas.overrides")
    mysql_read_replicas["overrides"]["display_name_suffix"] = (
        mysql_read_replicas["overrides"].get("display_name_suffix") or DEFAULT_MYSQL_REPLICA_SUFFIX
    )

    discovery["include_subcompartments"] = parse_bool(discovery.get("include_subcompartments"), False)
    discovery["expand_tagged_instances"] = parse_bool(discovery.get("expand_tagged_instances"), True)

    defaults["archive_after_days"] = int(defaults.get("archive_after_days", ARCHIVE_AFTER_DAYS))
    defaults["dest_region"] = defaults.get("dest_region") or CROSS_REGION
    defaults["automatic_backup_retention_days"] = int(
        defaults.get("automatic_backup_retention_days", DEFAULT_AUTOMATIC_BACKUP_RETENTION_DAYS)
    )
    volume_policy["retention_days"] = int(volume_policy.get("retention_days", DEFAULT_RETENTION_DAYS))
    volume_policy["name"] = volume_policy.get("name") or DEFAULT_POLICY_NAME
    volume_policy["policy_id"] = volume_policy.get("policy_id")
    defaults["volume_policy"] = volume_policy

    auth["method"] = auth.get("method") or "config_file"
    auth["profile"] = auth.get("profile") or "DEFAULT"
    alerts["topic_id"] = alerts.get("topic_id")

    merged["auth"] = auth
    merged["defaults"] = defaults
    merged["alerts"] = alerts
    merged["discovery"] = discovery
    merged["services"] = services
    merged["dr"] = dr
    merged["mysql_read_replicas"] = mysql_read_replicas

    return merged


def build_inline_plan_from_args(args: argparse.Namespace) -> Dict[str, Any]:
    dr_enabled = args.enable_dr or args.enable_cross_region_copy
    return validate_and_normalize_plan({
        "auth": {
            "method": args.auth,
            "profile": args.profile,
        },
        "defaults": {
            "dest_region": args.dr_region or args.dest_region,
            "archive_after_days": args.archive_after_days,
            "volume_policy": {
                "name": args.policy_name,
                "retention_days": args.retention_days,
                "policy_id": args.policy_id,
            },
        },
        "alerts": {
            "topic_id": getattr(args, "topic_id", None),
        },
        "discovery": {
            "mode": "defined_tag",
            "include_subcompartments": args.include_subcompartments,
            "expand_tagged_instances": True,
            "tag": {
                "namespace": DEFAULT_TAG_NAMESPACE,
                "key": DEFAULT_TAG_KEY,
                "value": DEFAULT_TAG_VALUE,
            },
        },
        "services": {
            "block_volumes": {
                "enabled": True,
            },
            "boot_volumes": {
                "enabled": True,
            },
            "object_storage": {
                "enabled": True,
            },
            "autonomous_db": {
                "enabled": True,
            },
            "mysql": {
                "enabled": True,
            },
            "postgresql": {
                "enabled": True,
            },
        },
        "dr": {
            "enabled": dr_enabled,
            "target_region": args.dr_region or args.dest_region,
            "block_volumes": {
                "enabled": dr_enabled,
                "target_ad_strategy": "match_source_ordinal",
            },
            "boot_volumes": {
                "enabled": dr_enabled,
                "target_ad_strategy": "match_source_ordinal",
            },
            "buckets": {
                "enabled": args.enable_dr,
            },
            "mysql": {
                "enabled": args.enable_dr,
                "backup_copy_enabled": args.enable_dr,
                "immediate_copy_new_manual_backup": args.enable_dr,
            },
            "postgresql": {
                "enabled": args.enable_dr,
                "backup_copy_enabled": args.enable_dr,
                "immediate_copy_new_manual_backup": args.enable_dr,
            },
        },
        "mysql_read_replicas": {
            "enabled": args.enable_mysql_read_replicas,
            "replica_count": args.mysql_replica_count,
        },
    })


def is_service_enabled(plan: Dict[str, Any], resource_type: str) -> bool:
    mapping = {
        "BlockVolume": "block_volumes",
        "BootVolume": "boot_volumes",
        "Bucket": "object_storage",
        "AutonomousDatabase": "autonomous_db",
        "MySQL": "mysql",
        "PostgreSQL": "postgresql",
        "Instance": None,
    }
    service_key = mapping.get(resource_type)
    if service_key is None:
        return False
    return parse_bool(plan["services"][service_key].get("enabled"), True)


def fetch_live_resource(config: Dict[str, Any], signer: Any, resource: DiscoveredResource) -> Any:
    if resource.resource_type == "BlockVolume":
        client = make_client(oci.core.BlockstorageClient, config, signer)
        return client.get_volume(resource.resource_id).data
    if resource.resource_type == "BootVolume":
        client = make_client(oci.core.BlockstorageClient, config, signer)
        return client.get_boot_volume(resource.resource_id).data
    if resource.resource_type == "Bucket":
        client = make_client(oci.object_storage.ObjectStorageClient, config, signer)
        namespace_name = get_object_storage_namespace(config, signer)
        return client.get_bucket(namespace_name, resource.display_name).data
    if resource.resource_type == "AutonomousDatabase":
        client = make_client(oci.database.DatabaseClient, config, signer)
        return client.get_autonomous_database(resource.resource_id).data
    if resource.resource_type == "MySQL":
        client = make_client(oci.mysql.DbSystemClient, config, signer)
        return client.get_db_system(resource.resource_id).data
    if resource.resource_type == "PostgreSQL":
        client = make_client(oci.psql.PostgresqlClient, config, signer)
        return client.get_db_system(resource.resource_id).data
    if resource.resource_type == "Instance":
        client = make_client(oci.core.ComputeClient, config, signer)
        return client.get_instance(resource.resource_id).data
    raise ValueError(f"Unsupported resource type for revalidation: {resource.resource_type}")


def revalidate_resource_eligibility(
    config: Dict[str, Any],
    signer: Any,
    resource: DiscoveredResource,
) -> DiscoveredResource:
    live_resource = fetch_live_resource(config, signer, resource)
    lifecycle_state = get_resource_lifecycle_state(live_resource, resource.resource_type)
    is_eligible = is_resource_backup_eligible(resource.resource_type, lifecycle_state)
    if is_eligible:
        resource.lifecycle_state = lifecycle_state
        resource.is_backup_eligible = True
        resource.skip_reason = None
        return resource

    inclusion_type = "direct" if resource.is_direct_tag_match else "derived"
    resource.lifecycle_state = lifecycle_state
    resource.is_backup_eligible = False
    resource.skip_reason = f"State changed before execution: {build_skip_reason(resource.resource_type, lifecycle_state, inclusion_type)}"
    return resource


def publish_run_notification(config: Dict[str, Any], signer: Any, topic_id: str, summary: Dict[str, Any]) -> None:
    ons_client = make_client(oci.ons.NotificationDataPlaneClient, config, signer)
    body = json.dumps(summary, indent=2, default=json_default)
    ons_client.publish_message(
        topic_id,
        oci.ons.models.MessageDetails(
            title="OCI tagged backup run summary",
            body=body,
        ),
    )


def build_run_result(resource: DiscoveredResource) -> Dict[str, Any]:
    return {
        "resource_type": resource.resource_type,
        "resource_id": resource.resource_id,
        "display_name": resource.display_name,
        "status": "pending",
        "detail": "",
        "lifecycle_state": resource.lifecycle_state,
        "planned_action": resource.planned_action,
        "planned_actions": list(resource.planned_actions),
        "actions": [],
    }


def add_action_result(result: Dict[str, Any], action_type: str, status: str, detail: str = "",
                      result_id: Optional[str] = None) -> None:
    action = {"type": action_type, "status": status, "detail": detail}
    if result_id:
        action["result_id"] = result_id
    result["actions"].append(action)
    result["status"] = aggregate_result_status(result["actions"])
    result["detail"] = aggregate_result_detail(result["actions"])


def execute_tagged_backup_run(
    config: Dict[str, Any],
    signer: Any,
    discovery: Dict[str, Any],
    plan: Dict[str, Any],
    dry_run: bool = False,
) -> Dict[str, Any]:
    resources: List[DiscoveredResource] = discovery["resources"]
    skipped_resources: List[DiscoveredResource] = discovery.get("skipped_resources", [])
    defaults = plan["defaults"]
    alerts = plan["alerts"]
    results: List[Dict[str, Any]] = []
    failed = False
    policy_cache: Dict[str, str] = {}
    namespace_name = None

    for resource in skipped_resources:
        result = build_run_result(resource)
        for action_name in resource.planned_actions or [resource.planned_action]:
            add_action_result(result, action_name, "skipped", resource.skip_reason or "Not backup eligible.")
        results.append(result)

    grouped = group_resources(resources)
    execution_order = [
        "Instance",
        "BlockVolume",
        "BootVolume",
        "Bucket",
        "AutonomousDatabase",
        "MySQL",
        "PostgreSQL",
    ]

    for resource_type in execution_order:
        for resource in grouped.get(resource_type, []):
            result = build_run_result(resource)

            if resource.resource_type == "Instance":
                add_action_result(result, "source_only", "source_only", "Tagged instance used only to derive attached storage.")
                results.append(result)
                continue

            if not is_service_enabled(plan, resource.resource_type):
                for action_name in resource.planned_actions:
                    add_action_result(result, action_name, "skipped", "Service disabled in plan.")
                results.append(result)
                continue

            if dry_run:
                for action_name in resource.planned_actions:
                    add_action_result(result, action_name, "planned", "Dry run.")
                results.append(result)
                continue

            try:
                resource = revalidate_resource_eligibility(config, signer, resource)
                result["lifecycle_state"] = resource.lifecycle_state
                if not resource.is_backup_eligible:
                    for action_name in resource.planned_actions:
                        add_action_result(result, action_name, "skipped", resource.skip_reason or "Resource is no longer backup eligible.")
                    results.append(result)
                    continue

                if resource.resource_type in {"BlockVolume", "BootVolume"}:
                    policy_id = policy_cache.get(resource.compartment_id)
                    if not policy_id:
                        policy_id = ensure_backup_policy(
                            config=config,
                            signer=signer,
                            compartment_id=resource.compartment_id,
                            policy_id=defaults["volume_policy"]["policy_id"],
                            policy_name=defaults["volume_policy"]["name"],
                            retention_days=defaults["volume_policy"]["retention_days"],
                        )
                        policy_cache[resource.compartment_id] = policy_id
                    assignment_status = assign_policy_to_asset(config, signer, resource.resource_id, policy_id)

                    if resource.resource_type == "BlockVolume":
                        backup = create_block_volume_backup(
                            config,
                            signer,
                            resource.resource_id,
                            display_name=f"{resource.display_name}-backup-{utc_timestamp()}",
                        )
                        add_action_result(result, "backup", "succeeded", f"policy={assignment_status}", backup.id)
                        if is_dr_enabled_for_resource(plan, resource.resource_type):
                            dr_result = configure_volume_dr_replication(
                                config,
                                signer,
                                resource,
                                plan["dr"]["target_region"],
                            )
                            add_action_result(result, "dr_config", dr_result["status"], dr_result["detail"], dr_result.get("result_id"))
                    else:
                        backup = create_boot_volume_backup(
                            config,
                            signer,
                            resource.resource_id,
                            display_name=f"{resource.display_name}-backup-{utc_timestamp()}",
                        )
                        add_action_result(result, "backup", "succeeded", f"policy={assignment_status}", backup.id)
                        if is_dr_enabled_for_resource(plan, resource.resource_type):
                            dr_result = configure_volume_dr_replication(
                                config,
                                signer,
                                resource,
                                plan["dr"]["target_region"],
                            )
                            add_action_result(result, "dr_config", dr_result["status"], dr_result["detail"], dr_result.get("result_id"))

                elif resource.resource_type == "Bucket":
                    if namespace_name is None:
                        namespace_name = get_object_storage_namespace(config, signer)
                    dest_bucket = f"{resource.display_name}-backup"
                    copied_count = create_object_storage_backup(
                        config=config,
                        signer=signer,
                        namespace=namespace_name,
                        source_bucket=resource.display_name,
                        dest_bucket=dest_bucket,
                        archive_after_days=defaults["archive_after_days"],
                        dest_compartment_id=resource.compartment_id,
                    )
                    add_action_result(result, "backup", "succeeded", f"copied_objects={copied_count}", dest_bucket)
                    if is_dr_enabled_for_resource(plan, resource.resource_type):
                        dr_region = plan["dr"]["target_region"]
                        dr_bucket = f"{resource.display_name}{plan['dr']['buckets']['destination_bucket_suffix']}-{dr_region}"
                        ensure_bucket_exists(config, signer, namespace_name, dr_bucket, resource.compartment_id, region=dr_region)
                        existing_policies = get_bucket_replication_policies(config, signer, namespace_name, resource.display_name)
                        matching_policy_exists = any(
                            getattr(policy, "destination_bucket_name", None) == dr_bucket and
                            getattr(policy, "destination_region_name", None) == dr_region
                            for policy in existing_policies
                        )
                        if matching_policy_exists:
                            add_action_result(result, "initial_dr_copy", "skipped", "DR bucket already seeded by existing replication policy.")
                        elif parse_bool(plan["dr"]["buckets"].get("seed_existing_objects_on_first_enablement"), True):
                            seed_result = seed_bucket_initial_dr_copy(
                                config,
                                signer,
                                namespace_name,
                                resource.display_name,
                                dr_bucket,
                                dr_region,
                            )
                            add_action_result(result, "initial_dr_copy", seed_result["status"], seed_result["detail"], seed_result.get("result_id"))
                        dr_result = configure_bucket_replication(
                            config,
                            signer,
                            namespace_name,
                            resource.display_name,
                            dr_bucket,
                            dr_region,
                        )
                        add_action_result(result, "dr_config", dr_result["status"], dr_result["detail"], dr_result.get("result_id"))

                elif resource.resource_type == "AutonomousDatabase":
                    backup = backup_database(
                        config,
                        signer,
                        resource.resource_id,
                        db_type="autonomous",
                        display_name=f"{resource.display_name}-backup-{utc_timestamp()}",
                    )
                    add_action_result(result, "backup", "succeeded", "Autonomous DB backup created.", backup.id)

                elif resource.resource_type == "MySQL":
                    backup = backup_database(
                        config,
                        signer,
                        resource.resource_id,
                        db_type="mysql",
                        display_name=f"{resource.display_name}-backup-{utc_timestamp()}",
                    )
                    add_action_result(result, "backup", "succeeded", "MySQL backup created.", backup.id)
                    if is_dr_enabled_for_resource(plan, resource.resource_type):
                        if parse_bool(plan["dr"]["mysql"].get("immediate_copy_new_manual_backup"), True):
                            copy_result = copy_mysql_backup_to_region(
                                config,
                                signer,
                                backup,
                                plan["dr"]["target_region"],
                                defaults["automatic_backup_retention_days"],
                            )
                            add_action_result(result, "initial_dr_copy", copy_result["status"], copy_result["detail"], copy_result.get("result_id"))
                        dr_result = configure_mysql_dr_backup_policy(
                            config,
                            signer,
                            resource.resource_id,
                            plan["dr"]["target_region"],
                            defaults["automatic_backup_retention_days"],
                        )
                        add_action_result(result, "dr_config", dr_result["status"], dr_result["detail"], dr_result.get("result_id"))
                    if parse_bool(plan["mysql_read_replicas"].get("enabled"), False):
                        replica_result = create_mysql_read_replicas(config, signer, resource.resource_id, plan)
                        add_action_result(result, "mysql_read_replica", replica_result["status"], replica_result["detail"], replica_result.get("result_id"))

                elif resource.resource_type == "PostgreSQL":
                    psql_live = fetch_live_resource(config, signer, resource)
                    postgres_dr_skip_reason = get_postgresql_dr_skip_reason(psql_live)
                    backup = backup_database(
                        config,
                        signer,
                        resource.resource_id,
                        db_type="postgresql",
                        display_name=f"{resource.display_name}-backup-{utc_timestamp()}",
                    )
                    backup_result_id = getattr(backup, "id", None) or getattr(backup, "work_request_id", None)
                    add_action_result(result, "backup", "succeeded", "PostgreSQL backup created.", backup_result_id)
                    if is_dr_enabled_for_resource(plan, resource.resource_type):
                        if postgres_dr_skip_reason:
                            add_action_result(result, "initial_dr_copy", "skipped", postgres_dr_skip_reason)
                            add_action_result(result, "dr_config", "skipped", postgres_dr_skip_reason)
                        else:
                            if parse_bool(plan["dr"]["postgresql"].get("immediate_copy_new_manual_backup"), True):
                                if not getattr(backup, "id", None):
                                    add_action_result(
                                        result,
                                        "initial_dr_copy",
                                        "skipped",
                                        "PostgreSQL backup copy skipped because the created backup OCID is not yet available.",
                                    )
                                else:
                                    copy_result = copy_postgresql_backup_to_region(
                                        config,
                                        signer,
                                        backup,
                                        plan["dr"]["target_region"],
                                        defaults["automatic_backup_retention_days"],
                                    )
                                    add_action_result(result, "initial_dr_copy", copy_result["status"], copy_result["detail"], copy_result.get("result_id"))
                            dr_result = configure_postgresql_dr(
                                config,
                                signer,
                                resource.resource_id,
                                plan["dr"]["target_region"],
                                defaults["automatic_backup_retention_days"],
                            )
                            add_action_result(result, "dr_config", dr_result["status"], dr_result["detail"], dr_result.get("result_id"))

                else:
                    add_action_result(result, resource.planned_action or "backup", "skipped", f"Unsupported resource type: {resource.resource_type}")

            except Exception as exc:
                failed = True
                action_name = next(
                    (action for action in resource.planned_actions if action not in {item["type"] for item in result["actions"]}),
                    resource.planned_action or "backup",
                )
                add_action_result(result, action_name, "failed", str(exc))

            results.append(result)

    summary = {
        "failed": failed,
        "dry_run": dry_run,
        "counts": {
            "total": len(results),
            "succeeded": sum(1 for item in results if item["status"] == "succeeded"),
            "planned": sum(1 for item in results if item["status"] == "planned"),
            "skipped": sum(1 for item in results if item["status"] == "skipped"),
            "source_only": sum(1 for item in results if item["status"] == "source_only"),
            "failed": sum(1 for item in results if item["status"] == "failed"),
        },
        "results": results,
    }

    if alerts.get("topic_id") and not dry_run:
        try:
            publish_run_notification(config, signer, alerts["topic_id"], summary)
        except Exception as exc:
            log.warning("Failed to publish notifications summary: %s", exc)

    return summary


def run_tagged_backup_flow(
    config: Dict[str, Any],
    signer: Any,
    root_compartment_id: str,
    plan: Dict[str, Any],
    dry_run: bool,
    output: str,
) -> Dict[str, Any]:
    discovery_cfg = plan["discovery"]
    tag_cfg = discovery_cfg["tag"]

    discovery = discover_tagged_resources(
        config=config,
        signer=signer,
        root_compartment_id=root_compartment_id,
        include_subcompartments=discovery_cfg["include_subcompartments"],
        namespace=tag_cfg["namespace"],
        tag_key_name=tag_cfg["key"],
        tag_value=tag_cfg["value"],
        expand_tagged_instances=discovery_cfg["expand_tagged_instances"],
    )
    discovery = enrich_discovery_with_plan_actions(discovery, plan)

    if output == "json":
        preview = {
            "discovery": {
                **discovery,
                "resources": [asdict(resource) for resource in discovery["resources"]],
                "skipped_resources": [asdict(resource) for resource in discovery.get("skipped_resources", [])],
            }
        }
        print_json(preview)
    else:
        print("Discovery preview:")
        print_discovery_table(discovery["resources"])
        print_skipped_discovery_table(discovery.get("skipped_resources", []))

    summary = execute_tagged_backup_run(config, signer, discovery, plan, dry_run=dry_run)

    if output == "json":
        print_json(summary)
    else:
        print_run_results_table(summary["results"])
        print("\nRun summary:")
        print_json(summary["counts"])

    return summary


def run_list_tagged_resources(
    config: Dict[str, Any],
    signer: Any,
    root_compartment_id: str,
    include_subcompartments: bool,
    output: str,
) -> Dict[str, Any]:
    discovery = discover_tagged_resources(
        config=config,
        signer=signer,
        root_compartment_id=root_compartment_id,
        include_subcompartments=include_subcompartments,
        namespace=DEFAULT_TAG_NAMESPACE,
        tag_key_name=DEFAULT_TAG_KEY,
        tag_value=DEFAULT_TAG_VALUE,
        expand_tagged_instances=True,
    )
    discovery = enrich_discovery_with_plan_actions(discovery, default_plan_dict())

    payload = {
        **discovery,
        "resources": [asdict(resource) for resource in discovery["resources"]],
        "skipped_resources": [asdict(resource) for resource in discovery.get("skipped_resources", [])],
    }
    if output == "json":
        print_json(payload)
    else:
        print(f"Defined tag discovery for {payload['tag']['path']}")
        print_discovery_table(discovery["resources"])
        print_skipped_discovery_table(discovery.get("skipped_resources", []))
        if payload["notes"]:
            print("\nNotes:")
            for note in payload["notes"]:
                print(f"- {note}")
    return payload


def build_parser():
    parser = argparse.ArgumentParser(
        description="OCI Backup Manager - centralized backup automation with tag-driven orchestration",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--auth", choices=["config_file", "instance_principal"],
                        default="config_file", help="Authentication method (default: config_file)")
    parser.add_argument("--profile", default="DEFAULT", help="OCI config profile name")
    parser.add_argument("--debug", action="store_true", help="Enable debug logging")

    sub = parser.add_subparsers(dest="command", required=True)

    p = sub.add_parser("create-policy", help="Create a volume backup policy")
    p.add_argument("--compartment-id", required=True)
    p.add_argument("--name", default="oci-backup-manager-policy")
    p.add_argument("--retention-days", type=int, default=DEFAULT_RETENTION_DAYS)

    p = sub.add_parser("assign-by-tag", help="Assign policy to tagged volumes")
    p.add_argument("--compartment-id", required=True)
    p.add_argument("--tag-key", required=True)
    p.add_argument("--tag-value", required=True)
    p.add_argument("--policy-id", required=True, help="OCID of the backup policy")

    p = sub.add_parser("list-backups", help="List all backups in a compartment")
    p.add_argument("--compartment-id", required=True)

    p = sub.add_parser("copy-cross-region", help="Copy a backup to another OCI region")
    p.add_argument("--backup-id", required=True)
    p.add_argument("--resource-type", required=True, choices=["block_volume", "boot_volume"])
    p.add_argument("--dest-region", default=CROSS_REGION)
    p.add_argument("--display-name")

    p = sub.add_parser("create-object-backup", help="Backup Object Storage bucket")
    p.add_argument("--namespace", required=True)
    p.add_argument("--bucket", required=True, help="Source bucket name")
    p.add_argument("--dest-bucket", required=True, help="Destination bucket name")
    p.add_argument("--archive-after-days", type=int, default=ARCHIVE_AFTER_DAYS)
    p.add_argument("--dest-compartment-id")

    p = sub.add_parser("backup-database", help="Trigger on-demand DB backup")
    p.add_argument("--db-id", required=True, help="OCID of the DB system")
    p.add_argument("--db-type", required=True, choices=["autonomous", "mysql", "postgresql"])
    p.add_argument("--display-name")

    p = sub.add_parser("monitor", help="Monitor backup job status (polling loop)")
    p.add_argument("--compartment-id", required=True)
    p.add_argument("--topic-id", help="OCI Notifications topic OCID for alerts")
    p.add_argument("--poll-seconds", type=int, default=30)

    p = sub.add_parser("restore", help="Restore a backup to a new volume")
    p.add_argument("--backup-id", required=True)
    p.add_argument("--resource-type", required=True, choices=["block_volume", "boot_volume"])
    p.add_argument("--availability-domain", required=True)
    p.add_argument("--compartment-id")
    p.add_argument("--display-name")

    p = sub.add_parser("list-tagged-resources", help="List supported resources tagged with Operations.backup=enabled")
    p.add_argument("--compartment-id", required=True)
    p.add_argument("--include-subcompartments", action="store_true")
    p.add_argument("--output", choices=SUPPORTED_OUTPUTS, default=DEFAULT_OUTPUT)

    p = sub.add_parser("run-tagged-backups", help="Discover tagged resources and run the correct backup for each")
    p.add_argument("--compartment-id", required=True)
    p.add_argument("--include-subcompartments", action="store_true")
    p.add_argument("--dest-region", default=CROSS_REGION)
    p.add_argument("--archive-after-days", type=int, default=ARCHIVE_AFTER_DAYS)
    p.add_argument("--topic-id")
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--output", choices=SUPPORTED_OUTPUTS, default=DEFAULT_OUTPUT)
    p.add_argument("--policy-id")
    p.add_argument("--policy-name", default=DEFAULT_POLICY_NAME)
    p.add_argument("--retention-days", type=int, default=DEFAULT_RETENTION_DAYS)
    p.add_argument("--enable-cross-region-copy", action="store_true", help="Deprecated alias for volume DR enablement.")
    p.add_argument("--enable-dr", action="store_true", help="Enable cross-region DR configuration for tagged resources that support it.")
    p.add_argument("--dr-region", default=CROSS_REGION, help="Destination region for DR replication or backup copy.")
    p.add_argument("--enable-mysql-read-replicas", action="store_true", help="Create same-region MySQL read replicas for tagged MySQL DB systems.")
    p.add_argument("--mysql-replica-count", type=int, default=1)

    p = sub.add_parser("validate-plan", help="Validate a JSON or YAML backup plan file")
    p.add_argument("--plan", required=True)
    p.add_argument("--output", choices=SUPPORTED_OUTPUTS, default=DEFAULT_OUTPUT)

    p = sub.add_parser("run-plan", help="Run tagged backups from a plan file")
    p.add_argument("--plan", required=True)
    p.add_argument("--compartment-id", required=True)
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--output", choices=SUPPORTED_OUTPUTS, default=DEFAULT_OUTPUT)

    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()

    if args.debug:
        logging.getLogger().setLevel(logging.DEBUG)

    try:
        if args.command == "validate-plan":
            plan = validate_and_normalize_plan(load_plan_file(args.plan))
            if args.output == "json":
                print_json(plan)
            else:
                print("Plan is valid.")
                print_json(plan)
            return 0

        if args.command == "create-policy":
            config, signer = get_config_and_signer(args.auth, getattr(args, "profile", "DEFAULT"))
            policy = create_backup_policy(config, signer, args.compartment_id, args.name, args.retention_days)
            print_json({"policy_id": policy.id, "name": policy.display_name})
            return 0

        if args.command == "assign-by-tag":
            config, signer = get_config_and_signer(args.auth, getattr(args, "profile", "DEFAULT"))
            count = assign_policy_by_tag(config, signer, args.compartment_id, args.tag_key, args.tag_value, args.policy_id)
            print(f"Assigned to {count} resource(s).")
            return 0

        if args.command == "list-backups":
            config, signer = get_config_and_signer(args.auth, getattr(args, "profile", "DEFAULT"))
            list_backups(config, signer, args.compartment_id)
            return 0

        if args.command == "copy-cross-region":
            config, signer = get_config_and_signer(args.auth, getattr(args, "profile", "DEFAULT"))
            copy = copy_backup_cross_region(config, signer, args.backup_id, args.resource_type, args.dest_region, args.display_name)
            print_json({"copy_id": copy.id, "dest_region": args.dest_region})
            return 0

        if args.command == "create-object-backup":
            config, signer = get_config_and_signer(args.auth, getattr(args, "profile", "DEFAULT"))
            count = create_object_storage_backup(
                config,
                signer,
                args.namespace,
                args.bucket,
                args.dest_bucket,
                args.archive_after_days,
                args.dest_compartment_id,
            )
            print(f"Copied {count} object(s) to '{args.dest_bucket}'.")
            return 0

        if args.command == "backup-database":
            config, signer = get_config_and_signer(args.auth, getattr(args, "profile", "DEFAULT"))
            backup = backup_database(config, signer, args.db_id, args.db_type, args.display_name)
            print_json({"backup_id": backup.id, "state": backup.lifecycle_state})
            return 0

        if args.command == "monitor":
            config, signer = get_config_and_signer(args.auth, getattr(args, "profile", "DEFAULT"))
            monitor_backups(config, signer, args.compartment_id, topic_id=args.topic_id, poll_seconds=args.poll_seconds)
            return 0

        if args.command == "restore":
            config, signer = get_config_and_signer(args.auth, getattr(args, "profile", "DEFAULT"))
            vol = restore_backup(config, signer, args.backup_id, args.resource_type, args.availability_domain, args.display_name, args.compartment_id)
            print_json({"volume_id": vol.id, "name": vol.display_name})
            return 0

        if args.command == "list-tagged-resources":
            config, signer = get_config_and_signer(args.auth, getattr(args, "profile", "DEFAULT"))
            run_list_tagged_resources(
                config=config,
                signer=signer,
                root_compartment_id=args.compartment_id,
                include_subcompartments=args.include_subcompartments,
                output=args.output,
            )
            return 0

        if args.command == "run-tagged-backups":
            config, signer = get_config_and_signer(args.auth, getattr(args, "profile", "DEFAULT"))
            plan = build_inline_plan_from_args(args)
            summary = run_tagged_backup_flow(
                config=config,
                signer=signer,
                root_compartment_id=args.compartment_id,
                plan=plan,
                dry_run=args.dry_run,
                output=args.output,
            )
            return 1 if summary["failed"] else 0

        if args.command == "run-plan":
            plan = validate_and_normalize_plan(load_plan_file(args.plan))
            auth_cfg = plan["auth"]
            config, signer = get_config_and_signer(auth_cfg["method"], auth_cfg["profile"])
            summary = run_tagged_backup_flow(
                config=config,
                signer=signer,
                root_compartment_id=args.compartment_id,
                plan=plan,
                dry_run=args.dry_run,
                output=args.output,
            )
            return 1 if summary["failed"] else 0

        parser.error(f"Unhandled command: {args.command}")
        return 2

    except (PlanValidationError, FileNotFoundError, RuntimeError, ValueError) as exc:
        log.error("%s", exc)
        return 2
    except Exception as exc:  # pragma: no cover - final guardrail
        log.exception("Unexpected failure: %s", exc)
        return 1


if __name__ == "__main__":
    sys.exit(main())
