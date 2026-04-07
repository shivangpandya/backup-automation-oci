# OCI Backup Manager

A centralized OCI backup automation tool inspired by **AWS Backup**. Automates backup creation, policy assignment, cross-region copy, lifecycle management, monitoring, and restore across all major OCI resource types — using the OCI Python SDK.

---

## Table of Contents

- [Overview](#overview)
- [Supported Resources](#supported-resources)
- [AWS Backup vs OCI Mapping](#aws-backup-vs-oci-mapping)
- [Prerequisites](#prerequisites)
- [Authentication Setup](#authentication-setup)
- [Installation](#installation)
- [Tag-Driven Orchestration](#tag-driven-orchestration)
- [Commands](#commands)
  - [create-policy](#1-create-policy)
  - [assign-by-tag](#2-assign-by-tag)
  - [list-backups](#3-list-backups)
  - [copy-cross-region](#4-copy-cross-region)
  - [create-object-backup](#5-create-object-backup)
  - [backup-database](#6-backup-database)
  - [monitor](#7-monitor)
  - [restore](#8-restore)
- [Global Flags](#global-flags)
- [Where to Find OCIDs](#where-to-find-ocids)
- [OCI Services Used](#oci-services-used)
- [Known Issues & Fixes](#known-issues--fixes)

---

## Overview

Managing backups across OCI manually — navigating separate consoles for Block Volumes, Databases, Object Storage, and MySQL — is tedious and error-prone. This script centralizes all of that into a single Python CLI tool, mirroring the experience of AWS Backup.

With one command you can:
- Create a backup policy with daily, weekly, and monthly schedules
- Automatically discover and protect all tagged resources
- Copy backups cross-region to `us-phoenix-1` for disaster recovery
- Monitor backup health in real time with failure alerts via OCI Notifications
- Restore any backup to a new volume with a single command

New in v2:
- Discover resources using the OCI defined tag `Operations.backup=enabled`
- Expand tagged Compute instances into their attached boot and block volumes
- Preview all discovered resources before running any backup action
- Run all supported backup actions with one command or from a saved plan file

---

## Supported Resources

| Resource | Backup Type | Cross-Region Copy |
|---|---|---|
| Block Volumes | Policy-based (incremental + full) | ✅ |
| Boot Volumes | Policy-based (incremental + full) | ✅ |
| Object Storage Buckets | Object copy + lifecycle rules | ❌ (same-region) |
| Autonomous Database | On-demand full backup | ❌ |
| MySQL HeatWave | On-demand full backup | ❌ |

---

## AWS Backup vs OCI Mapping

| AWS Backup Feature | OCI Equivalent in This Script |
|---|---|
| Backup Plans | `create-policy` → OCI `VolumeBackupPolicy` |
| Tag-based resource assignment | `assign-by-tag` → freeform tag discovery |
| Cross-region copy | `copy-cross-region` → OCI Volume Backup Copy API |
| Lifecycle (warm → cold) | Object Storage lifecycle rule (Standard → Archive) |
| Activity monitoring + SNS alerts | `monitor` → polling loop + OCI Notifications (ONS) |
| Restore jobs | `restore` → new volume from backup OCID |
| Centralized backup console | `list-backups` → consolidated status table |

---

## Prerequisites

- Python **3.8+**
- An OCI account with appropriate IAM permissions
- OCI API key configured (see Authentication Setup below)

```bash
python3 --version   # Must be 3.8 or higher
```

---

## Authentication Setup

### Step 1 — Create the OCI config file

```bash
mkdir -p ~/.oci
cat > ~/.oci/config << EOF
[DEFAULT]
user=ocid1.user.oc1..xxxxxxxxxxxxxx
fingerprint=xx:xx:xx:xx:xx:xx:xx:xx:xx:xx:xx:xx:xx:xx:xx:xx
tenancy=ocid1.tenancy.oc1..xxxxxxxxxxxxxx
region=us-ashburn-1
key_file=~/.oci/oci_api_key.pem
EOF
```

### Step 2 — Generate an API key pair

```bash
openssl genrsa -out ~/.oci/oci_api_key.pem 2048
chmod 600 ~/.oci/oci_api_key.pem
openssl rsa -pubout -in ~/.oci/oci_api_key.pem -out ~/.oci/oci_api_key_public.pem
```

### Step 3 — Upload the public key to OCI Console

Go to **OCI Console → Identity → Users → Your User → API Keys → Add API Key** and paste the contents of `oci_api_key_public.pem`.

### Step 4 — Validate the setup

```bash
python3 -c "import oci; cfg = oci.config.from_file(); oci.config.validate_config(cfg); print('Config OK')"
```

> **Running on OCI Compute?** Use `--auth instance_principal` instead of setting up a config file — no key management needed.

---

## Installation

```bash
pip install oci
python3 oci_backup_manager.py --help
```

---

## Tag-Driven Orchestration

The new default workflow is:

1. Tag supported OCI resources with the defined tag `Operations.backup=enabled`
2. Preview what will be backed up
3. Run one orchestration command

Supported tag-driven discovery behavior:

- Tagged `BlockVolume` -> creates a block volume backup
- Tagged `BootVolume` -> creates a boot volume backup
- Tagged `Bucket` -> copies objects to a deterministic backup bucket named `<source>-backup`
- Tagged `AutonomousDatabase` -> triggers an on-demand Autonomous DB backup
- Tagged `MySQL DB System` -> triggers an on-demand MySQL backup
- Tagged `Instance` -> discovers attached boot and block volumes and backs those up

### Preview tagged resources

```bash
python3 oci_backup_manager.py list-tagged-resources \
  --compartment-id ocid1.compartment.oc1..xxxxxx \
  --include-subcompartments
```

`list-tagged-resources` now shows only backup-eligible resources in the main list. Tagged resources in unsupported lifecycle states such as a block volume in `TERMINATED` are shown separately in a skipped section with the exact reason.

### Run all backups for tagged resources

```bash
python3 oci_backup_manager.py run-tagged-backups \
  --compartment-id ocid1.compartment.oc1..xxxxxx \
  --include-subcompartments \
  --archive-after-days 90 \
  --enable-cross-region-copy
```

`run-tagged-backups` re-checks each resource's live lifecycle state immediately before the backup action. If a tagged resource is no longer eligible, it is skipped and reported as `skipped` instead of failing the whole run.

### Dry-run before making changes

```bash
python3 oci_backup_manager.py run-tagged-backups \
  --compartment-id ocid1.compartment.oc1..xxxxxx \
  --dry-run
```

### Run from a saved plan file

Plan files can be JSON by default, or YAML if `pyyaml` is installed.

```json
{
  "auth": {
    "method": "config_file",
    "profile": "DEFAULT"
  },
  "defaults": {
    "dest_region": "us-phoenix-1",
    "archive_after_days": 90,
    "volume_policy": {
      "name": "oci-backup-manager-tagged-policy",
      "retention_days": 30,
      "policy_id": null
    }
  },
  "discovery": {
    "mode": "defined_tag",
    "include_subcompartments": true,
    "expand_tagged_instances": true,
    "tag": {
      "namespace": "Operations",
      "key": "backup",
      "value": "enabled"
    }
  },
  "services": {
    "block_volumes": {
      "enabled": true,
      "copy_to_region_enabled": true
    },
    "boot_volumes": {
      "enabled": true,
      "copy_to_region_enabled": true
    },
    "object_storage": {
      "enabled": true
    },
    "autonomous_db": {
      "enabled": true
    },
    "mysql": {
      "enabled": true
    }
  },
  "alerts": {
    "topic_id": null
  }
}
```

Validate and run the plan:

```bash
python3 oci_backup_manager.py validate-plan --plan backup-plan.json
python3 oci_backup_manager.py run-plan \
  --plan backup-plan.json \
  --compartment-id ocid1.compartment.oc1..xxxxxx
```

---

## Commands

### 1. `create-policy`

Creates an OCI Volume Backup Policy with three automatic schedules:

| Schedule | Type | Retention |
|---|---|---|
| Daily | Incremental | 7 days |
| Weekly | Full | 28 days |
| Monthly | Full | Custom (default: 30 days) |

```bash
python3 oci_backup_manager.py create-policy \
  --compartment-id ocid1.compartment.oc1..xxxxxx \
  --name "prod-backup-policy" \
  --retention-days 30
```

**Output:** Returns the new policy OCID. Save this — you'll need it for `assign-by-tag`.

| Argument | Required | Description |
|---|---|---|
| `--compartment-id` | ✅ | Compartment where the policy is created |
| `--name` | ❌ | Policy display name (default: `oci-backup-manager-policy`) |
| `--retention-days` | ❌ | Monthly backup retention in days (default: 30) |

---

### 2. `assign-by-tag`

Scans all Block Volumes and Boot Volumes in a compartment, filters those matching a freeform tag (`key=value`), and assigns the backup policy to each one automatically.

```bash
python3 oci_backup_manager.py assign-by-tag \
  --compartment-id ocid1.compartment.oc1..xxxxxx \
  --tag-key backup \
  --tag-value true \
  --policy-id ocid1.volumebackuppolicy.oc1..xxxxxx
```

**To tag a volume via CLI before running this:**
```bash
oci bv volume update \
  --volume-id ocid1.volume.oc1..xxxxx \
  --freeform-tags '{"backup": "true"}'
```

| Argument | Required | Description |
|---|---|---|
| `--compartment-id` | ✅ | Compartment to scan |
| `--tag-key` | ✅ | Freeform tag key to match |
| `--tag-value` | ✅ | Freeform tag value to match |
| `--policy-id` | ✅ | OCID of the policy to assign |

> Already-assigned volumes are safely skipped (no duplicate assignments).

---

### 3. `list-backups`

Prints a consolidated table of all backups across Block Volumes, Boot Volumes, Autonomous Databases, and MySQL HeatWave in a compartment.

```bash
python3 oci_backup_manager.py list-backups \
  --compartment-id ocid1.compartment.oc1..xxxxxx
```

**Sample output:**
```
TYPE             STATE        SIZE(GB)   NAME                                     CREATED
──────────────────────────────────────────────────────────────────────────────────────────────────────────────
BlockVolume      AVAILABLE    50         prod-vol-backup-20260405                 2026-04-05 02:00:01+00:00
BootVolume       AVAILABLE    100        prod-boot-backup-20260405                2026-04-05 02:00:05+00:00
AutonomousDB     ACTIVE       N/A        manual-backup-20260405123000             2026-04-05 12:30:00+00:00

Total: 3 backup(s) found.
```

| Argument | Required | Description |
|---|---|---|
| `--compartment-id` | ✅ | Compartment to list backups from |

---

### 4. `copy-cross-region`

Copies a block volume or boot volume backup to another OCI region. Defaults to `us-phoenix-1` for disaster recovery.

```bash
# Block volume
python3 oci_backup_manager.py copy-cross-region \
  --backup-id ocid1.volumebackup.oc1..xxxxxx \
  --resource-type block_volume

# Boot volume
python3 oci_backup_manager.py copy-cross-region \
  --backup-id ocid1.bootvolumebackup.oc1..xxxxxx \
  --resource-type boot_volume \
  --dest-region us-phoenix-1
```

| Argument | Required | Description |
|---|---|---|
| `--backup-id` | ✅ | OCID of the backup to copy |
| `--resource-type` | ✅ | `block_volume` or `boot_volume` |
| `--dest-region` | ❌ | Destination region (default: `us-phoenix-1`) |
| `--display-name` | ❌ | Name for the copied backup |

---

### 5. `create-object-backup`

Copies all objects from a source bucket to a destination backup bucket, and applies an Object Storage lifecycle rule to automatically transition objects to **Archive storage** after a specified number of days.

```bash
python3 oci_backup_manager.py create-object-backup \
  --namespace my-namespace \
  --bucket my-source-bucket \
  --dest-bucket my-backup-bucket \
  --archive-after-days 90
```

The destination bucket is created automatically if it does not exist.

| Argument | Required | Description |
|---|---|---|
| `--namespace` | ✅ | OCI Object Storage namespace |
| `--bucket` | ✅ | Source bucket name |
| `--dest-bucket` | ✅ | Destination (backup) bucket name |
| `--archive-after-days` | ❌ | Days before objects move to Archive tier (default: 90) |

---

### 6. `backup-database`

Triggers an on-demand backup for an Autonomous Database or MySQL HeatWave DB system.

```bash
# Autonomous DB
python3 oci_backup_manager.py backup-database \
  --db-id ocid1.autonomousdatabase.oc1..xxxxxx \
  --db-type autonomous \
  --display-name "pre-upgrade-backup"

# MySQL HeatWave
python3 oci_backup_manager.py backup-database \
  --db-id ocid1.mysqldbsystem.oc1..xxxxxx \
  --db-type mysql
```

| Argument | Required | Description |
|---|---|---|
| `--db-id` | ✅ | OCID of the DB system |
| `--db-type` | ✅ | `autonomous` or `mysql` |
| `--display-name` | ❌ | Backup display name (auto-generated if omitted) |

---

### 7. `monitor`

Runs a continuous polling loop that prints the live status of all block and boot volume backups in a compartment. Optionally fires an alert to an OCI Notifications topic when a backup enters a `FAULTY` state.

```bash
# Basic monitoring
python3 oci_backup_manager.py monitor \
  --compartment-id ocid1.compartment.oc1..xxxxxx \
  --poll-seconds 30

# With failure alerts via OCI Notifications
python3 oci_backup_manager.py monitor \
  --compartment-id ocid1.compartment.oc1..xxxxxx \
  --topic-id ocid1.onstopic.oc1..xxxxxx \
  --poll-seconds 60
```

Press `Ctrl+C` to stop the loop.

| Argument | Required | Description |
|---|---|---|
| `--compartment-id` | ✅ | Compartment to monitor |
| `--topic-id` | ❌ | OCI Notifications topic OCID for failure alerts |
| `--poll-seconds` | ❌ | Polling interval in seconds (default: 30) |

---

### 8. `restore`

Restores a block volume or boot volume backup to a new volume in the specified Availability Domain.

```bash
# Restore block volume
python3 oci_backup_manager.py restore \
  --backup-id ocid1.volumebackup.oc1..xxxxxx \
  --resource-type block_volume \
  --availability-domain "UsBwR:US-ASHBURN-AD-1" \
  --compartment-id ocid1.compartment.oc1..xxxxxx \
  --display-name "restored-vol-2026-04-05"

# Restore boot volume
python3 oci_backup_manager.py restore \
  --backup-id ocid1.bootvolumebackup.oc1..xxxxxx \
  --resource-type boot_volume \
  --availability-domain "UsBwR:US-ASHBURN-AD-1"
```

| Argument | Required | Description |
|---|---|---|
| `--backup-id` | ✅ | OCID of the backup to restore |
| `--resource-type` | ✅ | `block_volume` or `boot_volume` |
| `--availability-domain` | ✅ | Target Availability Domain name |
| `--compartment-id` | ❌ | Target compartment (defaults to source compartment) |
| `--display-name` | ❌ | Name for the restored volume (auto-generated if omitted) |

---

## Global Flags

These flags work with every command.

| Flag | Description |
|---|---|
| `--auth instance_principal` | Use OCI Instance Principal auth (when running on OCI Compute — no config file needed) |
| `--auth config_file` | Use `~/.oci/config` file (default) |
| `--profile MYPROFILE` | Use a named profile from `~/.oci/config` instead of `[DEFAULT]` |
| `--debug` | Enable verbose debug logging |

---

## Where to Find OCIDs

| Value | Where to find it |
|---|---|
| `compartment-id` | OCI Console → Identity & Security → Compartments |
| `policy-id` | OCI Console → Storage → Block Storage → Backup Policies |
| `backup-id` | OCI Console → Storage → Block Volume Backups |
| `db-id` (Autonomous) | OCI Console → Oracle Database → Autonomous Databases |
| `db-id` (MySQL) | OCI Console → Databases → MySQL HeatWave |
| `namespace` | OCI Console → Storage → Object Storage → any bucket detail page |
| `topic-id` | OCI Console → Developer Services → Application Integration → Notifications |
| `availability-domain` | OCI Console → any compute or storage resource detail page |

---

## OCI Services Used

| OCI Service | SDK Client | Used For |
|---|---|---|
| Block Storage | `oci.core.BlockstorageClient` | Volume/boot volume backups, policies, restore |
| Identity | `oci.identity.IdentityClient` | Listing Availability Domains |
| Object Storage | `oci.object_storage.ObjectStorageClient` | Bucket backup and lifecycle rules |
| Database | `oci.database.DatabaseClient` | Autonomous DB backups |
| MySQL | `oci.mysql.DbBackupsClient`, `oci.mysql.DbSystemClient` | MySQL HeatWave backups |
| Notifications | `oci.ons.NotificationDataPlaneClient` | Failure alerts |

---

## Known Issues & Fixes

### `AttributeError: 'ComputeClient' object has no attribute 'list_availability_domains'`

**Cause:** `list_availability_domains` belongs to `oci.identity.IdentityClient`, not `oci.core.ComputeClient`.

**Fix:** Already corrected in the current version of the script. The `assign-by-tag` function now uses `IdentityClient` to list Availability Domains before iterating over boot volumes.

---

## License

MIT — free to use, modify, and distribute.
