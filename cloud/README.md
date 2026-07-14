# Cloud projects (object-store backed)

These are the **cloud / SaaS** copies of the workspace projects. They are identical
to the on-prem copies under `../local/` **except for one thing**: each project's zone
is bound to a storage **connection** so every table the project creates lands on the
lake (any object store: Azure ADLS, AWS S3, or GCS) instead of the container's
ephemeral local disk.

## Why a separate set exists

The `local/` projects create their zones `TYPE TEMP` with **relative** table
`LOCATION`s (e.g. `ehr/bronze/raw_admissions`). With no connection, a zone is local:
its tables resolve under the node's local app-data directory. That is correct on a
desktop / on-prem install, but on a cloud deployment the local disk is ephemeral and
is not the lake, so the data would be lost on restart and is invisible to other nodes.

A cloud-backed zone must be bound to a **connection** (a `data_sources` row that
carries the storage account/bucket, container, and credentials). The connection is
what supplies the credentials and the object-store root; a bare storage path on its
own does not resolve them. The cloud copies therefore create each zone with a
`CONNECTION`, and the relative table `LOCATION`s resolve under the connection's root,
so the rest of each project is unchanged.

## The one edit you must make

Every `01_setup.sql` (and `supply-chain-analytics/bronze/01_create_zones.sql`)
references a cloud-neutral connection name:

```sql
CREATE ZONE IF NOT EXISTS ehr
  TYPE EXTERNAL
  CONNECTION objectstore
  STORAGE_ROOT = 'ehr'
  COMMENT 'Healthcare EHR pipeline zone';
```

`objectstore` is the name the platform auto-seeds at boot for the deployment's
object store, whichever cloud it runs on (Azure ADLS, AWS S3, or GCS), so the same
SQL works unchanged on every cloud. If your install uses a different connection name,
replace `objectstore` with it. `STORAGE_ROOT` here is a **subpath under the
connection's root** (it defaults to the zone name), not a full URL, so it does not
change per deployment.

### Creating the connection

The connection is created once, per install, not per project. Two ways:

- **GUI:** Configuration -> Connections -> add an Azure ADLS / S3 / GCS connection
  (this is the "Connection" shown in the Create Zone dialog).
- **SQL:** `CREATE CONNECTION <name> TYPE azure_adls OPTIONS (...)` (or `s3` / `gcs`).

On a cloud deployment the platform **auto-seeds** a storage connection named
`objectstore` and a cloud zone at boot, on whichever cloud it runs, so the connection
these projects reference already exists. Credentials are ambient (managed identity /
task role / service account), so no keys go in the SQL.

`CONNECTION` requires an object-store connection (Azure ADLS, S3, or GCS). Binding a
zone to a database connection is rejected.

## Importing the right set (folder = environment)

A workspace scan walks SQL files recursively from the path you give it and reports
each file by its path. So the top-level folder name is what tells you, and the
scanner, which environment a file belongs to:

- **Cloud install:** scan **this** `cloud/` folder.
- **On-prem / desktop install:** scan the sibling `../local/` folder.

Do **not** scan the workspace root. The `local/` and `cloud/` sets define the **same**
pipeline and zone names (`ehr_setup`, zone `ehr`, ...), so scanning the root would
discover both and they would collide. One environment, one folder, one scan.
