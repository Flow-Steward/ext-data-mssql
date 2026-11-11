# Microsoft SQL Server

Connect Microsoft SQL Server to Flow Steward through a direct database connection or the Cloud SQL for SQL Server profile. Read-only Datasets support connection testing, typed schema inspection, saved-query validation, previews, execution and output-schema inference. Selected table and column scopes keep saved queries focused on the data a workflow needs, such as operational lookups, reporting extracts or application records used in later steps.

A separate workflow action runs one INSERT, UPDATE or DELETE with named parameters and an optional parameter schema. Use it to store workflow results or update application records in the configured database. Parameter values are supplied separately from SQL, so a reusable statement can accept different inputs on each run. Workflow writes do not change the read-only nature of Dataset queries.

Direct connections require the host, database and user with a password in connection secrets. The Cloud SQL profile uses Google project, region, instance and IP-type settings and also requires database credentials. Reads return at most 100 rows and 480 KiB. SQL support is a restricted subset; DDL, transaction scripts and database triggers are outside the connector's operation set.

## Connection and operation reference

Supports profiles: `mssql`, `cloud-sql-mssql`. Read-only datasets support schema inspection, validation, bounded previews and saved-query execution. Workflows support one bound INSERT, UPDATE or DELETE, with project connection references and durable effect claims. No triggers or DDL are exposed.

Direct SQL Server connections use host, port, database and user, with password in project secrets. Cloud SQL for SQL Server uses Google project, region, instance and IP-type settings and requires database user/password credentials. The runtime also needs the appropriate Google Cloud access for the managed connection. The pinned definitions under `scripts/upstream-prebuilts/` document exact environment names and defaults.

SQL is a deliberately strict subset: raw backslashes and dollar characters are rejected before parsing; pass those characters through bound parameter values. Reads return at most 100 rows and 480 KiB. Writes have no automatic retry; an uncertain result is reported as unknown. Test mode suppresses writes before driver invocation.

The bundled AMD64 and ARM64 native executables are unchanged, checksum-verified minimal builds from the prior database-specific package. `toolbox-provenance.json`, `SHA256SUMS` and `THIRD_PARTY.yml` retain the pinned upstream source, original binding patch and build image. No other database adapter is installed. The common engine exists only as a pinned build dependency; `scripts/build_toolbox.sh` accepts no database selector and compiles only this package’s profiles.

Build: `scripts/build_toolbox.sh`. Package: `python3 scripts/package_extension.py --runtime-target linux-amd64` and `linux-arm64`. Development tests run through Flow Steward’s Docker extension gate. No live cloud account verification is claimed.

Credential validation: Password is required for mssql, cloud-sql-mssql. Supply password or the matching profile-specific secret env variable. Managed PostgreSQL/MySQL/AlloyDB IAM profiles keep password optional; public config env is not a credential source. Required profiles reject missing/blank secrets before provider dispatch, including workflow test mode. The secret environment alternative belongs in project secrets, for example `env: {PROFILE_PASSWORD: ...}` using the exact variable from the selected pinned profile. Wrong-profile environment keys do not satisfy the requirement.

## Automated CI and releases

Pushes to `main` and pull requests run the shared extension build workflow. The checked-in `.github/release-config.json` selects both Linux targets, the repository's package script and test inputs. Missing native payloads and published-only inventory are restored from checksum-pinned release archives; tracked source files are preserved. The source packager retains its provenance checks.

After the versioned release commit is on `main`, publish by pushing its version tag:

```sh
git tag v1.3.6
git push origin v1.3.6
```

The release workflow runs the shared checks, builds the target archives and publishes the GitHub release and checksums. Do not move an existing tag. To rerun a release for an existing tag, dispatch the same workflow:

```sh
gh workflow run release.yml --repo Flow-Steward/ext-data-mssql --ref main -f tag=v1.3.6
```

The manifest, descriptor (when present), package version and tag must identify the same release. Release publication uses the workflow token; no additional repository secret is required by these callers.

### Standalone CI coverage

The release recipe lists the standalone test files and individual test nodes explicitly, including package, source and provenance contracts. The shared runner clears repository pytest addopts so packaged contract checks are not silently deselected. This scope does not claim live provider or database verification.

The selected contract suite uses mocked provider responses and does not require a provider account.
