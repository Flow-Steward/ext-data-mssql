# Changelog

## 1.3.8

- Rebuild native Linux payloads from official Toolbox v1.17.0 with the reviewed dependency security updates and Go 1.27.4.
- Build native artifacts in CI and generate checksums and provenance from the resulting binaries.

## 1.3.7

- Clarify workflow action names, descriptions and field help; add setup and input examples. Existing action IDs, input schemas and runtime behavior are preserved. Move the unchanged test launcher under tests/ so installer runtime checks accept the package.


## 1.3.4

- Rebuild with checksum-verified official Go 1.27.4 SDK security fixes. The pinned Go 1.27.3 image supplies only the bootstrap environment; Toolbox source, all 106 module pins and safety patches remain unchanged.

## 1.3.3

- Upgrade pinned Toolbox to v1.16.0, Go 1.27.3 and the reviewed dependency baseline. Port bound SQL and native safety checks to current driver interfaces while retaining approved connection profiles.

## 1.3.2

- Rebuild pinned Toolbox v1.3.0 with Go 1.26.10 and security dependency floors, preserving database safety patches and host behavior.

## 1.3.1

- Use official database brand artwork with recorded source attribution.


## 1.3.0

- Publish Microsoft SQL Server as an independent extension and repository.
- Preserve installed IDs, native binaries and read/write behavior.
- Narrow settings, runtime profiles and user-facing pages to this database.
