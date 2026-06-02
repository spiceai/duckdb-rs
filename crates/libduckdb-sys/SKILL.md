---
name: maintain-bundled-vss
description: >
  Maintain the statically-linked VSS (vector similarity search / HNSW) extension in the
  bundled DuckDB built by libduckdb-sys. Use when regenerating duckdb.tar.gz, bumping the
  duckdb-sources submodule, upgrading the DuckDB or duckdb-vss version, or debugging why
  the bundled cc build does or doesn't include vss. Companion to the C++-side guide at
  duckdb-sources/extension/vss/SKILL.md.
---

# Bundled VSS (HNSW) in libduckdb-sys

## How the bundled build works

The `bundled` (cc) feature compiles DuckDB from a checked-in source tarball, not from the
submodule directly:

- `duckdb-sources/` is a git submodule pinned to a commit of the `spiceai/duckdb` C++ fork.
  It is **excluded from the published crate** (`exclude = ["duckdb-sources"]`) and is only an
  input to source regeneration.
- `update_sources.py` runs the fork's `scripts/package_build.py` over a fixed extension list,
  copies the selected base + extension sources into `duckdb/`, writes `duckdb/manifest.json`
  (base + per-extension `cpp_files`/`include_dirs`), and produces the committed **`duckdb.tar.gz`**.
- `build_bundled_cc.rs` untars `duckdb.tar.gz`, reads `manifest.json`, compiles the base plus
  the extensions for which `extension_enabled()` is true, and defines
  `DUCKDB_EXTENSION_<NAME>_LINKED=1` for each.

`duckdb.tar.gz` is the actual compile input. **Any change to `duckdb-sources` only takes effect
after re-running `update_sources.py` and committing the regenerated `duckdb.tar.gz` + `manifest.json`.**

## VSS wiring (what makes vss statically linked)

1. `duckdb-sources/extension/vss` (in the C++ fork) vendors the VSS source — see
   `duckdb-sources/extension/vss/SKILL.md` for that side (submodule pin, `vss_config.py`).
2. `update_sources.py`: `"vss"` is in `EXTENSIONS`, so package_build packages its sources and
   emits a loader guarded by `#if DUCKDB_EXTENSION_VSS_LINKED` (DuckDB 1.5's package_build is
   called with `default_linked_extensions=[]`, so the guard, not a hardcoded list, decides).
3. `build_bundled_cc.rs`:
   - `extension_enabled("vss")` returns true → vss sources compiled + `DUCKDB_EXTENSION_VSS_LINKED=1`
     defined → the generated loader registers `VssExtension` in `LinkedExtensions()`, which
     DuckDB auto-loads at database open. No loader rewrite, no runtime `INSTALL`/`LOAD` needed.
   - `cfg.define("DUCKDB_USEARCH_USE_SIMSIMD", "0")` — required by duckdb-vss's usearch wrapper
     (`#define USEARCH_USE_SIMSIMD DUCKDB_USEARCH_USE_SIMSIMD`); matches duckdb-vss's CMake
     default. fp16 is bundled (`USEARCH_USE_FP16LIB=1`) and OpenMP off, both hardcoded in the
     wrapper. usearch compiles under c++11 (it gates c++17 behind `__cplusplus >= 201703L`).

## Regenerate the tarball

```bash
# Populate the submodule + the nested vss source (targeted; see gotcha below).
git submodule update --init crates/libduckdb-sys/duckdb-sources
git -C crates/libduckdb-sys/duckdb-sources submodule update --init extension/vss/upstream

python3 crates/libduckdb-sys/update_sources.py        # regenerates duckdb.tar.gz + manifest.json

# Verify vss is present and compiles.
python3 -c "import json; print('vss' in json.load(open('crates/libduckdb-sys/duckdb/manifest.json'))['extensions'])"
cargo build -p libduckdb-sys --features bundled
nm target/debug/build/libduckdb-sys-*/out/libduckdb.a | grep -i 'VssExtension\|HNSWModule'
```

## Upgrade procedure

When bumping DuckDB and/or duckdb-vss:

1. **C++ fork first** (see `duckdb-sources/extension/vss/SKILL.md`): on a branch of the new
   `spiceai-<version>`, bump `extension/vss/upstream` to the ABI-matched duckdb-vss ref (from
   the new DuckDB's `.github/config/extensions/vss.cmake`), audit for drift, merge.
2. **Bump the submodule here** to the merged C++ commit:
   ```bash
   cd crates/libduckdb-sys/duckdb-sources && git fetch <spiceai-remote> && git checkout <merged-sha>
   cd - && git add crates/libduckdb-sys/duckdb-sources
   ```
3. Re-check `build_bundled_cc.rs`: any new `DUCKDB_USEARCH_*`/`USEARCH_*` external define, or a
   new c++ standard requirement, must be reflected here.
4. **Regenerate** `duckdb.tar.gz` (above), rebuild, and validate the Spice runtime: with no
   `vss.duckdb_extension` on the host, HNSW indexes are created at load with no INSTALL/download.
5. Commit the submodule bump + `update_sources.py`/`build_bundled_cc.rs` (if changed) + the
   regenerated `duckdb.tar.gz` + `manifest.json`.

## Gotchas

- **Always regenerate + commit `duckdb.tar.gz` after touching `duckdb-sources`.** A submodule
  bump alone is a no-op for the build; the tarball is what compiles.
- When initializing `extension/vss/upstream`, do **not** recurse into duckdb-vss's own
  `duckdb`/`extension-ci-tools` submodules — they are large and unused. Use a targeted
  `git -C .../extension/vss/upstream submodule deinit -f duckdb extension-ci-tools` if a
  `--recursive` init pulled them in.
- **DuckDB version pinning (critical):** `update_sources.py` sets `SETUPTOOLS_SCM_PRETEND_VERSION`
  to the release version derived from the crate version (1.10503.x -> 1.5.3), so the generated
  sources report a clean `DUCKDB_VERSION` (e.g. `v1.5.3`). The duckdb-sources commit sits a few
  commits past the release tag (spiceai patches + vendored out-of-tree extensions like vss), so a
  bare `git describe` would yield a dev version (e.g. `v1.5.4-dev5`). A dev version flips DuckDB
  into resolving extensions by commit hash, so downloadable extensions (tpch/tpcds/...) **404** —
  breaking any test/feature that loads them. Keep this pin; override only via the same env var.
  (The tarball bytes still change when the duckdb-sources commit changes because `DUCKDB_SOURCE_ID`
  embeds the commit hash — that part is benign.)
- Downstream (spiceai) must patch BOTH `[patch.crates-io]` and
  `[patch."https://github.com/spiceai/duckdb-rs.git"]` when overriding duckdb to a fork/path —
  datafusion-table-providers pulls duckdb via the git source, and a split yields two
  `libduckdb-sys` packages (`links = "duckdb"` conflict).
