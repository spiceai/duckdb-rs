//! Validates the spiceai bundled build contract: the engine reports the clean
//! DuckDB release version (a dev version would make downloadable extensions 404),
//! and the statically linked extensions work with no runtime INSTALL/LOAD.
//!
//! This branch links icu but not vss — DuckDB 1.4's `package_build.py` has no
//! static-extension loader generation, which arrived upstream in 1.5 and is what
//! registers an out-of-tree extension at database open. The vss case is kept and
//! ignored so the gap is visible here rather than remembered: un-ignoring it is
//! the check that VSS static linking actually works, if it is ever added.
#![cfg(feature = "bundled")]

use duckdb::{Connection, Result};

/// On this branch the crate version is the DuckDB version, so the engine must
/// report it verbatim. (The 1.5.x branches encode it as 1.<MMmmpp>.x instead.)
fn expected_duckdb_version() -> String {
    format!("v{}", env!("CARGO_PKG_VERSION"))
}

#[test]
fn bundled_version_is_clean_release() -> Result<()> {
    let db = Connection::open_in_memory()?;
    let v: String = db.query_row("PRAGMA version", [], |r| r.get(0))?;
    assert_eq!(v, expected_duckdb_version());
    Ok(())
}

#[test]
#[ignore = "vss is not statically linked on this branch; HNSW needs a runtime INSTALL vss"]
fn vss_hnsw_index_without_install() -> Result<()> {
    let db = Connection::open_in_memory()?;
    db.execute_batch(
        "CREATE TABLE t (v FLOAT[3]);
         INSERT INTO t VALUES ([1.0, 2.0, 3.0]), ([2.0, 3.0, 4.0]);
         CREATE INDEX idx ON t USING HNSW (v);",
    )?;
    let nearest: i64 = db.query_row(
        "SELECT count(*) FROM (SELECT * FROM t ORDER BY array_distance(v, [1.0, 2.0, 3.0]::FLOAT[3]) LIMIT 1)",
        [],
        |r| r.get(0),
    )?;
    assert_eq!(nearest, 1);
    Ok(())
}

#[test]
fn icu_time_zone_without_install() -> Result<()> {
    let db = Connection::open_in_memory()?;
    let ts: String = db.query_row(
        "SELECT strftime(TIMESTAMPTZ '2026-01-01 00:00:00+00' AT TIME ZONE 'America/New_York', '%Y-%m-%d %H:%M')",
        [],
        |r| r.get(0),
    )?;
    assert_eq!(ts, "2025-12-31 19:00");
    Ok(())
}
