//! Arrow scan support for DuckDB
//!
//! This module provides the ability to register Arrow streams as virtual views in DuckDB.

use std::ffi::CString;

use crate::arrow::ffi_stream::FFI_ArrowArrayStream;
use crate::{error::error_from_duckdb_code, ffi, Connection, Error, Result};

impl Connection {
    /// Registers a temporary view in DuckDB based on an Arrow stream.
    ///
    /// Note the underlying `duckdb_arrow_scan` C API is marked for deprecation.
    /// However, similar functionality will be preserved in a new yet-to-be-determined API.
    ///
    /// # Arguments
    ///
    /// * `view_name`: The name of the view to register
    /// * `arrow_scan`: The Arrow stream to register
    pub fn register_arrow_scan_view(&self, view_name: &str, arrow_scan: &FFI_ArrowArrayStream) -> Result<()> {
        let conn = self.db.borrow().con;
        let c_str = CString::new(view_name).map_err(Error::NulError)?;
        let transmuted_arrow_scan = arrow_scan as *const _ as ffi::duckdb_arrow_stream;
        let r = unsafe { ffi::duckdb_arrow_scan(conn, c_str.as_ptr(), transmuted_arrow_scan) };
        if r != ffi::DuckDBSuccess {
            return error_from_duckdb_code(r, Some("duckdb_arrow_scan failed to register view".to_string()));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::arrow::{
        array::{Int32Array, StringArray},
        datatypes::{DataType, Field, Schema, SchemaRef},
        error::ArrowError,
        record_batch::RecordBatch,
    };
    use std::sync::Arc;

    /// A simple RecordBatchReader implementation for testing
    struct TestRecordBatchReader {
        schema: SchemaRef,
        batches: Vec<RecordBatch>,
        index: usize,
    }

    impl TestRecordBatchReader {
        fn new(batches: Vec<RecordBatch>) -> Self {
            let schema = batches[0].schema();
            TestRecordBatchReader {
                schema,
                batches,
                index: 0,
            }
        }
    }

    impl Iterator for TestRecordBatchReader {
        type Item = std::result::Result<RecordBatch, ArrowError>;

        fn next(&mut self) -> Option<Self::Item> {
            if self.index < self.batches.len() {
                let batch = self.batches[self.index].clone();
                self.index += 1;
                Some(Ok(batch))
            } else {
                None
            }
        }
    }

    impl crate::arrow::record_batch::RecordBatchReader for TestRecordBatchReader {
        fn schema(&self) -> SchemaRef {
            Arc::clone(&self.schema)
        }
    }

    #[test]
    fn test_register_arrow_scan_view() -> Result<()> {
        let db = Connection::open_in_memory()?;

        let id_array = Int32Array::from(vec![1, 2, 3, 4, 5]);
        let name_array = StringArray::from(vec!["Alice", "Bob", "Charlie", "Dave", "Eve"]);

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
        ]));

        let record_batch = RecordBatch::try_new(schema, vec![Arc::new(id_array), Arc::new(name_array)])
            .expect("Failed to create record batch");

        let reader = TestRecordBatchReader::new(vec![record_batch]);

        let stream = FFI_ArrowArrayStream::new(
            Box::new(reader) as Box<dyn crate::arrow::record_batch::RecordBatchReader + Send>
        );

        db.register_arrow_scan_view("test_view", &stream)?;

        let rows = db
            .prepare("SELECT id, name FROM test_view ORDER BY id")?
            .query_map([], |row| Ok((row.get::<_, i32>(0)?, row.get::<_, String>(1)?)))?
            .collect::<Result<Vec<_>>>()?;

        assert_eq!(rows.len(), 5);
        assert_eq!(rows[0], (1, "Alice".to_string()));
        assert_eq!(rows[4], (5, "Eve".to_string()));

        Ok(())
    }
}
