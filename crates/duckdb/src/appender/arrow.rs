use super::{ffi, Appender, Result};
use crate::{error::result_from_duckdb_appender, vtab::arrow::duckdb_error_to_string, Error};
use arrow::{
    array::{ArrayData, StructArray},
    ffi::{FFI_ArrowArray, FFI_ArrowSchema},
    record_batch::RecordBatch,
};
use ffi::{duckdb_append_data_chunk, duckdb_vector_size};
use std::ptr;

impl Appender<'_> {
    /// Append one record batch
    ///
    /// ## Example
    ///
    /// ```rust,no_run
    /// # use duckdb::{Connection, Result, params};
    ///   use arrow::record_batch::RecordBatch;
    /// fn insert_record_batch(conn: &Connection,record_batch:RecordBatch) -> Result<()> {
    ///     let mut app = conn.appender("foo")?;
    ///     app.append_record_batch(record_batch)?;
    ///     Ok(())
    /// }
    /// ```
    ///
    /// # Failure
    ///
    /// Will return `Err` if append column count not the same with the table schema
    #[inline]
    pub fn append_record_batch(&mut self, record_batch: RecordBatch) -> Result<()> {
        if record_batch.num_rows() == 0 {
            return Ok(());
        }

        let connection_ptr = {
            let conn_ref = self.conn.db.borrow();
            conn_ref.con
        };

        let mut ffi_schema = FFI_ArrowSchema::try_from(record_batch.schema().as_ref())
            .map_err(|err| Error::DuckDBFailure(ffi::Error::new(ffi::DuckDBError), Some(err.to_string())))?;
        let schema_ptr = &mut ffi_schema as *mut FFI_ArrowSchema as *mut ffi::ArrowSchema;
        let mut converted_schema: ffi::duckdb_arrow_converted_schema = ptr::null_mut();
        let schema_err = unsafe { ffi::duckdb_schema_from_arrow(connection_ptr, schema_ptr, &mut converted_schema) };
        if !schema_err.is_null() {
            let message = duckdb_error_to_string(schema_err);
            return Err(Error::DuckDBFailure(ffi::Error::new(ffi::DuckDBError), Some(message)));
        }

        let vector_size = unsafe { duckdb_vector_size() } as usize;
        let num_rows = record_batch.num_rows();

        // Process record batch in chunks that fit within DuckDB's vector size
        let mut offset = 0;
        while offset < num_rows {
            let slice_len = std::cmp::min(vector_size, num_rows - offset);
            let slice = record_batch.slice(offset, slice_len);

            let struct_array = StructArray::from(slice);
            let array_data = ArrayData::from(struct_array);
            let mut ffi_array = FFI_ArrowArray::new(&array_data);
            let array_ptr = &mut ffi_array as *mut FFI_ArrowArray as *mut ffi::ArrowArray;
            let mut out_chunk: ffi::duckdb_data_chunk = ptr::null_mut();
            let chunk_err = unsafe {
                ffi::duckdb_data_chunk_from_arrow(connection_ptr, array_ptr, converted_schema, &mut out_chunk)
            };
            if !chunk_err.is_null() {
                let message = duckdb_error_to_string(chunk_err);
                unsafe {
                    if !converted_schema.is_null() {
                        let mut schema_ptr = converted_schema;
                        ffi::duckdb_destroy_arrow_converted_schema(&mut schema_ptr);
                    }
                }
                return Err(Error::DuckDBFailure(ffi::Error::new(ffi::DuckDBError), Some(message)));
            }

            let rc = unsafe { duckdb_append_data_chunk(self.app, out_chunk) };
            let append_result = result_from_duckdb_appender(rc, &mut self.app);
            unsafe {
                ffi::duckdb_destroy_data_chunk(&mut out_chunk);
            }
            append_result?;

            offset += slice_len;
        }

        unsafe {
            if !converted_schema.is_null() {
                let mut schema_ptr = converted_schema;
                ffi::duckdb_destroy_arrow_converted_schema(&mut schema_ptr);
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod test {
    use crate::{Connection, Result};
    use arrow::{
        array::{Int32Array, Int8Array, StringArray},
        datatypes::{DataType, Field, Schema},
        record_batch::RecordBatch,
    };
    use std::sync::Arc;

    #[test]
    fn test_append_record_batch() -> Result<()> {
        let db = Connection::open_in_memory()?;
        db.execute_batch("CREATE TABLE foo(id TINYINT not null,area TINYINT not null,name Varchar)")?;
        {
            let id_array = Int8Array::from(vec![1, 2, 3, 4, 5]);
            let area_array = Int8Array::from(vec![11, 22, 33, 44, 55]);
            let name_array = StringArray::from(vec![Some("11"), None, None, Some("44"), None]);
            let schema = Schema::new(vec![
                Field::new("id", DataType::Int8, true),
                Field::new("area", DataType::Int8, true),
                Field::new("name", DataType::Utf8, true),
            ]);
            let record_batch = RecordBatch::try_new(
                Arc::new(schema),
                vec![Arc::new(id_array), Arc::new(area_array), Arc::new(name_array)],
            )
            .unwrap();
            let mut app = db.appender("foo")?;
            app.append_record_batch(record_batch)?;
        }
        let mut stmt = db.prepare("SELECT id, area, name FROM foo")?;
        let rbs: Vec<RecordBatch> = stmt.query_arrow([])?.collect();
        assert_eq!(rbs.iter().map(|op| op.num_rows()).sum::<usize>(), 5);
        Ok(())
    }

    #[test]
    fn test_append_record_batch_large() -> Result<()> {
        let record_count = usize::pow(2, 16) + 1;
        let db = Connection::open_in_memory()?;
        db.execute_batch("CREATE TABLE foo(id INT)")?;
        {
            let id_array = Int32Array::from((0..record_count as i32).collect::<Vec<_>>());
            let schema = Schema::new(vec![Field::new("id", DataType::Int32, true)]);
            let record_batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(id_array)]).unwrap();
            let mut app = db.appender("foo")?;
            app.append_record_batch(record_batch)?;
        }
        let count: usize = db.query_row("SELECT COUNT(*) FROM foo", [], |row| row.get(0))?;
        assert_eq!(count, record_count);

        // Verify the data is correct
        let sum: i64 = db.query_row("SELECT SUM(id) FROM foo", [], |row| row.get(0))?;
        let expected_sum: i64 = (0..record_count as i64).sum();
        assert_eq!(sum, expected_sum);

        Ok(())
    }
}
