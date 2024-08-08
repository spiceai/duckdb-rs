use arrow::datatypes::Schema;

use super::{
    arrow::{datatypes::SchemaRef, record_batch::RecordBatch},
    Statement,
};

/// An handle for the resulting RecordBatch of a query.
#[must_use = "Arrow is lazy and will do nothing unless consumed"]
pub struct Arrow<'stmt> {
    pub(crate) stmt: Option<&'stmt Statement<'stmt>>,
    pub(crate) schema: Option<SchemaRef>,
}

impl<'stmt> Arrow<'stmt> {
    #[inline]
    pub(crate) fn new(stmt: &'stmt Statement<'stmt>) -> Arrow<'stmt> {
        Arrow {
            stmt: Some(stmt),
            schema: None,
        }
    }

    #[inline]
    pub(crate) fn new_with_schema(stmt: &'stmt Statement<'stmt>, schema: SchemaRef) -> Arrow<'stmt> {
        Arrow {
            stmt: Some(stmt),
            schema: Some(schema),
        }
    }

    /// return arrow schema
    #[inline]
    pub fn get_schema(&self) -> SchemaRef {
        self.stmt.unwrap().stmt.schema()
    }
}

impl<'stmt> Iterator for Arrow<'stmt> {
    type Item = RecordBatch;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(schema) = &self.schema {
            Some(RecordBatch::from(&self.stmt?.stream_step(schema.clone())?))
        } else {
            Some(RecordBatch::from(&self.stmt?.step()?))
        }
    }
}
