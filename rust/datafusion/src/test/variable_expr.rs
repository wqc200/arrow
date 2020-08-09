use std::sync::Arc;

use crate::execution::physical_plan::VariableExpr;
use crate::logicalplan::ScalarValue;
use crate::error::{ExecutionError, Result};
use crate::datasource::{CsvFile, CsvReadOptions, MemTable, TableProvider};

use arrow::record_batch::RecordBatch;
use arrow::datatypes::{DataType, Field, Schema, ToByteSlice};
use arrow::array::{
    ArrayData,
    BinaryArray,
    Int8Array,
    Int16Array,
    Int32Array,
    Int64Array,
    UInt8Array,
    UInt16Array,
    UInt32Array,
    UInt64Array,
    Float32Array,
    Float64Array,
    StringArray,
};

pub fn create_table_dual() -> Box<dyn TableProvider +Send + Sync> {
    let dual_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, false),
    ]));
    let batch = RecordBatch::try_new(
        dual_schema.clone(),
        vec![
            Arc::new(Int32Array::from(vec![1])),
            Arc::new(StringArray::from(vec!["a"])),
        ],
    ).unwrap();
    let provider = MemTable::new(dual_schema.clone(), vec![vec![batch.clone()]]).unwrap();
    Box::new(provider)
}


pub struct ScalarVariable {
}

impl ScalarVariable {
    pub fn new() -> Self {
        Self {
        }
    }
}

impl VariableExpr for ScalarVariable {
    fn get_value(&self, variable_names: Vec<String>) -> Result<ScalarValue> {
        Ok(ScalarValue::Utf8("test".to_string()))
    }
}
