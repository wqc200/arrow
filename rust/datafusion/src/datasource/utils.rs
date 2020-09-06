
//! Collection of utility functions that are leveraged by the datasource

use std::collections::{HashMap, HashSet};
use std::fs::File;

use arrow::csv;
use arrow::datatypes::{Field, Schema, SchemaRef};
use arrow::error::Result as ArrowResult;
use arrow::record_batch::{RecordBatch, RecordBatchReader};
use std::string::String;
use std::sync::Arc;

use crate::datasource::TableProvider;
use crate::error::{ExecutionError, Result};
use crate::physical_plan::csv::CsvExec;
pub use crate::physical_plan::csv::CsvReadOptions;
use crate::physical_plan::{common, ExecutionPlan};

/// Csv infer schema from path and options
pub fn csv_infer_schema(path: &str, options: CsvReadOptions) -> Result<SchemaRef> {
    let mut filenames: Vec<String> = vec![];
    common::build_file_list(path, &mut filenames, options.file_extension.as_str())?;
    if filenames.is_empty() {
        return Err(ExecutionError::General("No files found".to_string()));
    }

    let schema = CsvExec::try_infer_schema(&filenames, &options)?;

    let schema_ref = Arc::new(schema);
    Ok(schema_ref)
}

/// Parquet infer schema form path
pub fn parquet_infer_schema(path: &str) -> Result<SchemaRef> {
    let mut filenames: Vec<String> = vec![];
    common::build_file_list(path, &mut filenames, ".parquet")?;
    if filenames.is_empty() {
        return Err(ExecutionError::General("No files found".to_string()));
    }

    let file = File::open(&filenames[0])?;
    let file_reader = Rc::new(SerializedFileReader::new(file)?);
    let mut arrow_reader = ParquetFileArrowReader::new(file_reader);
    let schema = arrow_reader.get_schema()?;

    let schema_ref = Arc::new(schema);
    Ok(schema_ref)
}

/// reading from a data source
pub fn load_data(
    schema_name: &str,
    table_name: &str,
    schema: SchemaRef,
    t: &dyn TableProvider
) -> Result<Vec<Vec<RecordBatch>>> {
    let exec = t.scan(schema_name, table_name, schema, &None, 1024 * 1024)?;

    let mut data: Vec<Vec<RecordBatch>> =
        Vec::with_capacity(exec.output_partitioning().partition_count());
    for partition in 0..exec.output_partitioning().partition_count() {
        let it = exec.execute(partition)?;
        let mut it = it.lock().unwrap();
        let mut partition_batches = vec![];
        while let Ok(Some(batch)) = it.next_batch() {
            partition_batches.push(batch);
        }
        data.push(partition_batches);
    }

    Ok(data)
}
