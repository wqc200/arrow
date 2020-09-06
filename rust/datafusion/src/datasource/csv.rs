// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! CSV data source
//!
//! This CSV data source allows CSV files to be used as input for queries.
//!
//! Example:
//!
//! ```
//! use datafusion::datasource::TableProvider;
//! use datafusion::datasource::csv::{CsvFile, CsvReadOptions};
//!
//! let testdata = std::env::var("ARROW_TEST_DATA").expect("ARROW_TEST_DATA not defined");
//! let csvdata = CsvFile::try_new(
//!     &format!("{}/csv/aggregate_test_100.csv", testdata),
//!     CsvReadOptions::new().delimiter(b'|'),
//! ).unwrap();
//! let schema = csvdata.schema();
//! ```

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

pub struct CsvSource {
    path: String,
    options: CsvReadOptions,
}

impl CsvSource {
    /// Create a new csv source from the provided path and options
    pub fn new(path: &str, options: CsvReadOptions) -> Result<Self> {
        Ok(Self {
            path: path.to_string(),
            options,
        })
    }
}

/// Represents a CSV file with a provided schema
pub struct CsvFile {
    pub source: HashMap<String, Arc<CsvSource>>,
}

impl CsvFile {
    /// Create a new csv table
    pub fn new() -> Self {
        Self {
            source: HashMap::new(),
        }
    }

    /// Attempt to add a new `CsvSource` into `CsvFile`
    pub fn add_source(
        &mut self,
        schema_name: &str,
        table_name: &str,
        path: &str,
        options: CsvReadOptions,
    ) {
        let compound_name = [schema_name, table_name].join(".");
        let source = CsvSource::new(path, options)?;
        self.source.insert(compound_name.to_string(), Arc::new(source));
    }
}

impl TableProvider for CsvFile {
    fn name(&self) -> String {
        "csv".to_string()
    }

    fn scan(
        &self,
        schema_name: &str,
        table_name: &str,
        table_meta: SchemaRef,
        projection: &Option<Vec<usize>>,
        batch_size: usize,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let compound_name = [schema_name, table_name].join(".");
        let source;
        if let Some(s) = self.source.get(compound_name).map(|s| s) {
            source = s
        } else {
            return Err(ExecutionError::General(format!(
                "Source not found {}.{}",
                schema_name,
                table_name
            )))
        }

        Ok(Arc::new(CsvExec::try_new(
            &source.path,
            table_meta,
            source.options,
            projection.clone(),
            batch_size,
        )?))
    }
}

/// Iterator over CSV batches
// TODO: usage example (rather than documenting `new()`)
pub struct CsvBatchIterator {
    schema: SchemaRef,
    reader: csv::Reader<File>,
}

impl CsvBatchIterator {
    #[allow(missing_docs)]
    pub fn try_new(
        filename: &str,
        schema: SchemaRef,
        has_header: bool,
        delimiter: Option<u8>,
        projection: &Option<Vec<usize>>,
        batch_size: usize,
    ) -> Result<Self> {
        let file = File::open(filename)?;
        let reader = csv::Reader::new(
            file,
            schema.clone(),
            has_header,
            delimiter,
            batch_size,
            projection.clone(),
        );

        let projected_schema = match projection {
            Some(p) => {
                let projected_fields: Vec<Field> =
                    p.iter().map(|i| schema.fields()[*i].clone()).collect();

                Arc::new(Schema::new(projected_fields))
            }
            None => schema,
        };

        Ok(Self {
            schema: projected_schema,
            reader,
        })
    }
}

impl RecordBatchReader for CsvBatchIterator {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn next_batch(&mut self) -> ArrowResult<Option<RecordBatch>> {
        self.reader.next()
    }
}
