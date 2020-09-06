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

//! In-memory data source for presenting a Vec<RecordBatch> as a data source that can be
//! queried by DataFusion. This allows data to be pre-loaded into memory and then
//! repeatedly queried without incurring additional file I/O overhead.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use arrow::datatypes::{Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;

use crate::datasource::TableProvider;
use crate::error::{ExecutionError, Result};
use crate::physical_plan::memory::MemoryExec;
use crate::physical_plan::ExecutionPlan;

pub struct MemorySource {
    batches: Vec<Vec<RecordBatch>>,
}

impl MemorySource {
    /// Create a new in-memory table from the provided schema and record batches
    pub fn new(batches: Vec<Vec<RecordBatch>>) -> Result<Self> {
        Ok(Self {
            batches
        })
    }
}

/// In-memory table
pub struct MemTable {
    pub source: HashMap<String, Arc<MemorySource>>,
}

impl MemTable {
    /// Create a new in-memory table
    pub fn new() -> Self {
        Self {
            source: HashMap::new(),
        }
    }

    /// Create a new in-memory table from the provided compound table name and record batches
    pub fn add_source(
        &mut self,
        schema_name: &str,
        table_name: &str,
        partitions: Vec<Vec<RecordBatch>>
    ) {
        let compound_name = [schema_name, table_name].join(".");
        let source = MemorySource::new(partitions)?;
        self.source.insert(compound_name.to_string(), Arc::new(source));
    }
}

impl TableProvider for MemTable {
    fn name(&self) -> String {
        "memory".to_string()
    }

    fn scan(
        &self,
        schema_name: &str,
        table_name: &str,
        table_meta: SchemaRef,
        projection: &Option<Vec<usize>>,
        _batch_size: usize,
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

        let columns: Vec<usize> = match projection {
            Some(p) => p.clone(),
            None => {
                let l = table_meta.fields().len();
                let mut v = Vec::with_capacity(l);
                for i in 0..l {
                    v.push(i);
                }
                v
            }
        };

        let projected_columns: Result<Vec<Field>> = columns
            .iter()
            .map(|i| {
                if *i < table_meta.fields().len() {
                    Ok(self.schema.field(*i).clone())
                } else {
                    Err(ExecutionError::General(
                        "Projection index out of range".to_string(),
                    ))
                }
            })
            .collect();

        let projected_schema = Arc::new(Schema::new(projected_columns?));

        Ok(Arc::new(MemoryExec::try_new(
            &source.batches,
            projected_schema,
            projection.clone(),
        )?))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Int32Array;
    use arrow::datatypes::{DataType, Field, Schema};

    #[test]
    fn test_with_projection() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Int32, false),
            Field::new("c", DataType::Int32, false),
        ]));

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3])),
                Arc::new(Int32Array::from(vec![4, 5, 6])),
                Arc::new(Int32Array::from(vec![7, 8, 9])),
            ],
        )?;

        let mut provider = MemTable::new();
        provider.add_source("default", "test",vec![vec![batch]]);

        // scan with projection
        let exec = provider.scan("default", "test", schema, &Some(vec![2, 1]), 1024)?;
        let it = exec.execute(0)?;
        let batch2 = it.lock().expect("mutex lock").next_batch()?.unwrap();
        assert_eq!(2, batch2.schema().fields().len());
        assert_eq!("c", batch2.schema().field(0).name());
        assert_eq!("b", batch2.schema().field(1).name());
        assert_eq!(2, batch2.num_columns());

        Ok(())
    }

    #[test]
    fn test_without_projection() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Int32, false),
            Field::new("c", DataType::Int32, false),
        ]));

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3])),
                Arc::new(Int32Array::from(vec![4, 5, 6])),
                Arc::new(Int32Array::from(vec![7, 8, 9])),
            ],
        )?;

        let mut provider = MemTable::new();
        provider.add_source("default", "test",vec![vec![batch]]);

        let exec = provider.scan("default", "test", schema, &None, 1024)?;
        let it = exec.execute(0)?;
        let batch1 = it.lock().expect("mutex lock").next_batch()?.unwrap();
        assert_eq!(3, batch1.schema().fields().len());
        assert_eq!(3, batch1.num_columns());

        Ok(())
    }

    #[test]
    fn test_invalid_projection() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Int32, false),
            Field::new("c", DataType::Int32, false),
        ]));

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3])),
                Arc::new(Int32Array::from(vec![4, 5, 6])),
                Arc::new(Int32Array::from(vec![7, 8, 9])),
            ],
        )?;

        let mut provider = MemTable::new();
        provider.add_source("default", "test",vec![vec![batch]]);

        let projection: Vec<usize> = vec![0, 4];

        match provider.scan("default", "test", schema, &Some(projection), 1024) {
            Err(ExecutionError::General(e)) => {
                assert_eq!("\"Projection index out of range\"", format!("{:?}", e))
            }
            _ => assert!(false, "Scan should failed on invalid projection"),
        };

        Ok(())
    }
}
