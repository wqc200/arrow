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

//! UDF support

use std::fmt;

use arrow::array::ArrayRef;
use arrow::datatypes::{DataType, Schema};

use crate::error::Result;
use crate::physical_plan::PhysicalExpr;

use arrow::record_batch::RecordBatch;
use fmt::{Debug, Formatter};
use std::{collections::HashMap, sync::Arc};

/// Scalar UDF
pub type ScalarUdf = Arc<dyn Fn(&[ArrayRef]) -> Result<ArrayRef> + Send + Sync>;

/// Scalar UDF Expression
#[derive(Clone)]
pub struct ScalarFunction {
    /// Function name
    pub name: String,
    /// Function argument meta-data
    pub arg_types: Vec<DataType>,
    /// Return type
    pub return_type: DataType,
    /// UDF implementation
    pub fun: ScalarUdf,
}

/// Something which provides information for particular scalar functions
pub trait ScalarFunctionRegistry {
    /// Return ScalarFunction for `name`
    fn lookup(&self, name: &str) -> Option<Arc<ScalarFunction>>;
}

impl ScalarFunctionRegistry for HashMap<String, Arc<ScalarFunction>> {
    fn lookup(&self, name: &str) -> Option<Arc<ScalarFunction>> {
        self.get(name).and_then(|func| Some(func.clone()))
    }
}

impl Debug for ScalarFunction {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("ScalarFunction")
            .field("name", &self.name)
            .field("arg_types", &self.arg_types)
            .field("return_type", &self.return_type)
            .field("fun", &"<FUNC>")
            .finish()
    }
}

impl ScalarFunction {
    /// Create a new ScalarFunction
    pub fn new(
        name: &str,
        arg_types: Vec<DataType>,
        return_type: DataType,
        fun: ScalarUdf,
    ) -> Self {
        Self {
            name: name.to_owned(),
            arg_types,
            return_type,
            fun,
        }
    }
}

/// Scalar UDF Physical Expression
pub struct ScalarFunctionExpr {
    fun: ScalarUdf,
    name: String,
    args: Vec<Arc<dyn PhysicalExpr>>,
    return_type: DataType,
}

impl Debug for ScalarFunctionExpr {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("ScalarFunctionExpr")
            .field("fun", &"<FUNC>")
            .field("name", &self.name)
            .field("args", &self.args)
            .field("return_type", &self.return_type)
            .finish()
    }
}

impl ScalarFunctionExpr {
    /// Create a new Scalar function
    pub fn new(
        name: &str,
        fun: ScalarUdf,
        args: Vec<Arc<dyn PhysicalExpr>>,
        return_type: &DataType,
    ) -> Self {
        Self {
            fun,
            name: name.to_owned(),
            args,
            return_type: return_type.clone(),
        }
    }
}

impl fmt::Display for ScalarFunctionExpr {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "{}({})",
            self.name,
            self.args
                .iter()
                .map(|e| format!("{}", e))
                .collect::<Vec<String>>()
                .join(", ")
        )
    }
}

impl PhysicalExpr for ScalarFunctionExpr {
    fn data_type(&self, _input_schema: &Schema) -> Result<DataType> {
        Ok(self.return_type.clone())
    }

    fn nullable(&self, _input_schema: &Schema) -> Result<bool> {
        Ok(true)
    }

    fn evaluate(&self, batch: &RecordBatch) -> Result<ArrayRef> {
        // evaluate the arguments
        let inputs = self
            .args
            .iter()
            .map(|e| e.evaluate(batch))
            .collect::<Result<Vec<_>>>()?;

        // evaluate the function
        let fun = self.fun.as_ref();
        (fun)(&inputs)
    }
}
