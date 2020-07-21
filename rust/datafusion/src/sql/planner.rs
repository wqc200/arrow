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

//! SQL Query Planner (produces logical plan from SQL AST)

use std::sync::Arc;
#[cfg(feature = "bigdecimal")]
use bigdecimal::BigDecimal;

use crate::error::{ExecutionError, Result};
use crate::logicalplan::{
    Expr, FunctionMeta, LogicalPlan, LogicalPlanBuilder, Operator, ScalarValue,
};

use arrow::datatypes::*;

use crate::logicalplan::Expr::Alias;
use sqlparser::ast::{Statement, SetExpr, SelectItem, UnaryOperator, BinaryOperator, Function, OrderByExpr};
use sqlparser::ast::Expr as ASTExpr;
use sqlparser::ast::DataType as ASTDataType;

/// The SchemaProvider trait allows the query planner to obtain meta-data about tables and
/// functions referenced in SQL statements
pub trait SchemaProvider {
    /// Getter for a field description
    fn get_table_meta(&self, name: &str) -> Option<SchemaRef>;
    /// Getter for a UDF description
    fn get_function_meta(&self, name: &str) -> Option<Arc<FunctionMeta>>;
}

/// SQL query planner
pub struct SqlToRel<S: SchemaProvider> {
    schema_provider: S,
}

impl<S: SchemaProvider> SqlToRel<S> {
    /// Create a new query planner
    pub fn new(schema_provider: S) -> Self {
        SqlToRel { schema_provider }
    }

    fn relation(&self, id: &str) -> Result<LogicalPlan> {
        match self.schema_provider.get_table_meta(id.as_ref()) {
            Some(schema) => Ok(LogicalPlanBuilder::scan(
                "default",
                id,
                schema.as_ref(),
                None,
            )?
                .build()?),
            None => Err(ExecutionError::General(format!(
                "no schema found for table {}",
                id
            ))),
        }
    }

    /// Generate a logic plan from a SQL AST node
    pub fn sql_to_rel(&self, sql: &Statement) -> Result<LogicalPlan> {
        let plan = LogicalPlanBuilder::empty();

        match sql {
            Statement::Query(query) => {
                match query.body {
                    SetExpr::Select(s) => {
                        if s.having.is_some() {
                            return Err(ExecutionError::NotImplemented(
                                "HAVING is not implemented yet".to_string(),
                            ));
                        }

                        if s.from.len() > 0 {
                            let plan = self.relation(s.from.pop().unwrap().relation.table.name);
                        } else {
                            let plan = self.relation("dual".as_ref());
                        }

                        // parse the input relation so we have access to the row type
                        let plan = match s.from.pop().unwrap().relation {
                            TableFactor::Table {
                                name,
                                alias,
                                args,
                                with_hints,
                            }
                            None => self.relation("dual".as_ref())?,
                            _ => {}
                        };

                        // selection first
                        let plan = self.filter(&plan, &s.selection)?;

                        let projection_expr: Vec<Expr> = s.projection
                            .iter()
                            .map(|e| self.sql_to_rex(self.select_item(e), &plan.schema()))
                            .collect::<Result<Vec<Expr>>>()?;

                        let aggr_expr: Vec<Expr> = projection_expr
                            .iter()
                            .filter(|e| is_aggregate_expr(e))
                            .map(|e| e.clone())
                            .collect();

                        // apply projection or aggregate
                        let plan = if s.group_by.len() > 0 || aggr_expr.len() > 0 {
                            self.aggregate(&plan, projection_expr, s.group_by, aggr_expr)?
                        } else {
                            self.project(&plan, projection_expr)?
                        };


                        let plan = self.order_by(&plan, query.order_by)?;

                        let plan = self.limit(&plan, &query.limit)?;

                        Ok(plan)
                    }
                    _ => {}
                }
            }
            _ => Err(ExecutionError::ExecutionError(format!(
                "sql_to_rel does not support this relation: {:?}",
                sql
            ))),
        }
    }

    fn select_item(&self, selectItem: &SelectItem) -> &ASTExpr {
        match selectItem {
            SelectItem::UnnamedExpr(expr) => expr,
            SelectItem::Wildcard => ASTExpr::Wildcard,
            _ => panic!("Expected UnnamedExpr"),
        }
    }

    /// Apply a filter to the plan
    fn filter(
        &self,
        input: &LogicalPlan,
        selection: &Option<ASTExpr>,
    ) -> Result<LogicalPlan> {
        match *selection {
            Some(ref filter_expr) => LogicalPlanBuilder::from(&input)
                .filter(self.sql_to_rex(filter_expr, &input.schema())?)?
                .build(),
            _ => Ok(input.clone()),
        }
    }

    /// Wrap a plan in a projection
    fn project(&self, input: &LogicalPlan, expr: Vec<Expr>) -> Result<LogicalPlan> {
        LogicalPlanBuilder::from(input).project(expr)?.build()
    }

    /// Wrap a plan in an aggregate
    fn aggregate(
        &self,
        input: &LogicalPlan,
        projection_expr: Vec<Expr>,
        group_by: Vec<ASTExpr>,
        aggr_expr: Vec<Expr>,
    ) -> Result<LogicalPlan> {
        let group_expr = group_by
            .iter()
            .map(|e| self.sql_to_rex(e, &input.schema()))
            .collect::<Result<Vec<Expr>>>()?;

        let group_by_count = group_expr.len();
        let aggr_count = aggr_expr.len();

        if group_by_count + aggr_count != projection_expr.len() {
            return Err(ExecutionError::General(
                "Projection references non-aggregate values".to_owned(),
            ));
        }

        let plan = LogicalPlanBuilder::from(&input)
            .aggregate(group_expr, aggr_expr)?
            .build()?;

        // wrap in projection to preserve final order of fields
        let mut projected_fields = Vec::with_capacity(group_by_count + aggr_count);
        let mut group_expr_index = 0;
        let mut aggr_expr_index = 0;
        for i in 0..projection_expr.len() {
            if is_aggregate_expr(&projection_expr[i]) {
                projected_fields.push(group_by_count + aggr_expr_index);
                aggr_expr_index += 1;
            } else {
                projected_fields.push(group_expr_index);
                group_expr_index += 1;
            }
        }

        // determine if projection is needed or not
        // NOTE this would be better done later in a query optimizer rule
        let mut projection_needed = false;
        for i in 0..projected_fields.len() {
            if projected_fields[i] != i {
                projection_needed = true;
                break;
            }
        }

        if projection_needed {
            self.project(
                &plan,
                projected_fields.iter().map(|i| Expr::Column(*i)).collect(),
            )
        } else {
            Ok(plan)
        }
    }

    /// Wrap a plan in a limit
    fn limit(
        &self,
        input: &LogicalPlan,
        limit: &Option<ASTExpr>,
    ) -> Result<LogicalPlan> {
        match *limit {
            Some(ref limit_expr) => {
                let n = match self.sql_to_rex(&limit_expr, &input.schema())? {
                    Expr::Literal(ScalarValue::Int64(n)) => Ok(n as usize),
                    _ => Err(ExecutionError::General(
                        "Unexpected expression for LIMIT clause".to_string(),
                    )),
                }?;

                LogicalPlanBuilder::from(&input).limit(n)?.build()
            }
            _ => Ok(input.clone()),
        }
    }

    /// Wrap the logical in a sort
    fn order_by(
        &self,
        input: &LogicalPlan,
        order_by_expr: Vec<OrderByExpr>,
    ) -> Result<LogicalPlan> {
        if order_by_expr.len() < 1 {
            Ok(input.clone())
        }

        let input_schema = input.schema();
        let order_by_rex: Result<Vec<Expr>> = order_by_expr
            .iter()
            .map(|e| {
                Ok(Expr::Sort {
                    expr: Box::new(
                        self.sql_to_rex(&e.expr, &input_schema).unwrap(),
                    ),
                    asc: e.asc.unwrap(),
                    // by default nulls first to be consistent with spark
                    nulls_first: e.nulls_first.unwrap_or(true),
                })
            })
            .collect();

        LogicalPlanBuilder::from(&input)
            .sort(order_by_rex?)?
            .build()
    }

    /// Generate a relational expression from a SQL expression
    pub fn sql_to_rex(&self, expr: &ASTExpr, schema: &Schema) -> Result<Expr> {
        match expr {
            #[cfg(not(feature = "bigdecimal"))]
            ASTExpr::Value(sqlparser::ast::Value::Number(String::from(n))) => {
                Ok(Expr::Literal(ScalarValue::Int64(n)))
            }
            #[cfg(feature = "bigdecimal")]
            ASTExpr::Value(sqlparser::ast::Value::Number(bigdecimal::BigDecimal::from(n))) => {
                Ok(Expr::Literal(ScalarValue::Float64(n)))
            }
            ASTExpr::Value(sqlparser::ast::Value::SingleQuotedString(ref s)) => {
                Ok(Expr::Literal(ScalarValue::Utf8(s.clone())))
            }

            // ASTExpr::AliasedExpr(ref expr, ref alias) => Ok(Alias(
            //     Box::new(self.sql_to_rex(&expr, schema)?),
            //     alias.to_owned(),
            // )),

            ASTExpr::Identifier(ref id) => {
                match schema.fields().iter().position(|c| c.name().eq(&id.value)) {
                    Some(index) => Ok(Expr::Column(index)),
                    None => Err(ExecutionError::ExecutionError(format!(
                        "Invalid identifier '{}' for schema {}",
                        id,
                        schema.to_string()
                    ))),
                }
            }

            ASTExpr::Wildcard => Ok(Expr::Wildcard),

            ASTExpr::Cast {
                ref expr,
                ref data_type,
            } => Ok(Expr::Cast {
                expr: Box::new(self.sql_to_rex(&expr, schema)?),
                data_type: convert_data_type(data_type)?,
            }),

            ASTExpr::IsNull(ref expr) => {
                Ok(Expr::IsNull(Box::new(self.sql_to_rex(expr, schema)?)))
            }

            ASTExpr::IsNotNull(ref expr) => {
                Ok(Expr::IsNotNull(Box::new(self.sql_to_rex(expr, schema)?)))
            }

            ASTExpr::UnaryOp {
                ref op,
                ref expr,
            } => match *op {
                UnaryOperator::Not => {
                    Ok(Expr::Not(Box::new(self.sql_to_rex(expr, schema)?)))
                }
                _ => Err(ExecutionError::InternalError(format!(
                    "SQL binary operator cannot be interpreted as a unary operator"
                ))),
            },

            ASTExpr::BinaryOp {
                ref left,
                ref op,
                ref right,
            } => {
                let operator = match op {
                    BinaryOperator::Gt => Operator::Gt,
                    BinaryOperator::GtEq => Operator::GtEq,
                    BinaryOperator::Lt => Operator::Lt,
                    BinaryOperator::LtEq => Operator::LtEq,
                    BinaryOperator::Eq => Operator::Eq,
                    BinaryOperator::NotEq => Operator::NotEq,
                    BinaryOperator::Plus => Operator::Plus,
                    BinaryOperator::Minus => Operator::Minus,
                    BinaryOperator::Multiply => Operator::Multiply,
                    BinaryOperator::Divide => Operator::Divide,
                    BinaryOperator::Modulus => Operator::Modulus,
                    BinaryOperator::And => Operator::And,
                    BinaryOperator::Or => Operator::Or,
                    //BinaryOperator::Not => Operator::Not,
                    BinaryOperator::Like => Operator::Like,
                    BinaryOperator::NotLike => Operator::NotLike,
                    _ => {
                        Err(ExecutionError::InternalError(format!(
                            "SQL binary operator \"{:?}\" not found", op
                        )))
                    }
                };

                Ok(Expr::BinaryExpr {
                    left: Box::new(self.sql_to_rex(&left, &schema)?),
                    op: operator,
                    right: Box::new(self.sql_to_rex(&right, &schema)?),
                })
            }

            //            &ASTNode::SQLOrderBy { ref expr, asc } => Ok(Expr::Sort {
            //                expr: Box::new(self.sql_to_rex(&expr, &schema)?),
            //                asc,
            //            }),
            ASTExpr::Function(Function { name, args, over, distinct }) => {
                //TODO: fix this hack
                match name.to_lowercase().as_ref() {
                    "min" | "max" | "sum" | "avg" => {
                        let rex_args = args
                            .iter()
                            .map(|a| self.sql_to_rex(a, schema))
                            .collect::<Result<Vec<Expr>>>()?;

                        // return type is same as the argument type for these aggregate
                        // functions
                        let return_type = rex_args[0].get_type(schema)?.clone();

                        Ok(Expr::AggregateFunction {
                            name: name.to_string(),
                            args: rex_args,
                            return_type,
                        })
                    }
                    "count" => {
                        let rex_args = args
                            .iter()
                            .map(|a| match a {
                                ASTExpr::Value(sqlparser::ast::Value::Number(_)) => {
                                    Ok(Expr::Literal(ScalarValue::UInt8(1)))
                                }
                                ASTExpr::Wildcard => {
                                    Ok(Expr::Literal(ScalarValue::UInt8(1)))
                                }
                                _ => self.sql_to_rex(a, schema),
                            })
                            .collect::<Result<Vec<Expr>>>()?;

                        Ok(Expr::AggregateFunction {
                            name: name.to_string(),
                            args: rex_args,
                            return_type: DataType::UInt64,
                        })
                    }
                    _ => match self.schema_provider.get_function_meta(name.to_string().as_str()) {
                        Some(fm) => {
                            let rex_args = args
                                .iter()
                                .map(|a| self.sql_to_rex(a, schema))
                                .collect::<Result<Vec<Expr>>>()?;

                            let mut safe_args: Vec<Expr> = vec![];
                            for i in 0..rex_args.len() {
                                safe_args.push(
                                    rex_args[i]
                                        .cast_to(fm.args()[i].data_type(), schema)?,
                                );
                            }

                            Ok(Expr::ScalarFunction {
                                name: name.to_string(),
                                args: safe_args,
                                return_type: fm.return_type().clone(),
                            })
                        }
                        _ => Err(ExecutionError::General(format!(
                            "Invalid function '{}'",
                            name
                        ))),
                    },
                }
            }

            _ => Err(ExecutionError::General(format!(
                "Unsupported ast node {:?} in sqltorel",
                expr
            ))),
        }
    }
}

/// Determine if an expression is an aggregate expression or not
fn is_aggregate_expr(e: &Expr) -> bool {
    match e {
        Expr::AggregateFunction { .. } => true,
        _ => false,
    }
}

/// Convert SQL data type to relational representation of data type
pub fn convert_data_type(sql: &ASTDataType) -> Result<DataType> {
    match sql {
        ASTDataType::Boolean => Ok(DataType::Boolean),
        ASTDataType::SmallInt => Ok(DataType::Int16),
        ASTDataType::Int => Ok(DataType::Int32),
        ASTDataType::BigInt => Ok(DataType::Int64),
        ASTDataType::Float(_) | ASTDataType::Real => Ok(DataType::Float64),
        ASTDataType::Double => Ok(DataType::Float64),
        ASTDataType::Char(_) | ASTDataType::Varchar(_) => Ok(DataType::Utf8),
        ASTDataType::Timestamp => Ok(DataType::Timestamp(TimeUnit::Nanosecond, None)),
        other => Err(ExecutionError::NotImplemented(format!(
            "Unsupported SQL type {:?}",
            other
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::logicalplan::FunctionType;
    use sqlparser::sqlparser::*;

    #[test]
    fn select_no_relation() {
        quick_test(
            "SELECT 1",
            "Projection: Int64(1)\
             \n  EmptyRelation",
        );
    }

    #[test]
    fn select_scalar_func_with_literal_no_relation() {
        quick_test(
            "SELECT sqrt(9)",
            "Projection: sqrt(CAST(Int64(9) AS Float64))\
             \n  EmptyRelation",
        );
    }

    #[test]
    fn select_simple_selection() {
        let sql = "SELECT id, first_name, last_name \
                   FROM person WHERE state = 'CO'";
        let expected = "Projection: #0, #1, #2\
                        \n  Selection: #4 Eq Utf8(\"CO\")\
                        \n    TableScan: person projection=None";
        quick_test(sql, expected);
    }

    #[test]
    fn select_neg_selection() {
        let sql = "SELECT id, first_name, last_name \
                   FROM person WHERE NOT state";
        let expected = "Projection: #0, #1, #2\
                        \n  Selection: NOT #4\
                        \n    TableScan: person projection=None";
        quick_test(sql, expected);
    }

    #[test]
    fn select_compound_selection() {
        let sql = "SELECT id, first_name, last_name \
                   FROM person WHERE state = 'CO' AND age >= 21 AND age <= 65";
        let expected = "Projection: #0, #1, #2\
            \n  Selection: #4 Eq Utf8(\"CO\") And #3 GtEq Int64(21) And #3 LtEq Int64(65)\
            \n    TableScan: person projection=None";
        quick_test(sql, expected);
    }

    #[test]
    fn test_timestamp_selection() {
        let sql = "SELECT state FROM person WHERE birth_date < CAST (158412331400600000 as timestamp)";

        let expected = "Projection: #4\
            \n  Selection: #6 Lt CAST(Int64(158412331400600000) AS Timestamp(Nanosecond, None))\
            \n    TableScan: person projection=None";

        quick_test(sql, expected);
    }

    #[test]
    fn select_all_boolean_operators() {
        let sql = "SELECT age, first_name, last_name \
                   FROM person \
                   WHERE age = 21 \
                   AND age != 21 \
                   AND age > 21 \
                   AND age >= 21 \
                   AND age < 65 \
                   AND age <= 65";
        let expected = "Projection: #3, #1, #2\
                        \n  Selection: #3 Eq Int64(21) \
                        And #3 NotEq Int64(21) \
                        And #3 Gt Int64(21) \
                        And #3 GtEq Int64(21) \
                        And #3 Lt Int64(65) \
                        And #3 LtEq Int64(65)\
                        \n    TableScan: person projection=None";
        quick_test(sql, expected);
    }

    #[test]
    fn select_simple_aggregate() {
        quick_test(
            "SELECT MIN(age) FROM person",
            "Aggregate: groupBy=[[]], aggr=[[MIN(#3)]]\
             \n  TableScan: person projection=None",
        );
    }

    #[test]
    fn test_sum_aggregate() {
        quick_test(
            "SELECT SUM(age) from person",
            "Aggregate: groupBy=[[]], aggr=[[SUM(#3)]]\
             \n  TableScan: person projection=None",
        );
    }

    #[test]
    fn select_simple_aggregate_with_groupby() {
        quick_test(
            "SELECT state, MIN(age), MAX(age) FROM person GROUP BY state",
            "Aggregate: groupBy=[[#4]], aggr=[[MIN(#3), MAX(#3)]]\
             \n  TableScan: person projection=None",
        );
    }

    #[test]
    fn test_wildcard() {
        quick_test(
            "SELECT * from person",
            "Projection: #0, #1, #2, #3, #4, #5, #6\
            \n  TableScan: person projection=None",
        );
    }

    #[test]
    fn select_count_one() {
        let sql = "SELECT COUNT(1) FROM person";
        let expected = "Aggregate: groupBy=[[]], aggr=[[COUNT(UInt8(1))]]\
                        \n  TableScan: person projection=None";
        quick_test(sql, expected);
    }

    #[test]
    fn select_count_column() {
        let sql = "SELECT COUNT(id) FROM person";
        let expected = "Aggregate: groupBy=[[]], aggr=[[COUNT(#0)]]\
                        \n  TableScan: person projection=None";
        quick_test(sql, expected);
    }

    #[test]
    fn select_scalar_func() {
        let sql = "SELECT sqrt(age) FROM person";
        let expected = "Projection: sqrt(CAST(#3 AS Float64))\
                        \n  TableScan: person projection=None";
        quick_test(sql, expected);
    }

    #[test]
    fn select_aliased_scalar_func() {
        let sql = "SELECT sqrt(age) AS square_people FROM person";
        let expected = "Projection: sqrt(CAST(#3 AS Float64)) AS square_people\
                        \n  TableScan: person projection=None";
        quick_test(sql, expected);
    }

    #[test]
    fn select_order_by() {
        let sql = "SELECT id FROM person ORDER BY id";
        let expected = "Sort: #0 ASC NULLS FIRST\
                        \n  Projection: #0\
                        \n    TableScan: person projection=None";
        quick_test(sql, expected);
    }

    #[test]
    fn select_order_by_desc() {
        let sql = "SELECT id FROM person ORDER BY id DESC";
        let expected = "Sort: #0 DESC NULLS FIRST\
                        \n  Projection: #0\
                        \n    TableScan: person projection=None";
        quick_test(sql, expected);
    }

    #[test]
    fn select_order_by_nulls_last() {
        quick_test(
            "SELECT id FROM person ORDER BY id DESC NULLS LAST",
            "Sort: #0 DESC NULLS LAST\
            \n  Projection: #0\
            \n    TableScan: person projection=None",
        );

        quick_test(
            "SELECT id FROM person ORDER BY id NULLS LAST",
            "Sort: #0 ASC NULLS LAST\
            \n  Projection: #0\
            \n    TableScan: person projection=None",
        );
    }

    #[test]
    fn select_group_by() {
        let sql = "SELECT state FROM person GROUP BY state";
        let expected = "Aggregate: groupBy=[[#4]], aggr=[[]]\
                        \n  TableScan: person projection=None";

        quick_test(sql, expected);
    }

    #[test]
    fn select_7480_1() {
        let sql = "SELECT c1, MIN(c12) FROM aggregate_test_100 GROUP BY c1, c13";
        let err = logical_plan(sql).expect_err("query should have failed");
        assert_eq!(
            "General(\"Projection references non-aggregate values\")",
            format!("{:?}", err)
        );
    }

    #[test]
    fn select_7480_2() {
        let sql = "SELECT c1, c13, MIN(c12) FROM aggregate_test_100 GROUP BY c1";
        let err = logical_plan(sql).expect_err("query should have failed");
        assert_eq!(
            "General(\"Projection references non-aggregate values\")",
            format!("{:?}", err)
        );
    }

    fn logical_plan(sql: &str) -> Result<LogicalPlan> {
        use sqlparser::dialect::*;
        use sqlparser::parser::Parser;
        let dialect = GenericSqlDialect {};
        let planner = SqlToRel::new(MockSchemaProvider {});
        let mut ast = Parser::parse_sql(&dialect, sql).unwrap();
        planner.sql_to_rel(&ast.pop().unwrap())
    }

    /// Create logical plan, write with formatter, compare to expected output
    fn quick_test(sql: &str, expected: &str) {
        let plan = logical_plan(sql).unwrap();
        assert_eq!(expected, format!("{:?}", plan));
    }

    struct MockSchemaProvider {}

    impl SchemaProvider for MockSchemaProvider {
        fn get_table_meta(&self, name: &str) -> Option<SchemaRef> {
            match name {
                "person" => Some(Arc::new(Schema::new(vec![
                    Field::new("id", DataType::UInt32, false),
                    Field::new("first_name", DataType::Utf8, false),
                    Field::new("last_name", DataType::Utf8, false),
                    Field::new("age", DataType::Int32, false),
                    Field::new("state", DataType::Utf8, false),
                    Field::new("salary", DataType::Float64, false),
                    Field::new(
                        "birth_date",
                        DataType::Timestamp(TimeUnit::Nanosecond, None),
                        false,
                    ),
                ]))),
                "aggregate_test_100" => Some(Arc::new(Schema::new(vec![
                    Field::new("c1", DataType::Utf8, false),
                    Field::new("c2", DataType::UInt32, false),
                    Field::new("c3", DataType::Int8, false),
                    Field::new("c4", DataType::Int16, false),
                    Field::new("c5", DataType::Int32, false),
                    Field::new("c6", DataType::Int64, false),
                    Field::new("c7", DataType::UInt8, false),
                    Field::new("c8", DataType::UInt16, false),
                    Field::new("c9", DataType::UInt32, false),
                    Field::new("c10", DataType::UInt64, false),
                    Field::new("c11", DataType::Float32, false),
                    Field::new("c12", DataType::Float64, false),
                    Field::new("c13", DataType::Utf8, false),
                ]))),
                _ => None,
            }
        }

        fn get_function_meta(&self, name: &str) -> Option<Arc<FunctionMeta>> {
            match name {
                "sqrt" => Some(Arc::new(FunctionMeta::new(
                    "sqrt".to_string(),
                    vec![Field::new("n", DataType::Float64, false)],
                    DataType::Float64,
                    FunctionType::Scalar,
                ))),
                _ => None,
            }
        }
    }
}
