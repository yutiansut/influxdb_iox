//! This module contains plumbing to connect InfluxDB IOx extensions to
//! DataFusion

use std::{fmt, sync::Arc};

use arrow_deps::{
    arrow::record_batch::RecordBatch,
    datafusion::{
        execution::context::{ExecutionContextState, QueryPlanner},
        logical_plan::{LogicalPlan, UserDefinedLogicalNode},
        physical_plan::{
            collect,
            merge::MergeExec,
            planner::{DefaultPhysicalPlanner, ExtensionPlanner},
            ExecutionPlan, PhysicalPlanner, SendableRecordBatchStream,
        },
        prelude::*,
    },
};

use crate::exec::schema_pivot::{SchemaPivotExec, SchemaPivotNode};

use tracing::debug;

// Reuse DataFusion error and Result types for this module
pub use arrow_deps::datafusion::error::{DataFusionError as Error, Result};

use super::counters::ExecutionCounters;

/// This structure implements the DataFusion notion of "query planner"
/// and is needed to create plans with the IOx extension nodes.
struct IOxQueryPlanner {}

impl QueryPlanner for IOxQueryPlanner {
    fn rewrite_logical_plan(&self, plan: LogicalPlan) -> Result<LogicalPlan> {
        // TODO: implement any IOx specific query rewrites needed
        Ok(plan)
    }

    /// Given a `LogicalPlan` created from above, create an
    /// `ExecutionPlan` suitable for execution
    fn create_physical_plan(
        &self,
        logical_plan: &LogicalPlan,
        ctx_state: &ExecutionContextState,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        // Teach the default physical planner how to plan SchemaPivot nodes.
        let physical_planner =
            DefaultPhysicalPlanner::with_extension_planner(Arc::new(IOxExtensionPlanner {}));
        // Delegate most work of physical planning to the default physical planner
        physical_planner.create_physical_plan(logical_plan, ctx_state)
    }
}

/// Physical planner for InfluxDB IOx extension plans
struct IOxExtensionPlanner {}

impl ExtensionPlanner for IOxExtensionPlanner {
    /// Create a physical plan for an extension node
    fn plan_extension(
        &self,
        node: &dyn UserDefinedLogicalNode,
        inputs: Vec<Arc<dyn ExecutionPlan>>,
        _ctx_state: &ExecutionContextState,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        match node.as_any().downcast_ref::<SchemaPivotNode>() {
            Some(schema_pivot) => {
                assert_eq!(inputs.len(), 1, "Inconsistent number of inputs");
                Ok(Arc::new(SchemaPivotExec::new(
                    inputs[0].clone(),
                    schema_pivot.schema().as_ref().clone().into(),
                )))
            }
            None => Err(Error::Internal(format!(
                "Unknown extension node type {:?}",
                node
            ))),
        }
    }
}

/// This is an execution context for planning in IOx.
/// It wraps a DataFusion execution context and incudes
/// statistical counters.
///
/// Eventually we envision this as also managing resources
/// and providing visibility into what plans are running
pub struct IOxExecutionContext {
    counters: Arc<ExecutionCounters>,
    inner: ExecutionContext,
}

impl fmt::Debug for IOxExecutionContext {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("IOxExecutionContext")
            .field("counters", &self.counters)
            .field("inner", &"<DataFusion ExecutionContext>")
            .finish()
    }
}

impl IOxExecutionContext {
    /// Create an ExecutionContext suitable for executing DataFusion plans
    pub fn new(counters: Arc<ExecutionCounters>) -> Self {
        const BATCH_SIZE: usize = 1000;

        // TBD: Should we be reusing an execution context across all executions?
        let config = ExecutionConfig::new().with_batch_size(BATCH_SIZE);

        let config = config.with_query_planner(Arc::new(IOxQueryPlanner {}));
        let inner = ExecutionContext::with_config(config);

        Self { counters, inner }
    }

    /// returns a reference to the inner datafusion execution context
    pub fn inner(&self) -> &ExecutionContext {
        &self.inner
    }

    /// returns a mutable reference to the inner datafusion execution context
    pub fn inner_mut(&mut self) -> &mut ExecutionContext {
        &mut self.inner
    }

    /// Prepare a SQL statement for execution. This assumes that any
    /// tables referenced in the SQL have been registered with this context
    pub async fn prepare_sql(&mut self, sql: &str) -> Result<Arc<dyn ExecutionPlan>> {
        let logical_plan = self.inner.sql(sql)?.to_logical_plan();
        self.prepare_plan(&logical_plan).await
    }

    /// Prepare (optimize + plan) a pre-created logical plan for execution
    pub async fn prepare_plan(&self, plan: &LogicalPlan) -> Result<Arc<dyn ExecutionPlan>> {
        debug!(
            "Creating plan: Initial plan\n----\n{}\n{}\n----",
            plan.display_indent_schema(),
            plan.display_graphviz(),
        );

        let plan = self.inner.optimize(&plan)?;

        debug!(
            "Creating plan: Optimized plan\n----\n{}\n{}\n----",
            plan.display_indent_schema(),
            plan.display_graphviz(),
        );

        self.inner.create_physical_plan(&plan)
    }

    /// Executes the logical plan using DataFusion and produces RecordBatches
    pub async fn collect(&self, physical_plan: Arc<dyn ExecutionPlan>) -> Result<Vec<RecordBatch>> {
        self.counters.inc_plans_run();

        debug!("Running plan, physical:\n{:?}", physical_plan);

        collect(physical_plan).await
    }

    /// Executes the physical plan and produces a RecordBatchStream to stream
    /// over the result that iterates over the results.
    pub async fn execute(
        &self,
        physical_plan: Arc<dyn ExecutionPlan>,
    ) -> Result<SendableRecordBatchStream> {
        if physical_plan.output_partitioning().partition_count() <= 1 {
            physical_plan.execute(0).await
        } else {
            // merge into a single partition
            let plan = MergeExec::new(physical_plan);
            // MergeExec must produce a single partition
            assert_eq!(1, plan.output_partitioning().partition_count());
            plan.execute(0).await
        }
    }
}
