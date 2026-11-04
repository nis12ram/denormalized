use async_trait::async_trait;
use std::sync::Arc;

use datafusion::catalog::Session;
use datafusion::error::Result;
use datafusion::execution::context::QueryPlanner;
use datafusion::logical_expr::LogicalPlan;
use datafusion::physical_plan::{ChildrenPropertiesMode, ExecutionPlan, ReplaceChildrenOptions};
use datafusion::physical_planner::{DefaultPhysicalPlanner, PhysicalPlanner};

use crate::physical_plan::continuous::streaming_window::StreamingWindowExec;
use crate::physical_plan::stream_table::DenormalizedStreamingTableExec;
use crate::planner::streaming_window::StreamingWindowPlanner;

#[derive(Debug)]
pub struct StreamingQueryPlanner {}

/// Assigns stable, deterministic node ids to the streaming operators that need
/// them for checkpointing.
///
/// DataFusion does not expose a global node id. We therefore mirror the
/// pre-order (children first) numbering that the previous DataFusion fork used:
/// every node in the physical plan consumes an id, but only the denormalized
/// nodes store it. This keeps checkpoint keys stable across restarts.
fn assign_node_ids(
    plan: Arc<dyn ExecutionPlan>,
    next_id: &mut usize,
) -> Result<Arc<dyn ExecutionPlan>> {
    let mut children = Vec::with_capacity(plan.children().len());
    for child in plan.children() {
        children.push(assign_node_ids(Arc::clone(child), next_id)?);
    }

    let plan = plan.replace_children(
        children,
        ReplaceChildrenOptions::new(ChildrenPropertiesMode::Recompute),
    )?;
    let node_id = *next_id;
    *next_id += 1;

    if let Some(streaming_window) = plan.downcast_ref::<StreamingWindowExec>() {
        Ok(streaming_window.with_node_id(node_id))
    } else if let Some(streaming_table) = plan.downcast_ref::<DenormalizedStreamingTableExec>() {
        Ok(streaming_table.with_node_id(node_id))
    } else {
        Ok(plan)
    }
}

#[async_trait]
impl QueryPlanner for StreamingQueryPlanner {
    /// Given a `LogicalPlan`, create an
    /// `ExecutionPlan` suitable for execution
    async fn create_physical_plan(
        &self,
        logical_plan: &LogicalPlan,
        session_state: &dyn Session,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let physical_planner = DefaultPhysicalPlanner::with_extension_planners(vec![Arc::new(
            StreamingWindowPlanner {},
        )]);

        let plan = physical_planner
            .create_physical_plan(logical_plan, session_state)
            .await?;

        let mut next_id = 0;
        assign_node_ids(plan, &mut next_id)
    }
}
