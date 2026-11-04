use std::sync::Arc;

use datafusion::optimizer::OptimizerRule;

use datafusion::optimizer::common_subexpr_eliminate::CommonSubexprEliminate;
use datafusion::optimizer::decorrelate_predicate_subquery::DecorrelatePredicateSubquery;
use datafusion::optimizer::eliminate_cross_join::EliminateCrossJoin;
use datafusion::optimizer::eliminate_duplicated_expr::EliminateDuplicatedExpr;
use datafusion::optimizer::eliminate_filter::EliminateFilter;
use datafusion::optimizer::eliminate_group_by_constant::EliminateGroupByConstant;
use datafusion::optimizer::eliminate_join::EliminateJoin;
use datafusion::optimizer::eliminate_limit::EliminateLimit;
use datafusion::optimizer::eliminate_outer_join::EliminateOuterJoin;
use datafusion::optimizer::extract_equijoin_predicate::ExtractEquijoinPredicate;
use datafusion::optimizer::filter_null_join_keys::FilterNullJoinKeys;
use datafusion::optimizer::optimize_unions::OptimizeUnions;
use datafusion::optimizer::propagate_empty_relation::PropagateEmptyRelation;
use datafusion::optimizer::push_down_filter::PushDownFilter;
use datafusion::optimizer::push_down_limit::PushDownLimit;
use datafusion::optimizer::replace_distinct_aggregate::ReplaceDistinctWithAggregate;
use datafusion::optimizer::scalar_subquery_to_join::ScalarSubqueryToJoin;
use datafusion::optimizer::simplify_expressions::SimplifyExpressions;
use datafusion::optimizer::single_distinct_to_groupby::SingleDistinctToGroupBy;

/// Return the optimizer rules needed for streaming. These may differ from the default DataFusion rules
pub fn get_default_optimizer_rules() -> Vec<Arc<dyn OptimizerRule + Sync + Send>> {
    vec![
        Arc::new(OptimizeUnions::new()),
        Arc::new(SimplifyExpressions::new()),
        Arc::new(ReplaceDistinctWithAggregate::new()),
        Arc::new(EliminateJoin::new()),
        Arc::new(DecorrelatePredicateSubquery::new()),
        Arc::new(ScalarSubqueryToJoin::new()),
        Arc::new(ExtractEquijoinPredicate::new()),
        // simplify expressions does not simplify expressions in subqueries, so we
        // run it again after running the optimizations that potentially converted
        // subqueries to joins
        Arc::new(SimplifyExpressions::new()),
        Arc::new(EliminateDuplicatedExpr::new()),
        Arc::new(EliminateFilter::new()),
        Arc::new(EliminateCrossJoin::new()),
        Arc::new(CommonSubexprEliminate::new()),
        Arc::new(EliminateLimit::new()),
        Arc::new(PropagateEmptyRelation::new()),
        Arc::new(FilterNullJoinKeys::default()),
        Arc::new(EliminateOuterJoin::new()),
        // Filters can't be pushed down past Limits, we should do PushDownFilter after PushDownLimit
        Arc::new(PushDownLimit::new()),
        Arc::new(PushDownFilter::new()),
        Arc::new(SingleDistinctToGroupBy::new()),
        // The previous optimizations added expressions and projections,
        // that might benefit from the following rules
        Arc::new(SimplifyExpressions::new()),
        Arc::new(CommonSubexprEliminate::new()),
        Arc::new(EliminateGroupByConstant::new()),
    ]
}
