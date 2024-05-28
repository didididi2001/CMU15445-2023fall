#include <algorithm>
#include <memory>
#include "catalog/column.h"
#include "catalog/schema.h"
#include "common/exception.h"
#include "common/macros.h"
#include "execution/expressions/column_value_expression.h"
#include "execution/expressions/comparison_expression.h"
#include "execution/expressions/constant_value_expression.h"
#include "execution/expressions/logic_expression.h"
#include "execution/plans/abstract_plan.h"
#include "execution/plans/filter_plan.h"
#include "execution/plans/hash_join_plan.h"
#include "execution/plans/nested_loop_join_plan.h"
#include "execution/plans/projection_plan.h"
#include "optimizer/optimizer.h"
#include "type/type_id.h"

namespace bustub {

auto ParserLogicExpression(const AbstractExpressionRef &expr, std::vector<AbstractExpressionRef> &left_exprs,
                           std::vector<AbstractExpressionRef> &right_exprs) -> bool {
  auto logic_expr = std::dynamic_pointer_cast<LogicExpression>(expr);
  if (logic_expr) {
    auto left_result = ParserLogicExpression(expr->GetChildAt(0), left_exprs, right_exprs);
    auto right_result = ParserLogicExpression(expr->GetChildAt(1), left_exprs, right_exprs);
    return left_result && right_result;
  }
  auto equal_expr = std::dynamic_pointer_cast<ComparisonExpression>(expr);
  if (equal_expr) {
    if (equal_expr) {
      auto cmp_type = equal_expr->comp_type_;
      if (cmp_type == ComparisonType::Equal) {
        auto left_value = std::dynamic_pointer_cast<ColumnValueExpression>(equal_expr->GetChildAt(0));
        if (left_value) {
          std::cout << "left_value" << left_value->ToString() << std::endl;
          auto right_value = std::dynamic_pointer_cast<ColumnValueExpression>(equal_expr->GetChildAt(1));
          if (right_value) {
            std::cout << "right_value" << right_value->ToString() << std::endl;
            if (left_value->GetTupleIdx() == 0) {
              left_exprs.push_back(left_value);
              right_exprs.push_back(right_value);
            } else {
              left_exprs.push_back(right_value);
              right_exprs.push_back(left_value);
            }
            return true;
          }
        }
      }
    }
  }
  return false;
}

auto Optimizer::OptimizeNLJAsHashJoin(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  // TODO(student): implement NestedLoopJoin -> HashJoin optimizer rule
  // Note for 2023 Fall: You should support join keys of any number of conjunction of equi-condistions:
  // E.g. <column expr> = <column expr> AND <column expr> = <column expr> AND ...
  std::vector<AbstractPlanNodeRef> children;

  for (const auto &child : plan->GetChildren()) {
    children.emplace_back(OptimizeNLJAsHashJoin(child));
  }
  auto optimized_plan = plan->CloneWithChildren(std::move(children));

  if (optimized_plan->GetType() == PlanType::NestedLoopJoin) {
    const auto &nlj_plan = dynamic_cast<const NestedLoopJoinPlanNode &>(*optimized_plan);
    auto &predicate = nlj_plan.predicate_;
    std::vector<AbstractExpressionRef> left_exprs;
    std::vector<AbstractExpressionRef> right_exprs;
    if (predicate != nullptr) {
      auto is_success = ParserLogicExpression(predicate, left_exprs, right_exprs);
      if (is_success) {
        return std::make_shared<HashJoinPlanNode>(nlj_plan.output_schema_, nlj_plan.GetLeftPlan(),
                                                  nlj_plan.GetRightPlan(), std::move(left_exprs),
                                                  std::move(right_exprs), nlj_plan.GetJoinType());
      }
    }
  }
  return optimized_plan;
}

}  // namespace bustub
