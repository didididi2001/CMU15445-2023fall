#include "execution/expressions/column_value_expression.h"
#include "execution/expressions/comparison_expression.h"
#include "execution/expressions/logic_expression.h"
#include "execution/plans/index_scan_plan.h"
#include "execution/plans/seq_scan_plan.h"
#include "optimizer/optimizer.h"
#include "storage/index/generic_key.h"

namespace bustub {

auto Optimizer::OptimizeSeqScanAsIndexScan(const bustub::AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  // TODO(student): implement seq scan with predicate -> index scan optimizer rule
  // The Filter Predicate Pushdown has been enabled for you in optimizer.cpp when forcing starter rule
  std::vector<AbstractPlanNodeRef> children;
  for (const auto &child : plan->GetChildren()) {
    children.emplace_back(OptimizeMergeFilterScan(child));
  }

  auto optimized_plan = plan->CloneWithChildren(std::move(children));
  if (optimized_plan->GetType() == PlanType::SeqScan) {
    const auto &seq_scan_plan = dynamic_cast<const SeqScanPlanNode &>(*optimized_plan);
    BUSTUB_ASSERT(plan->children_.empty(), "must have no children");
    // check if there index
    if (seq_scan_plan.filter_predicate_ != nullptr) {
      auto table_name = seq_scan_plan.table_name_;
      auto indexes = catalog_.GetTableIndexes(table_name);
      auto logic_expr = std::dynamic_pointer_cast<LogicExpression>(seq_scan_plan.filter_predicate_);
      if (!indexes.empty() && !logic_expr) {
        auto equal_expr = std::dynamic_pointer_cast<ComparisonExpression>(seq_scan_plan.filter_predicate_);
        // 需要判断是否为条件谓词

        if (equal_expr) {
          if (equal_expr) {
            auto com_type = equal_expr->comp_type_;
            // 只能是等值判断才能转化为索引扫描
            if (com_type == ComparisonType::Equal) {
              // 获取表的id
              auto table_oid = seq_scan_plan.table_oid_;
              // 返回索引扫描节点
              auto column_expr = dynamic_cast<const ColumnValueExpression &>(*equal_expr->GetChildAt(0));
              // 根据谓词的列，获取表的索引信息
              auto column_index = column_expr.GetColIdx();
              auto col_name = this->catalog_.GetTable(table_oid)->schema_.GetColumn(column_index).GetName();
              // 如果存在相关索引，获取表索引info
              for (auto *index : indexes) {
                const auto &columns = index->index_->GetKeyAttrs();
                std::vector<uint32_t> column_ids;
                column_ids.push_back(column_index);
                if (columns == column_ids) {
                  // 获取pred-key
                  auto pred_key = std::dynamic_pointer_cast<ConstantValueExpression>(equal_expr->GetChildAt(1));
                  // 从智能指针中获取裸指针
                  ConstantValueExpression *raw_pred_key = pred_key ? pred_key.get() : nullptr;
                  return std::make_shared<IndexScanPlanNode>(seq_scan_plan.output_schema_, table_oid, index->index_oid_,
                                                             seq_scan_plan.filter_predicate_, raw_pred_key);
                }
              }
            }
          }
        }
      }
    }
  }
  return optimized_plan;
}

}  // namespace bustub
