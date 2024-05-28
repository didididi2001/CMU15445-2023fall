#include "execution/executors/sort_executor.h"

namespace bustub {

SortExecutor::SortExecutor(ExecutorContext *exec_ctx, const SortPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void SortExecutor::Init() {
  child_executor_->Init();

  auto order_bys = plan_->GetOrderBy();
  auto ouputschema = GetOutputSchema();
  for (const auto &order_by : order_bys) {
    std::cout << order_by.second->ToString() << std::endl;
  }

  Tuple child_tuple;
  RID child_rid;

  while (child_executor_->Next(&child_tuple, &child_rid)) {
    ans_tuples_.emplace_back(child_tuple);
  }

  std::sort(ans_tuples_.begin(), ans_tuples_.end(), [&order_bys, &ouputschema](const Tuple &a, const Tuple &b) {
    for (const auto &order_by : order_bys) {
      auto order_type = order_by.first;
      const auto &expr = order_by.second;
      Value val_a = expr->Evaluate(&a, ouputschema);
      Value val_b = expr->Evaluate(&b, ouputschema);

      if (val_a.CompareEquals(val_b) == CmpBool::CmpTrue) {
        continue;
      }

      if (order_type == OrderByType::ASC || order_type == OrderByType::DEFAULT) {
        return val_a.CompareLessThan(val_b) == CmpBool::CmpTrue;
      }
      return val_a.CompareGreaterThan(val_b) == CmpBool::CmpTrue;
    }
    return false;
  });
}

auto SortExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (ans_index_ < static_cast<int>(ans_tuples_.size())) {
    *tuple = ans_tuples_[ans_index_++];
    return true;
  }
  return false;
}

}  // namespace bustub
