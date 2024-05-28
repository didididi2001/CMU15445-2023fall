#include "execution/executors/topn_executor.h"

namespace bustub {

TopNExecutor::TopNExecutor(ExecutorContext *exec_ctx, const TopNPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      child_executor_(std::move(child_executor)),
      topn_(plan_->GetN(), CustomComparator{plan_->GetOrderBy(), *plan_->output_schema_}) {}

void TopNExecutor::Init() {
  std::cout << "TopNExecutor::Init()" << std::endl;
  child_executor_->Init();
  Tuple child_tuple;
  RID child_rid;

  while (child_executor_->Next(&child_tuple, &child_rid)) {
    topn_.Add(child_tuple);
  }

  ans_tuples_ = topn_.GetTopN();
  std::cout << ans_tuples_.size() << std::endl;
}

auto TopNExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (ans_index_ < ans_tuples_.size()) {
    *tuple = ans_tuples_[ans_index_++];
    return true;
  }
  return false;
}

auto TopNExecutor::GetNumInHeap() -> size_t { return ans_tuples_.size(); };

}  // namespace bustub
