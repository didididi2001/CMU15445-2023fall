//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// aggregation_executor.cpp
//
// Identification: src/execution/aggregation_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>
#include <vector>

#include "execution/executors/aggregation_executor.h"

namespace bustub {

AggregationExecutor::AggregationExecutor(ExecutorContext *exec_ctx, const AggregationPlanNode *plan,
                                         std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      child_executor_(std::move(child_executor)),
      aht_(plan->aggregates_, plan->agg_types_),
      aht_iterator_(aht_.Begin()) {}

void AggregationExecutor::Init() {
  child_executor_->Init();
  is_first_ = true;
  Tuple child_tuple;
  RID child_rid;
  while (child_executor_->Next(&child_tuple, &child_rid)) {
    auto key = MakeAggregateKey(&child_tuple);
    auto value = MakeAggregateValue(&child_tuple);
    aht_.InsertCombine(key, value);
    // std::cout << " aaa " << std::endl;
  }
  aht_iterator_ = aht_.Begin();
}

auto AggregationExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (is_first_) {
    if (aht_iterator_ != aht_.End()) {
      std::vector<Value> values;
      auto key = aht_iterator_.Key();
      auto value = aht_iterator_.Val();
      values.reserve(key.group_bys_.size() + value.aggregates_.size());
      values.insert(values.end(), key.group_bys_.begin(), key.group_bys_.end());
      values.insert(values.end(), value.aggregates_.begin(), value.aggregates_.end());
      *tuple = {values, &GetOutputSchema()};
      ++aht_iterator_;
    } else {
      if (!plan_->group_bys_.empty()) {
        return false;
      }
      // std::cout << " plan_->group_bys_.size(): " << plan_->group_bys_.size() << std::endl;
      // std::cout << GetOutputSchema().ToString() << std::endl;
      *tuple = {aht_.GenerateInitialAggregateValue().aggregates_, &GetOutputSchema()};
    }
    is_first_ = false;
    return true;
  }
  if (aht_iterator_ != aht_.End()) {
    std::vector<Value> values;
    auto key = aht_iterator_.Key();
    auto value = aht_iterator_.Val();
    values.reserve(key.group_bys_.size() + value.aggregates_.size());
    values.insert(values.end(), key.group_bys_.begin(), key.group_bys_.end());
    values.insert(values.end(), value.aggregates_.begin(), value.aggregates_.end());
    *tuple = {values, &GetOutputSchema()};
    std::cout << tuple->ToString(&GetOutputSchema()) << std::endl;
    // std::cout << "bbb" << std::endl;
    ++aht_iterator_;
    return true;
  }

  return false;
}

auto AggregationExecutor::GetChildExecutor() const -> const AbstractExecutor * { return child_executor_.get(); }

}  // namespace bustub
