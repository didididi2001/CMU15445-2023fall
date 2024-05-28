//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_loop_join_executor.cpp
//
// Identification: src/execution/nested_loop_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_loop_join_executor.h"
#include "binder/table_ref/bound_join_ref.h"
#include "common/exception.h"
#include "type/value_factory.h"

namespace bustub {

NestedLoopJoinExecutor::NestedLoopJoinExecutor(ExecutorContext *exec_ctx, const NestedLoopJoinPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&left_executor,
                                               std::unique_ptr<AbstractExecutor> &&right_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_executor_(std::move(left_executor)),
      right_executor_(std::move(right_executor)),
      left_schema_(left_executor_->GetOutputSchema()),
      right_schema_(right_executor_->GetOutputSchema()) {
  if (!(plan->GetJoinType() == JoinType::LEFT || plan->GetJoinType() == JoinType::INNER)) {
    // Note for 2023 Fall: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
}

void NestedLoopJoinExecutor::Init() {
  left_executor_->Init();
  right_executor_->Init();

  Tuple temp_tuple;
  RID temp_rid;

  // Preload all left and right tuples into memory
  while (left_executor_->Next(&temp_tuple, &temp_rid)) {
    left_tuple_.emplace_back(temp_tuple);
  }

  while (right_executor_->Next(&temp_tuple, &temp_rid)) {
    right_tuple_.emplace_back(temp_tuple);
  }

  const auto &predicate = plan_->predicate_;
  const bool is_left_join = (plan_->GetJoinType() == JoinType::LEFT);
  const bool is_inner_join = (plan_->GetJoinType() == JoinType::INNER);

  const auto &output_schema = GetOutputSchema();
  const int left_schema_size = left_schema_.GetColumnCount();
  const int right_schema_size = right_schema_.GetColumnCount();

  for (const auto &l_tuple : left_tuple_) {
    bool left_join = false;
    right_executor_->Init();
    for (const auto &r_tuple : right_tuple_) {
      if (auto result = predicate->EvaluateJoin(&l_tuple, left_schema_, &r_tuple, right_schema_);
          (is_inner_join && !result.IsNull() && result.GetAs<bool>()) || (is_left_join && result.GetAs<bool>())) {
        std::vector<Value> values;
        values.reserve(output_schema.GetColumnCount());

        for (int i = 0; i < left_schema_size; ++i) {
          values.emplace_back(l_tuple.GetValue(&left_schema_, i));
        }

        for (int i = 0; i < right_schema_size; ++i) {
          values.emplace_back(r_tuple.GetValue(&right_schema_, i));
        }

        ans_tuple_.emplace_back(Tuple{values, &output_schema});
        left_join = true;
      }
    }

    if (is_left_join && !left_join) {
      std::vector<Value> values;
      values.reserve(output_schema.GetColumnCount());

      for (int i = 0; i < left_schema_size; ++i) {
        values.emplace_back(l_tuple.GetValue(&left_schema_, i));
      }

      for (int i = 0; i < right_schema_size; ++i) {
        values.emplace_back(ValueFactory::GetNullValueByType(right_schema_.GetColumn(i).GetType()));
      }

      ans_tuple_.emplace_back(Tuple{values, &output_schema});
    }
  }
}

auto NestedLoopJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  // std::cout << ans_index_ << static_cast<int>(ans_tuple_.size()) << std::endl;
  if (ans_index_ < static_cast<int>(ans_tuple_.size())) {
    *tuple = ans_tuple_[ans_index_++];
    return true;
  }
  return false;
}

}  // namespace bustub
