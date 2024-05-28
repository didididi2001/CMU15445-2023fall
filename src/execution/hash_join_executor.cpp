//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_join_executor.cpp
//
// Identification: src/execution/hash_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/hash_join_executor.h"
#include "type/value_factory.h"

namespace bustub {

HashJoinExecutor::HashJoinExecutor(ExecutorContext *exec_ctx, const HashJoinPlanNode *plan,
                                   std::unique_ptr<AbstractExecutor> &&left_child,
                                   std::unique_ptr<AbstractExecutor> &&right_child)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_child_(std::move(left_child)),
      right_child_(std::move(right_child)),
      left_schema_(left_child_->GetOutputSchema()),
      right_schema_(right_child_->GetOutputSchema()) {
  left_exprs_ = plan_->LeftJoinKeyExpressions();
  right_exprs_ = plan_->RightJoinKeyExpressions();
  if (!(plan->GetJoinType() == JoinType::LEFT || plan->GetJoinType() == JoinType::INNER)) {
    // Note for 2023 Fall: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
}

void HashJoinExecutor::Init() {
  left_child_->Init();
  right_child_->Init();

  Tuple child_tuple;
  RID child_rid;
  while (right_child_->Next(&child_tuple, &child_rid)) {
    std::vector<Value> right_keys;
    right_keys.clear();
    for (const auto &expr : right_exprs_) {
      auto value = expr->Evaluate(&child_tuple, right_schema_);
      right_keys.emplace_back(value);
    }
    HashJoinKey key{right_keys};

    auto it = ht_.find(key);
    if (it != ht_.end()) {
      it->second.tuples_.emplace_back(child_tuple);
    } else {
      // 使用聚合初始化创建 HashJoinValue，避免创建临时对象
      ht_.emplace(std::move(key), HashJoinValue{{child_tuple}});
    }
  }

  // std::cout << ht_.size() << std::endl;

  while (left_child_->Next(&child_tuple, &child_rid)) {
    std::vector<Value> left_keys;
    left_keys.clear();
    for (const auto &expr : left_exprs_) {
      auto value = expr->Evaluate(&child_tuple, left_schema_);
      left_keys.emplace_back(value);
    }
    HashJoinKey key{left_keys};

    auto it = ht_.find(key);
    if (it != ht_.end()) {
      auto &right_tuples = it->second.tuples_;
      for (auto &right_tuple : right_tuples) {
        std::vector<Value> ans_tuple;
        ans_tuple.reserve(GetOutputSchema().GetColumnCount());

        int left_schema_size = left_schema_.GetColumnCount();
        for (int i = 0; i < left_schema_size; i++) {
          ans_tuple.push_back(child_tuple.GetValue(&left_schema_, i));
        }

        int right_schema_size = right_schema_.GetColumnCount();
        for (int i = 0; i < right_schema_size; i++) {
          ans_tuple.push_back(right_tuple.GetValue(&right_schema_, i));
        }

        ans_tuples_.emplace_back(Tuple{ans_tuple, &GetOutputSchema()});
      }
    } else if (plan_->GetJoinType() == JoinType::LEFT) {
      std::vector<Value> ans_tuple;
      ans_tuple.reserve(GetOutputSchema().GetColumnCount());

      int left_schema_size = left_schema_.GetColumnCount();
      for (int i = 0; i < left_schema_size; i++) {
        ans_tuple.push_back(child_tuple.GetValue(&left_schema_, i));
      }

      int right_schema_size = right_schema_.GetColumnCount();
      for (int i = 0; i < right_schema_size; i++) {
        ans_tuple.push_back(ValueFactory::GetNullValueByType(right_schema_.GetColumn(i).GetType()));
      }

      ans_tuples_.emplace_back(Tuple{ans_tuple, &GetOutputSchema()});
    }
  }
  ht_.clear();
}

auto HashJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  // std::cout << ans_index_ << std::endl;
  if (ans_index_ < static_cast<int>(ans_tuples_.size())) {
    *tuple = ans_tuples_[ans_index_++];
    return true;
  }
  return false;
}

}  // namespace bustub
