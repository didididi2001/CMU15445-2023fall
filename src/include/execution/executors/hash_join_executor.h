//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_join_executor.h
//
// Identification: src/include/execution/executors/hash_join_executor.h
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <utility>

#include "common/util/hash_util.h"
#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/plans/hash_join_plan.h"
#include "storage/table/tuple.h"

namespace bustub {

/**
 * HashJoinExecutor executes a nested-loop JOIN on two tables.
 */

struct HashJoinKey {
  // 成员变量和函数
  std::vector<Value> values_;

  auto operator==(const HashJoinKey &other) const -> bool {
    if (values_.size() != other.values_.size()) {
      return false;
    }
    for (uint32_t i = 0; i < other.values_.size(); i++) {
      if (values_[i].CompareEquals(other.values_[i]) != CmpBool::CmpTrue) {
        return false;
      }
    }
    return true;
  }
};

struct HashJoinValue {
  std::vector<Tuple> tuples_;
};

struct HashJoinKeyHash {
  auto operator()(const HashJoinKey &key) const -> std::size_t {
    std::size_t curr_hash = 0;
    for (const auto &value : key.values_) {
      if (!value.IsNull()) {
        curr_hash = HashUtil::CombineHashes(curr_hash, HashUtil::HashValue(&value));
      }
    }
    return curr_hash;
  }
};

struct HashJoinKeyEqual {
  auto operator()(const HashJoinKey &lhs, const HashJoinKey &rhs) const -> bool {
    if (lhs.values_.size() != rhs.values_.size()) {
      return false;
    }
    for (size_t i = 0; i < lhs.values_.size(); ++i) {
      if (lhs.values_[i].CompareEquals(rhs.values_[i]) != CmpBool::CmpTrue) {
        return false;
      }
    }
    return true;
  }
};

class HashJoinExecutor : public AbstractExecutor {
 public:
  /**
   * Construct a new HashJoinExecutor instance.
   * @param exec_ctx The executor context
   * @param plan The HashJoin join plan to be executed
   * @param left_child The child executor that produces tuples for the left side of join
   * @param right_child The child executor that produces tuples for the right side of join
   */
  HashJoinExecutor(ExecutorContext *exec_ctx, const HashJoinPlanNode *plan,
                   std::unique_ptr<AbstractExecutor> &&left_child, std::unique_ptr<AbstractExecutor> &&right_child);

  /** Initialize the join */
  void Init() override;

  /**
   * Yield the next tuple from the join.
   * @param[out] tuple The next tuple produced by the join.
   * @param[out] rid The next tuple RID, not used by hash join.
   * @return `true` if a tuple was produced, `false` if there are no more tuples.
   */
  auto Next(Tuple *tuple, RID *rid) -> bool override;

  /** @return The output schema for the join */
  auto GetOutputSchema() const -> const Schema & override { return plan_->OutputSchema(); };

 private:
  /** The HashJoin plan node to be executed. */
  const HashJoinPlanNode *plan_;
  std::unique_ptr<AbstractExecutor> left_child_;
  std::unique_ptr<AbstractExecutor> right_child_;
  std::vector<AbstractExpressionRef> right_exprs_;
  std::vector<AbstractExpressionRef> left_exprs_;
  // std::unordered_map<HashJoinKey, HashJoinValue> ht_;
  std::unordered_map<HashJoinKey, HashJoinValue, HashJoinKeyHash, HashJoinKeyEqual> ht_;
  std::vector<Tuple> ans_tuples_;
  Schema left_schema_;
  Schema right_schema_;
  int ans_index_{0};
};

}  // namespace bustub

// namespace std {

// template <>
// struct hash<bustub::HashJoinKey> {
//   auto operator()(const bustub::HashJoinKey &keys) const -> std::size_t {
//     size_t curr_hash = 0;
//     for (const auto &key : keys.values) {
//       if (!key.IsNull()) {
//         curr_hash = bustub::HashUtil::CombineHashes(curr_hash, bustub::HashUtil::HashValue(&key));
//       }
//     }
//     return curr_hash;
//   }
// };

// }  // namespace std