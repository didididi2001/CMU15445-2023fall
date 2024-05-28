//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// topn_executor.h
//
// Identification: src/include/execution/executors/topn_executor.h
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <utility>
#include <vector>

#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/plans/seq_scan_plan.h"
#include "execution/plans/topn_plan.h"
#include "storage/table/tuple.h"

namespace bustub {

/**
 * The TopNExecutor executor executes a topn.
 */

class CustomComparator {
 public:
  CustomComparator(std::vector<std::pair<OrderByType, AbstractExpressionRef>> order_bys, Schema output_schema)
      : order_bys_(std::move(order_bys)), output_schema_(std::move(output_schema)) {}

  auto operator()(const Tuple &a, const Tuple &b) const -> bool {
    for (const auto &order_by : order_bys_) {
      auto order_type = order_by.first;
      const auto &expr = order_by.second;
      Value val_a = expr->Evaluate(&a, output_schema_);
      Value val_b = expr->Evaluate(&b, output_schema_);

      if (val_a.CompareEquals(val_b) == CmpBool::CmpTrue) {
        continue;
      }

      if (order_type == OrderByType::ASC || order_type == OrderByType::DEFAULT) {
        return val_a.CompareLessThan(val_b) == CmpBool::CmpTrue;
      }
      return val_a.CompareGreaterThan(val_b) == CmpBool::CmpTrue;
    }
    return false;
  }

 private:
  std::vector<std::pair<OrderByType, AbstractExpressionRef>> order_bys_;
  Schema output_schema_;
};

class TopN {
 public:
  explicit TopN(size_t n, const CustomComparator &comp) : n_(n), min_heap_(comp), comp_(comp) {}

  void Add(const Tuple &value) {
    if (min_heap_.size() < n_) {
      min_heap_.push(value);
    } else if (comp_(value, min_heap_.top())) {
      min_heap_.pop();
      min_heap_.push(value);
    }
  }

  auto GetTopN() const -> std::vector<Tuple> {
    std::vector<Tuple> result;
    auto copy = min_heap_;  // Make a copy of the heap
    while (!copy.empty()) {
      result.push_back(copy.top());
      copy.pop();
    }
    std::reverse(result.begin(), result.end());  // Reverse the order to get the correct top-n order
    return result;
  }

 private:
  size_t n_;
  std::priority_queue<Tuple, std::vector<Tuple>, CustomComparator> min_heap_;
  CustomComparator comp_;
};

class TopNExecutor : public AbstractExecutor {
 public:
  /**
   * Construct a new TopNExecutor instance.
   * @param exec_ctx The executor context
   * @param plan The TopN plan to be executed
   */
  TopNExecutor(ExecutorContext *exec_ctx, const TopNPlanNode *plan, std::unique_ptr<AbstractExecutor> &&child_executor);

  /** Initialize the TopN */
  void Init() override;

  /**
   * Yield the next tuple from the TopN.
   * @param[out] tuple The next tuple produced by the TopN
   * @param[out] rid The next tuple RID produced by the TopN
   * @return `true` if a tuple was produced, `false` if there are no more tuples
   */
  auto Next(Tuple *tuple, RID *rid) -> bool override;

  /** @return The output schema for the TopN */
  auto GetOutputSchema() const -> const Schema & override { return plan_->OutputSchema(); }

  /** Sets new child executor (for testing only) */
  void SetChildExecutor(std::unique_ptr<AbstractExecutor> &&child_executor) {
    child_executor_ = std::move(child_executor);
  }

  /** @return The size of top_entries_ container, which will be called on each child_executor->Next(). */
  auto GetNumInHeap() -> size_t;

 private:
  /** The TopN plan node to be executed */
  const TopNPlanNode *plan_;
  /** The child executor from which tuples are obtained */
  std::unique_ptr<AbstractExecutor> child_executor_;

  std::vector<Tuple> ans_tuples_;
  size_t ans_index_{0};
  TopN topn_;
};
}  // namespace bustub
