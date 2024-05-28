//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.cpp
//
// Identification: src/execution/seq_scan_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/seq_scan_executor.h"

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan) {}

void SeqScanExecutor::Init() {
  auto catalog = exec_ctx_->GetCatalog();
  auto table_oid = plan_->GetTableOid();
  auto table_info = catalog->GetTable(table_oid);
  if (table_info == Catalog::NULL_TABLE_INFO) {
    std::cout << "can't find plan's table_info" << std::endl;
  }
  table_iterator_ = std::make_unique<TableIterator>(table_info->table_->MakeIterator());
}

auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  while (!table_iterator_->IsEnd()) {
    if (table_iterator_->GetTuple().first.is_deleted_) {
      ++(*table_iterator_);
      continue;
    }
    *tuple = table_iterator_->GetTuple().second;
    *rid = table_iterator_->GetRID();
    if (plan_->filter_predicate_ != nullptr) {
      auto value = plan_->filter_predicate_->Evaluate(tuple, GetOutputSchema());
      if (value.IsNull() || !value.GetAs<bool>()) {
        ++(*table_iterator_);
        continue;
      }
      // std::vector<Value> values{};
      // values.reserve(GetOutputSchema().GetColumnCount());
      // for(auto filter_predicate_children : plan_->filter_predicate_->children_ ){
      //   auto value = filter_predicate_children->Evaluate(tuple, GetOutputSchema());
      //   if(!value.IsNull() && value.GetAs<bool>()){
      //     values.push_back(value);
      //     std::cout << value.ToString() << std::endl;
      //   }
      // }
      // *tuple = Tuple{values, &GetOutputSchema()};
      // Tuple a = Tuple{values, &GetOutputSchema()};
      // std::cout << a.ToString(&GetOutputSchema()) << std::endl;
    }
    ++(*table_iterator_);
    return true;
  }
  tuple = nullptr;
  rid = nullptr;
  return false;
}
}  // namespace bustub
