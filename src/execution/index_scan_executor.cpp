//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// index_scan_executor.cpp
//
// Identification: src/execution/index_scan_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include "execution/executors/index_scan_executor.h"

namespace bustub {
IndexScanExecutor::IndexScanExecutor(ExecutorContext *exec_ctx, const IndexScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan) {}

void IndexScanExecutor::Init() {
  auto catalog = exec_ctx_->GetCatalog();
  auto index_oid = plan_->GetIndexOid();
  auto index_info = catalog->GetIndex(index_oid);
  if (index_info == Catalog::NULL_INDEX_INFO) {
    std::cout << "can't find plan's index_info" << std::endl;
  }
  auto keys_schema = index_info->index_->GetKeySchema();
  auto key_attrs = index_info->index_->GetKeyAttrs();
  std::vector<Value> values;
  values.reserve(key_attrs.size());
  int key_attrs_size = key_attrs.size();
  for (int i = 0; i < key_attrs_size; ++i) {
    values.emplace_back(plan_->pred_key_->val_);
  }
  auto key = Tuple{values, keys_schema};
  index_info->index_->ScanKey(key, &out_rid_, exec_ctx_->GetTransaction());
  for (const auto &id : out_rid_) {
    auto tuple_meta = catalog->GetTable(plan_->table_oid_)->table_->GetTuple(id);
    if (!tuple_meta.first.is_deleted_) {
      out_tuple_.push_back(tuple_meta.second);
      out_rid_.push_back(id);
    }
  }
}

auto IndexScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (!is_done_ && !out_tuple_.empty()) {
    int out_size = out_tuple_.size();
    for (int i = 0; i < out_size; ++i) {
      *tuple = out_tuple_[i];
      *rid = out_rid_[i];
    }
    is_done_ = true;
    return true;
  }
  return false;
}

}  // namespace bustub
