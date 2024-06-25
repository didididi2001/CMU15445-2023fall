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
#include "concurrency/transaction_manager.h"
#include "execution/execution_common.h"

namespace bustub {
IndexScanExecutor::IndexScanExecutor(ExecutorContext *exec_ctx, const IndexScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan) {}

void IndexScanExecutor::Init() {
  // std::cout << "index scan" << std::endl;
  auto catalog = exec_ctx_->GetCatalog();
  auto index_oid = plan_->GetIndexOid();
  auto index_info = catalog->GetIndex(index_oid);
  TransactionManager *txn_mgr = exec_ctx_->GetTransactionManager();
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
    auto &meta = tuple_meta.first;
    auto &tuple = tuple_meta.second;
    // std::cout << tuple.ToString(&GetOutputSchema()) << std::endl;
    if (meta.ts_ == exec_ctx_->GetTransaction()->GetTransactionTempTs() ||
        (meta.ts_ <= exec_ctx_->GetTransaction()->GetReadTs())) {
      if (meta.is_deleted_) {
        continue;
      }
      // 如果存在过滤条件，进行评估
      if (plan_->filter_predicate_ != nullptr) {
        auto value = plan_->filter_predicate_->Evaluate(&tuple, GetOutputSchema());
        if (value.IsNull() || !value.GetAs<bool>()) {
          continue;
        }
      }
      out_tuple_.push_back(tuple_meta.second);
      out_rid_.push_back(id);
      continue;
    }
    auto undo_link = txn_mgr->GetUndoLink(id);
    if (undo_link.has_value()) {
      std::vector<UndoLog> undo_logs;
      auto &undo_link_value = undo_link.value();
      while (undo_link_value.IsValid()) {
        auto undo_log_optional = txn_mgr->GetUndoLogOptional(undo_link_value);
        // std::cout << "undo_log.ts_: " << undo_log.ts_ << std::endl;
        if (undo_log_optional.has_value()) {
          auto undo_log = undo_log_optional.value();
          undo_logs.emplace_back(undo_log);
          if (undo_log.ts_ <= exec_ctx_->GetTransaction()->GetReadTs()) {
            if (!undo_logs.empty()) {
              auto new_tuple = ReconstructTuple(&GetOutputSchema(), tuple, {2333, false}, undo_logs);
              if (new_tuple.has_value()) {
                if (plan_->filter_predicate_ != nullptr) {
                  auto value = plan_->filter_predicate_->Evaluate(&new_tuple.value(), GetOutputSchema());
                  if (value.IsNull() || !value.GetAs<bool>()) {
                    break;
                  }
                }
                out_tuple_.push_back(new_tuple.value());
                out_rid_.push_back(id);
                break;
              }
            }
            break;
          }
          undo_link_value = undo_log.prev_version_;
        }
      }
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
