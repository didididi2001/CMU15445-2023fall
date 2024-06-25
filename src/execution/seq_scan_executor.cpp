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
#include "concurrency/transaction_manager.h"
#include "execution/execution_common.h"

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

// auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
//   while (!table_iterator_->IsEnd()) {
//     if (table_iterator_->GetTuple().first.is_deleted_) {
//       ++(*table_iterator_);
//       continue;
//     }
//     *tuple = table_iterator_->GetTuple().second;
//     *rid = table_iterator_->GetRID();
//     if (plan_->filter_predicate_ != nullptr) {
//       auto value = plan_->filter_predicate_->Evaluate(tuple, GetOutputSchema());
//       if (value.IsNull() || !value.GetAs<bool>()) {
//         ++(*table_iterator_);
//         continue;
//       }
//     }
//     ++(*table_iterator_);
//     return true;
//   }
//   tuple = nullptr;
//   rid = nullptr;
//   return false;
// }
auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  TransactionManager *txn_mgr = exec_ctx_->GetTransactionManager();
  while (!table_iterator_->IsEnd()) {
    // std::cout << "11111" << std::endl;
    auto tuplemeta = table_iterator_->GetTuple().first;
    *tuple = table_iterator_->GetTuple().second;
    *rid = table_iterator_->GetRID();
    if (tuplemeta.ts_ == exec_ctx_->GetTransaction()->GetTransactionTempTs() ||
        (tuplemeta.ts_ <= exec_ctx_->GetTransaction()->GetReadTs())) {
      if (tuplemeta.is_deleted_) {
        ++(*table_iterator_);
        continue;
      }
      // 如果存在过滤条件，进行评估
      if (plan_->filter_predicate_ != nullptr) {
        auto value = plan_->filter_predicate_->Evaluate(tuple, GetOutputSchema());
        if (value.IsNull() || !value.GetAs<bool>()) {
          ++(*table_iterator_);
          continue;
        }
      }
      ++(*table_iterator_);
      return true;
    }

    // need reconstructTuple
    auto undo_link = txn_mgr->GetUndoLink(*rid);
    if (undo_link.has_value()) {
      std::vector<UndoLog> undo_logs;
      auto &undo_link_value = undo_link.value();
      while (undo_link_value.IsValid()) {
        auto undo_log_optional = txn_mgr->GetUndoLogOptional(undo_link_value);
        if (undo_log_optional.has_value()) {
          // std::cout << "undo_log.ts_: " << undo_log.ts_ << std::endl;
          auto undo_log = undo_log_optional.value();
          undo_logs.emplace_back(undo_log);
          if (undo_log.ts_ <= exec_ctx_->GetTransaction()->GetReadTs()) {
            if (!undo_logs.empty()) {
              auto new_tuple = ReconstructTuple(&GetOutputSchema(), *tuple, {2333, false}, undo_logs);
              if (new_tuple.has_value()) {
                *tuple = new_tuple.value();
                if (plan_->filter_predicate_ != nullptr) {
                  auto value = plan_->filter_predicate_->Evaluate(tuple, GetOutputSchema());
                  if (value.IsNull() || !value.GetAs<bool>()) {
                    break;
                  }
                }
                ++(*table_iterator_);
                return true;
              }
            }
            break;
          }
          undo_link_value = undo_log.prev_version_;
        }
      }
    }
    ++(*table_iterator_);
  }
  tuple = nullptr;
  rid = nullptr;
  return false;
}
}  // namespace bustub
