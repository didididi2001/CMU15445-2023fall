//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// transaction_manager.cpp
//
// Identification: src/concurrency/transaction_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "concurrency/transaction_manager.h"

#include <memory>
#include <mutex>  // NOLINT
#include <optional>
#include <shared_mutex>
#include <unordered_map>
#include <unordered_set>

#include "catalog/catalog.h"
#include "catalog/column.h"
#include "catalog/schema.h"
#include "common/config.h"
#include "common/exception.h"
#include "common/macros.h"
#include "concurrency/transaction.h"
#include "execution/execution_common.h"
#include "storage/table/table_heap.h"
#include "storage/table/tuple.h"
#include "type/type_id.h"
#include "type/value.h"
#include "type/value_factory.h"

namespace bustub {

auto TransactionManager::Begin(IsolationLevel isolation_level) -> Transaction * {
  std::unique_lock<std::shared_mutex> l(txn_map_mutex_);
  auto txn_id = next_txn_id_++;
  auto txn = std::make_unique<Transaction>(txn_id, isolation_level);
  auto *txn_ref = txn.get();
  txn_map_.insert(std::make_pair(txn_id, std::move(txn)));

  // TODO(fall2023): set the timestamps here. Watermark updated below.
  txn_ref->read_ts_ = last_commit_ts_.load();

  running_txns_.AddTxn(txn_ref->read_ts_);
  return txn_ref;
}

auto TransactionManager::VerifyTxn(Transaction *txn) -> bool { return true; }

auto TransactionManager::Commit(Transaction *txn) -> bool {
  std::unique_lock<std::mutex> commit_lck(commit_mutex_);

  // TODO(fall2023): acquire commit ts!

  if (txn->state_ != TransactionState::RUNNING) {
    throw Exception("txn not in running state");
  }
  txn->commit_ts_ = last_commit_ts_.load() + 1;
  if (txn->GetIsolationLevel() == IsolationLevel::SERIALIZABLE) {
    if (!VerifyTxn(txn)) {
      commit_lck.unlock();
      Abort(txn);
      return false;
    }
  }

  // TODO(fall2023): Implement the commit logic!
  auto &write_sets = txn->GetWriteSets();
  std::cout << "write_sets.size(): " << write_sets.size() << std::endl;
  for (auto &it : write_sets) {
    auto table = catalog_->GetTable(it.first);
    for (auto &rid : it.second) {
      auto tuple_meta = table->table_->GetTuple(rid);
      auto meta = tuple_meta.first;
      auto &tuple = tuple_meta.second;
      meta.ts_ = txn->commit_ts_;
      // std::cout << "meta.ts_ "<< meta.ts_ << std::endl;
      table->table_->UpdateTupleInPlace(meta, tuple, rid, nullptr);
    }
  }

  std::unique_lock<std::shared_mutex> lck(txn_map_mutex_);
  // TODO(fall2023): set commit timestamp + update last committed timestamp here.
  last_commit_ts_.fetch_add(1);
  txn->state_ = TransactionState::COMMITTED;
  running_txns_.UpdateCommitTs(txn->commit_ts_);
  running_txns_.RemoveTxn(txn->read_ts_);

  return true;
}

void TransactionManager::Abort(Transaction *txn) {
  if (txn->state_ != TransactionState::RUNNING && txn->state_ != TransactionState::TAINTED) {
    throw Exception("txn not in running / tainted state");
  }

  // TODO(fall2023): Implement the abort logic!

  std::unique_lock<std::shared_mutex> lck(txn_map_mutex_);
  txn->state_ = TransactionState::ABORTED;
  running_txns_.RemoveTxn(txn->read_ts_);
}

void TransactionManager::GarbageCollection() {
  // 用于记录有效的撤销日志的哈希表
  std::unordered_map<txn_id_t, size_t> valid_undo_logs;
  // 获取当前水位线
  auto watermark = GetWatermark();

  // 遍历所有版本信息
  for (const auto &page_id_it : version_info_) {
    const auto &prev_versions = page_id_it.second->prev_version_;

    // 遍历每个版本的前版本列表
    for (const auto &prev_version : prev_versions) {
      auto undo_link = prev_version.second.prev_;
      txn_id_t pre_txn_id = -1;  // 用于记录前一个事务ID
      bool is_valid = false;     // 用于标记是否找到有效的撤销日志

      // 遍历撤销日志链
      while (undo_link.IsValid()) {
        auto txn_id = undo_link.prev_txn_;
        auto txn_it = txn_map_.find(txn_id);

        // 如果事务ID不存在于事务映射表中，则退出循环
        if (txn_it == txn_map_.end()) {
          break;
        }

        // 如果有前一个事务ID，记录它的撤销日志次数
        if (pre_txn_id != -1) {
          valid_undo_logs[pre_txn_id]++;
        }

        // 检查事务是否提交且提交时间不大于当前水位线
        if (txn_it->second->GetTransactionState() == TransactionState::COMMITTED &&
            txn_it->second->GetCommitTs() <= watermark) {
          is_valid = true;  // 找到有效的撤销日志
          break;
        }

        // 更新前一个事务ID
        pre_txn_id = txn_id;
        // 获取当前撤销日志的前一个版本
        auto undo_log = GetUndoLog(undo_link);
        undo_link = undo_log.prev_version_;
      }

      // 如果未找到有效的撤销日志且前一个事务ID有效，记录它的撤销日志次数
      if (!is_valid && pre_txn_id != -1) {
        valid_undo_logs[pre_txn_id]++;
      }
    }
  }

  std::vector<txn_id_t> delete_txn_ids;

  // 找到可以删除的事务ID
  for (const auto &it : txn_map_) {
    if (valid_undo_logs.find(it.first) == valid_undo_logs.end() &&
        it.second->GetTransactionState() == TransactionState::COMMITTED) {
      delete_txn_ids.emplace_back(it.first);
    }
  }

  // 从事务映射表中删除无效的事务记录
  for (const auto &delete_txn_id : delete_txn_ids) {
    txn_map_.erase(delete_txn_id);
  }
}

}  // namespace bustub
