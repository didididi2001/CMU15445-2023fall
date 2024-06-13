#include "execution/execution_common.h"
#include "catalog/catalog.h"
#include "common/config.h"
#include "common/macros.h"
#include "concurrency/transaction_manager.h"
#include "fmt/core.h"
#include "storage/table/table_heap.h"
#include "type/value.h"
#include "type/value_factory.h"

namespace bustub {

auto ReconstructTuple(const Schema *schema, const Tuple &base_tuple, const TupleMeta &base_meta,
                      const std::vector<UndoLog> &undo_logs) -> std::optional<Tuple> {
  if (undo_logs.empty()) {
    auto size = schema->GetColumnCount();
    for (size_t i = 0; i < size; ++i) {
      if (!base_tuple.IsNull(schema, i)) {
        return base_tuple;
      }
    }
    return std::nullopt;
  }
  bool is_delete = false;
  auto size = schema->GetColumnCount();
  std::vector<Value> values;
  for (size_t i = 0; i < size; i++) {
    values.emplace_back(base_tuple.GetValue(schema, i));
  }
  for (const auto &undo_log : undo_logs) {
    const auto &modified_fields = undo_log.modified_fields_;
    is_delete = undo_log.is_deleted_;
    if (is_delete) {
      continue;
    }
    std::vector<uint32_t> attrs;
    for (size_t i = 0; i < size; ++i) {
      if (modified_fields[i] && !is_delete) {
        if (modified_fields[i]) {
          attrs.emplace_back(i);
        }
        auto cur_schema = Schema::CopySchema(schema, attrs);
        values[i] = undo_log.tuple_.GetValue(&cur_schema, attrs.size() - 1);
      }
    }
  }
  if (is_delete) {
    return std::nullopt;
  }
  // for (size_t i = 0; i < size; ++i) {
  //   if (!values[i].IsNull()) {
  //     return Tuple{values, schema};
  //   }
  // }
  return Tuple{values, schema};
}

void TxnMgrDbg(const std::string &info, TransactionManager *txn_mgr, const TableInfo *table_info,
               TableHeap *table_heap) {
  // always use stderr for printing logs...
  fmt::println(stderr, "debug_hook: {}", info);
  auto iter = table_heap->MakeIterator();
  while (!iter.IsEnd()) {
    auto rid = iter.GetRID();
    auto tuple = iter.GetTuple();
    fmt::println(stderr, "RID={}/{}  ts={} del_marker={} tuple={}", rid.GetPageId(), rid.GetSlotNum(), tuple.first.ts_,
                 tuple.first.is_deleted_, tuple.second.ToString(&table_info->schema_));
    auto undo_link = txn_mgr->GetUndoLink(rid);
    if (undo_link.has_value()) {
      auto undo_link_value = undo_link.value();

      while (undo_link_value.IsValid()) {
        if (txn_mgr->txn_map_.find(undo_link_value.prev_txn_) == txn_mgr->txn_map_.end()) {
          break;
        }
        auto undo_log = txn_mgr->GetUndoLog(undo_link_value);
        std::vector<uint32_t> attrs;
        std::string modified_fields_str;
        for (size_t i = 0; i < undo_log.modified_fields_.size(); ++i) {
          if (undo_log.modified_fields_[i]) {
            attrs.emplace_back(i);
            modified_fields_str += "1 ";
          } else {
            modified_fields_str += "0 ";
          }
        }
        auto cur_schema = Schema::CopySchema(&table_info->schema_, attrs);
        fmt::println(stderr, "  txn{}@{} tuple={} , is_delete={} ,modified_fields_={} ts={}", undo_link_value.prev_txn_,
                     undo_link_value.prev_log_idx_, undo_log.tuple_.ToString(&cur_schema), undo_log.is_deleted_,
                     modified_fields_str, undo_log.ts_);
        undo_link_value = undo_log.prev_version_;
      }
    }
    ++iter;
  }
  // We recommend implementing this function as traversing the table heap and print the version chain. An example output
  // of our reference solution:
  //
  // debug_hook: before verify scan
  // RID=0/0 ts=txn8 tuple=(1, <NULL>, <NULL>)
  //   txn8@0 (2, _, _) ts=1
  // RID=0/1 ts=3 tuple=(3, <NULL>, <NULL>)
  //   txn5@0 <del> ts=2
  //   txn3@0 (4, <NULL>, <NULL>) ts=1
  // RID=0/2 ts=4 <del marker> tuple=(<NULL>, <NULL>, <NULL>)
  //   txn7@0 (5, <NULL>, <NULL>) ts=3
  // RID=0/3 ts=txn6 <del marker> tuple=(<NULL>, <NULL>, <NULL>)
  //   txn6@0 (6, <NULL>, <NULL>) ts=2
  //   txn3@1 (7, _, _) ts=1
}

auto UnionUndoLog(UndoLog &old_undo_log, UndoLog &new_undo_log, Schema &base_schema) -> void {
  // 获取被修改字段的数量
  size_t modified_fields_count = old_undo_log.modified_fields_.size();

  // 定义存储属性和值的向量
  std::vector<uint32_t> combined_attrs;
  std::vector<uint32_t> old_attrs;
  std::vector<uint32_t> new_attrs;
  std::vector<Value> combined_values;

  // 遍历所有被修改的字段
  for (size_t index = 0; index < modified_fields_count; ++index) {
    // 如果新旧日志中的字段有修改，记录该属性
    if (new_undo_log.modified_fields_[index] || old_undo_log.modified_fields_[index]) {
      combined_attrs.emplace_back(index);
    }

    // 如果新日志中的字段有修改，记录新属性
    if (new_undo_log.modified_fields_[index]) {
      new_attrs.emplace_back(index);
    }

    // 如果旧日志中的字段有修改，复制旧的模式并记录旧属性的值
    if (old_undo_log.modified_fields_[index]) {
      old_attrs.emplace_back(index);
      auto old_schema = Schema::CopySchema(&base_schema, old_attrs);
      combined_values.emplace_back(old_undo_log.tuple_.GetValue(&old_schema, old_attrs.size() - 1));
    }

    // 如果新日志中的字段有修改且旧日志中的字段没有修改，复制新的模式并记录新的值
    if (new_undo_log.modified_fields_[index] && !old_undo_log.modified_fields_[index]) {
      auto new_schema = Schema::CopySchema(&base_schema, new_attrs);
      old_undo_log.modified_fields_[index] = true;
      combined_values.emplace_back(new_undo_log.tuple_.GetValue(&new_schema, new_attrs.size() - 1));
    }
  }

  // 创建当前的模式并更新旧日志的元组
  auto combined_schema = Schema::CopySchema(&base_schema, combined_attrs);
  old_undo_log.tuple_ = Tuple{combined_values, &combined_schema};
}

auto UpdateUodoLog(TupleMeta &meta, Transaction *transaction, TransactionManager *txn_mgr, Tuple &undo_tuple,
                   RID &undo_rid, std::vector<bool> &modified_fields, TableInfo *table_info) -> void {
  if (meta.ts_ != transaction->GetTransactionTempTs()) {
    UndoLink undo_link;
    auto pre_undo_link = txn_mgr->GetUndoLink(undo_rid);
    // Schema empty_schema{std::vector<Column>(0)};
    if (pre_undo_link.has_value()) {
      undo_link =
          transaction->AppendUndoLog({meta.is_deleted_, modified_fields, undo_tuple, meta.ts_, pre_undo_link.value()});
    } else {
      undo_link = transaction->AppendUndoLog({meta.is_deleted_, modified_fields, undo_tuple, meta.ts_, {}});
    }
    // std::cout << undo_link.prev_txn_ << " " << undo_link.prev_log_idx_ << std::endl;
    txn_mgr->UpdateUndoLink(undo_rid, undo_link, nullptr);
  } else {
    auto undo_link = txn_mgr->GetUndoLink(undo_rid);
    if (undo_link.has_value()) {
      // Schema empty_schema{std::vector<Column>(0)};
      auto &txn_id = undo_link.value().prev_txn_;
      auto &log_idx = undo_link.value().prev_log_idx_;
      if (txn_id == transaction->GetTransactionTempTs()) {
        // need union undo log
        auto old_undo_log = transaction->GetUndoLog(log_idx);
        UndoLog new_undo_log{meta.is_deleted_, modified_fields, undo_tuple, meta.ts_, {}};
        UnionUndoLog(old_undo_log, new_undo_log, table_info->schema_);
        transaction->ModifyUndoLog(log_idx, old_undo_log);
      }
    }
  }
}
auto LockRID(RID rid, TransactionManager *txn_mgr) -> bool {
  // 获取 RID 对应的版本链接
  auto version_link = txn_mgr->GetVersionLink(rid);

  // 如果版本链接存在
  if (version_link.has_value()) {
    // 更新版本链接，设置撤销链接的前一版本为当前版本，且 in_progress 设置为 true
    return (
        txn_mgr->UpdateVersionLink(rid,
                                   VersionUndoLink{UndoLink{version_link->prev_},
                                                   true},  // 新的版本链接，撤销链接指向前一个版本，in_progress 为 true
                                   [version_link](std::optional<VersionUndoLink> origin_version_link) -> bool {
                                     // 更新的条件：原始版本链接存在，且不在进行中，且前一版本与当前版本链接的前一版本相同
                                     return origin_version_link.has_value() && !origin_version_link->in_progress_ &&
                                            origin_version_link->prev_ == version_link->prev_;
                                   }));
  }

  // 如果版本链接不存在，创建新的版本链接，撤销链接为空，in_progress 为 true
  return txn_mgr->UpdateVersionLink(
      rid, VersionUndoLink{UndoLink{}, true},  // 新的版本链接，撤销链接为空，in_progress 为 true
      [](std::optional<VersionUndoLink> version_link) -> bool {
        // 更新的条件：原始版本链接不存在
        return !version_link.has_value();
      });
}

}  // namespace bustub
