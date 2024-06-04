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

  fmt::println(
      stderr,
      "You see this line of text because you have not implemented `TxnMgrDbg`. You should do this once you have "
      "finished task 2. Implementing this helper function will save you a lot of time for debugging in later tasks.");

  auto iter = table_heap->MakeIterator();
  while (!iter.IsEnd()) {
    auto rid = iter.GetRID();
    auto tuple = iter.GetTuple();
    fmt::println(stderr, "RID={}  ts=txn{} del marker = {} tuple={}", rid.ToString(), tuple.first.ts_,
                 tuple.first.is_deleted_, tuple.second.ToString(&table_info->schema_));
    auto undo_link = txn_mgr->GetUndoLink(rid);
    if (undo_link.has_value()) {
      auto undo_link_value = undo_link.value();

      while (undo_link_value.IsValid()) {
        auto undo_log = txn_mgr->GetUndoLog(undo_link_value);
        std::vector<uint32_t> attrs;
        std::string modified_fields_str = "";
        for (size_t i = 0; i < undo_log.modified_fields_.size(); ++i) {
          if (undo_log.modified_fields_[i]) {
            attrs.emplace_back(i);
            modified_fields_str += "1 ";
          } else {
            modified_fields_str += "0 ";
          }
        }
        auto cur_schema = Schema::CopySchema(&table_info->schema_, attrs);
        fmt::println(stderr, "  txn{}@{} tuple={} ,modified_fields_={} ts={}", undo_link_value.prev_txn_,
                     undo_link_value.prev_log_idx_, undo_log.tuple_.ToString(&cur_schema), modified_fields_str,
                     undo_log.ts_);
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

}  // namespace bustub
