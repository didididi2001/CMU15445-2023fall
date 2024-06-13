//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// delete_executor.cpp
//
// Identification: src/execution/delete_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "concurrency/transaction_manager.h"
#include "execution/execution_common.h"
#include "execution/executors/delete_executor.h"

namespace bustub {

DeleteExecutor::DeleteExecutor(ExecutorContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void DeleteExecutor::Init() { child_executor_->Init(); }

auto DeleteExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  // if (!is_done_) {
  //   auto catalog = exec_ctx_->GetCatalog();
  //   auto table_oid = plan_->GetTableOid();
  //   auto table_info = catalog->GetTable(table_oid);
  //   auto indexes_info = catalog->GetTableIndexes(table_info->name_);
  //   Tuple delete_tuple;
  //   RID delete_rid;
  //   int delete_num = 0;
  //   while (child_executor_->Next(&delete_tuple, &delete_rid)) {
  //     // update table;
  //     table_info->table_->UpdateTupleMeta({0, true}, delete_rid);

  //     // update indexes
  //     for (auto index : indexes_info) {
  //       auto key = delete_tuple.KeyFromTuple(table_info->schema_, index->key_schema_, index->index_->GetKeyAttrs());
  //       index->index_->DeleteEntry(key, delete_rid, exec_ctx_->GetTransaction());
  //     }
  //     delete_num++;
  //   }
  //   *tuple = Tuple{{Value{TypeId::INTEGER, delete_num}}, &GetOutputSchema()};
  //   is_done_ = true;
  //   return true;
  // }
  // return false;

  if (!is_done_) {
    auto catalog = exec_ctx_->GetCatalog();
    auto table_oid = plan_->GetTableOid();
    auto table_info = catalog->GetTable(table_oid);
    auto table = table_info->table_.get();
    auto indexes_info = catalog->GetTableIndexes(table_info->name_);
    Transaction *transaction = exec_ctx_->GetTransaction();
    TransactionManager *txn_mgr = exec_ctx_->GetTransactionManager();
    Tuple delete_tuple;
    RID delete_rid;
    int delete_num = 0;
    while (child_executor_->Next(&delete_tuple, &delete_rid)) {
      // update table;
      // std::cout << delete_tuple.ToString(&table_info->schema_) << std::endl;
      auto tuple_meta = table->GetTuple(delete_rid);
      auto &meta = tuple_meta.first;
      // auto &old_tuple = tuple_meta.second;
      if (meta.ts_ > transaction->GetReadTs() && meta.ts_ != transaction->GetTransactionTempTs()) {
        // delete fail
        exec_ctx_->GetTransaction()->SetTainted();
        throw ExecutionException("update execution fail");
        return false;
      }
      std::vector<bool> modified_fields(table_info->schema_.GetColumnCount(), true);
      UpdateUodoLog(meta, transaction, txn_mgr, delete_tuple, delete_rid, modified_fields, table_info);
      meta.ts_ = transaction->GetTransactionTempTs();
      meta.is_deleted_ = true;
      table_info->table_->UpdateTupleMeta(meta, delete_rid);
      transaction->AppendWriteSet(table_oid, delete_rid);
      delete_num++;
    }
    *tuple = Tuple{{Value{TypeId::INTEGER, delete_num}}, &GetOutputSchema()};
    is_done_ = true;
    return true;
  }
  return false;
}

}  // namespace bustub
