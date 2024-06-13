//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// update_executor.cpp
//
// Identification: src/execution/update_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>

#include "concurrency/transaction_manager.h"
#include "execution/execution_common.h"
#include "execution/executors/update_executor.h"

namespace bustub {

UpdateExecutor::UpdateExecutor(ExecutorContext *exec_ctx, const UpdatePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {
  // As of Fall 2022, you DON'T need to implement update executor to have perfect score in project 3 / project 4.
}

void UpdateExecutor::Init() { child_executor_->Init(); }

auto UpdateExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (!is_done_) {
    auto catalog = exec_ctx_->GetCatalog();
    auto table_oid = plan_->GetTableOid();
    auto table_info = catalog->GetTable(table_oid);
    auto table = table_info->table_.get();
    Transaction *transaction = exec_ctx_->GetTransaction();
    TransactionManager *txn_mgr = exec_ctx_->GetTransactionManager();
    auto indexes_info = catalog->GetTableIndexes(table_info->name_);
    Tuple update_tuple;
    RID update_rid;
    int update_num = 0;
    std::vector<Tuple> insert_tuples;
    while (child_executor_->Next(&update_tuple, &update_rid)) {
      // std::cout << update_rid.ToString() << std::endl;
      auto tuple_meta = table->GetTuple(update_rid);
      auto &meta = tuple_meta.first;
      auto &old_tuple = tuple_meta.second;
      if (meta.ts_ > transaction->GetReadTs() && meta.ts_ != transaction->GetTransactionTempTs()) {
        // update fail
        exec_ctx_->GetTransaction()->SetTainted();
        throw ExecutionException("update execution fail");
        return false;
      }
      // update tuple
      std::vector<Value> values{};
      std::vector<bool> modified_fields;
      std::vector<uint32_t> attrs;
      std::vector<Value> undo_values;
      size_t expr_size = plan_->target_expressions_.size();
      for (size_t i = 0; i < expr_size; ++i) {
        const auto &expr = plan_->target_expressions_[i];
        // std::cout << expr->ToString() << std::endl;
        values.push_back(expr->Evaluate(&update_tuple, child_executor_->GetOutputSchema()));
        auto old_value = old_tuple.GetValue(&table_info->schema_, i);
        if (!values[i].CompareExactlyEquals(old_value)) {
          modified_fields.emplace_back(true);
          attrs.emplace_back(i);
          undo_values.emplace_back(old_value);
        } else {
          modified_fields.emplace_back(false);
        }
      }
      auto new_tuple = Tuple{values, &table_info->schema_};
      if (indexes_info.empty()) {
        auto cur_schema = Schema::CopySchema(&table_info->schema_, attrs);
        Tuple undo_log_tuple{undo_values, &cur_schema};
        UpdateUodoLog(meta, transaction, txn_mgr, undo_log_tuple, update_rid, modified_fields, table_info);
        meta.ts_ = transaction->GetTransactionTempTs();
        table_info->table_->UpdateTupleMeta(meta, update_rid);
        table->UpdateTupleInPlace(meta, new_tuple, update_rid, nullptr);
        transaction->AppendWriteSet(table_oid, update_rid);
        update_num++;
      } else {
        // 改为删除和插入
        std::vector<bool> modified_fields(table_info->schema_.GetColumnCount(), true);
        UpdateUodoLog(meta, transaction, txn_mgr, update_tuple, update_rid, modified_fields, table_info);
        meta.ts_ = transaction->GetTransactionTempTs();
        meta.is_deleted_ = true;
        table_info->table_->UpdateTupleMeta(meta, update_rid);
        transaction->AppendWriteSet(table_oid, update_rid);
        insert_tuples.emplace_back(new_tuple);
        update_num++;
      }
    }
    // insert
    for (auto &insert_tuple : insert_tuples) {
      for (const auto &index : indexes_info) {
        auto key = insert_tuple.KeyFromTuple(table_info->schema_, index->key_schema_, index->index_->GetKeyAttrs());
        std::vector<RID> find_rids;
        index->index_->ScanKey(key, &find_rids, nullptr);
        if (!find_rids.empty()) {  // 如果找到重复键
          auto &find_rid = find_rids[0];
          std::cout << find_rid.ToString() << std::endl;
          auto tuple_meta = table->GetTuple(find_rid);
          auto &meta = tuple_meta.first;
          if ((meta.ts_ > transaction->GetReadTs() && meta.ts_ != transaction->GetTransactionTempTs()) ||
              (index->is_primary_key_ && !meta.is_deleted_)) {
            transaction->SetTainted();  // 设置事务状态为TAINTED
            throw ExecutionException("Insert execution failed, the index already exists");  // 抛出异常
            return false;
          }
          if ((meta.ts_ <= transaction->GetReadTs() || meta.ts_ == transaction->GetTransactionTempTs()) &&
              meta.is_deleted_) {
            // 执行更新逻辑
            UndoLink undo_link;
            auto pre_undo_link = txn_mgr->GetUndoLink(find_rid);
            if (pre_undo_link.has_value()) {
              undo_link = transaction->AppendUndoLog({true, {}, insert_tuple, meta.ts_, pre_undo_link.value()});
            }
            txn_mgr->UpdateUndoLink(find_rid, undo_link, nullptr);
            meta.ts_ = transaction->GetTransactionTempTs();
            meta.is_deleted_ = false;
            table->UpdateTupleMeta(meta, find_rid);
            table->UpdateTupleInPlace(meta, insert_tuple, find_rid, nullptr);
            transaction->AppendWriteSet(table_oid, find_rid);
          }
        } else {
          auto new_rid = table_info->table_->InsertTuple({transaction->GetTransactionTempTs(), false}, insert_tuple);
          if (new_rid.has_value()) {
            index->index_->InsertEntry(key, new_rid.value(), transaction);
          }
          transaction->AppendWriteSet(table_oid, new_rid.value());
          // 更新版本链接
          txn_mgr->UpdateVersionLink(new_rid.value(), std::nullopt, nullptr);
        }
      }
    }
    *tuple = Tuple{{Value{TypeId::INTEGER, update_num}}, &GetOutputSchema()};
    is_done_ = true;
    return true;
  }
  return false;
}

}  // namespace bustub
