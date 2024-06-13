//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// insert_executor.cpp
//
// Identification: src/execution/insert_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "concurrency/transaction_manager.h"
#include "execution/executors/insert_executor.h"

namespace bustub {

InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void InsertExecutor::Init() { child_executor_->Init(); }

auto InsertExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (!is_done_) {
    // 获取执行上下文中的目录、表ID、事务和事务管理器
    auto catalog = exec_ctx_->GetCatalog();
    auto table_oid = plan_->GetTableOid();
    Transaction *transaction = exec_ctx_->GetTransaction();
    TransactionManager *txn_mgr = exec_ctx_->GetTransactionManager();

    // 获取表信息和该表的所有索引信息
    auto table_info = catalog->GetTable(table_oid);
    auto indexes_info = catalog->GetTableIndexes(table_info->name_);
    auto table = table_info->table_.get();
    Tuple insert_tuple;  // 插入的元组
    RID insert_rid;      // 插入元组的记录ID
    int insert_num = 0;  // 记录插入的元组数量

    // 循环获取子执行器的下一个元组
    while (child_executor_->Next(&insert_tuple, &insert_rid)) {
      // 遍历表的所有索引
      if (indexes_info.empty()) {
        auto new_rid = table_info->table_->InsertTuple({transaction->GetTransactionTempTs(), false}, insert_tuple);
        if (new_rid.has_value()) {
          transaction->AppendWriteSet(table_oid, new_rid.value());
          // 更新版本链接
          txn_mgr->UpdateVersionLink(new_rid.value(), std::nullopt, nullptr);
          insert_num++;
        }
      } else {
        for (const auto &index : indexes_info) {
          // 从插入的元组中生成索引键
          auto key = insert_tuple.KeyFromTuple(table_info->schema_, index->key_schema_, index->index_->GetKeyAttrs());
          std::vector<RID> find_rids;  // 用于存储查找到的记录ID

          // 在索引中扫描该键，检查是否已存在
          index->index_->ScanKey(key, &find_rids, nullptr);
          if (!find_rids.empty()) {  // 如果找到重复键
            auto &find_rid = find_rids[0];
            // std::cout << find_rid.ToString() << std::endl;
            auto tuple_meta = table->GetTuple(find_rid);
            auto &meta = tuple_meta.first;
            if ((meta.ts_ > transaction->GetReadTs() && meta.ts_ != transaction->GetTransactionTempTs()) ||
                (index->is_primary_key_ && !meta.is_deleted_)) {
              is_done_ = true;            // 标记插入完成
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

              std::cout << undo_link.prev_txn_ << " " << undo_link.prev_log_idx_ << std::endl;
              txn_mgr->UpdateUndoLink(find_rid, undo_link, nullptr);
              meta.ts_ = transaction->GetTransactionTempTs();
              meta.is_deleted_ = false;
              table->UpdateTupleMeta(meta, find_rid);
              table->UpdateTupleInPlace(meta, insert_tuple, find_rid, nullptr);
              transaction->AppendWriteSet(table_oid, find_rid);
              insert_num++;
            }
            // is_done_ = true;            // 标记插入完成
            // transaction->SetTainted();  // 设置事务状态为TAINTED
            // throw ExecutionException("Insert execution failed, the index already exists");  // 抛出异常
            // return false;  // 返回false表示插入失败
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
    }

    // 设置输出元组，表示插入的数量
    *tuple = Tuple{{Value{TypeId::INTEGER, insert_num}}, &GetOutputSchema()};
    is_done_ = true;  // 标记插入完成
    return true;      // 返回true表示插入成功
  }
  return false;  // 如果已经插入完成，直接返回false
}

}  // namespace bustub
