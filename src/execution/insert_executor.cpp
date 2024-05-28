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

#include "execution/executors/insert_executor.h"

namespace bustub {

InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void InsertExecutor::Init() { child_executor_->Init(); }

auto InsertExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (!is_done_) {
    auto catalog = exec_ctx_->GetCatalog();
    auto table_oid = plan_->GetTableOid();
    auto table_info = catalog->GetTable(table_oid);
    auto indexes_info = catalog->GetTableIndexes(table_info->name_);
    Tuple insert_tuple;
    RID insert_rid;
    int insert_num = 0;
    while (child_executor_->Next(&insert_tuple, &insert_rid)) {
      // update table
      auto new_rid = table_info->table_->InsertTuple({0, false}, insert_tuple);
      // update indexes
      if (new_rid.has_value()) {
        for (auto index : indexes_info) {
          auto key = insert_tuple.KeyFromTuple(table_info->schema_, index->key_schema_, index->index_->GetKeyAttrs());
          index->index_->InsertEntry(key, new_rid.value(), exec_ctx_->GetTransaction());
        }
      }

      insert_num++;
      // std::cout << insert_num << std::endl;
    }

    *tuple = Tuple{{Value{TypeId::INTEGER, insert_num}}, &GetOutputSchema()};
    is_done_ = true;
    return true;
  }
  return false;
}

}  // namespace bustub
