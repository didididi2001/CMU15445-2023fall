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
    auto indexes_info = catalog->GetTableIndexes(table_info->name_);
    Tuple update_tuple;
    RID update_rid;
    int update_num = 0;
    while (child_executor_->Next(&update_tuple, &update_rid)) {
      // update table;
      // std::cout << update_tuple.ToString(&table_info->schema_) << std::endl;
      table_info->table_->UpdateTupleMeta({0, true}, update_rid);
      // update tuple
      std::vector<Value> values{};
      for (const auto &expr : plan_->target_expressions_) {
        // std::cout << expr->ToString() << std::endl;
        values.push_back(expr->Evaluate(&update_tuple, child_executor_->GetOutputSchema()));
      }
      auto new_tuple = Tuple{values, &table_info->schema_};
      auto new_rid = table_info->table_->InsertTuple({0, false}, new_tuple);
      auto old_tuple = table_info->table_->GetTuple(update_rid).second;
      // update indexes
      std::cout << old_tuple.ToString(&table_info->schema_) << "   " << update_tuple.ToString(&table_info->schema_)
                << std::endl;

      for (auto index : indexes_info) {
        auto key = old_tuple.KeyFromTuple(table_info->schema_, index->key_schema_, index->index_->GetKeyAttrs());
        auto new_key = new_tuple.KeyFromTuple(table_info->schema_, index->key_schema_, index->index_->GetKeyAttrs());
        index->index_->DeleteEntry(key, update_rid, exec_ctx_->GetTransaction());
        if (new_rid.has_value()) {
          index->index_->InsertEntry(new_key, new_rid.value(), exec_ctx_->GetTransaction());
          // index->index_->InsertEntry({values,&index->key_schema_}, new_rid.value(), exec_ctx_->GetTransaction());
        }
      }
      update_num++;
    }
    *tuple = Tuple{{Value{TypeId::INTEGER, update_num}}, &GetOutputSchema()};
    is_done_ = true;
    return true;
  }
  return false;
}

}  // namespace bustub
