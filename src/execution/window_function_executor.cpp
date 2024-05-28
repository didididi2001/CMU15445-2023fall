#include "execution/executors/window_function_executor.h"
#include "execution/expressions/column_value_expression.h"
#include "execution/plans/window_plan.h"
#include "storage/table/tuple.h"
#include "type/value_factory.h"

namespace bustub {

WindowFunctionExecutor::WindowFunctionExecutor(ExecutorContext *exec_ctx, const WindowFunctionPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void WindowFunctionExecutor::Init() {
  child_executor_->Init();

  auto &window_functions = plan_->window_functions_;
  auto &columns = plan_->columns_;
  auto columns_size = columns.size();

  Tuple child_tuple;
  RID child_rid;

  std::vector<Tuple> tuples;
  std::vector<std::vector<Value>> temp_tuples;
  while (child_executor_->Next(&child_tuple, &child_rid)) {
    tuples.emplace_back(child_tuple);
  }

  // sort all tuples
  auto window_functions_iter = plan_->window_functions_.begin();
  if (window_functions_iter != plan_->window_functions_.end()) {
    auto order_bys = window_functions_iter->second.order_by_;
    auto ouputschema = GetOutputSchema();
    for (const auto &order_by : order_bys) {
      std::cout << order_by.second->ToString() << std::endl;
    }

    std::sort(tuples.begin(), tuples.end(), [&order_bys, &ouputschema](const Tuple &a, const Tuple &b) {
      for (const auto &order_by : order_bys) {
        auto order_type = order_by.first;
        const auto &expr = order_by.second;
        Value val_a = expr->Evaluate(&a, ouputschema);
        Value val_b = expr->Evaluate(&b, ouputschema);

        if (val_a.CompareEquals(val_b) == CmpBool::CmpTrue) {
          continue;
        }

        if (order_type == OrderByType::ASC || order_type == OrderByType::DEFAULT) {
          return val_a.CompareLessThan(val_b) == CmpBool::CmpTrue;
        }
        return val_a.CompareGreaterThan(val_b) == CmpBool::CmpTrue;
      }
      return false;
    });
  }

  std::cout << plan_->ToString() << std::endl;
  std::cout << GetOutputSchema().ToString() << std::endl;
  // Generate the initial value for each partition
  std::vector<std::unordered_map<ValuesKey, Value, ValuesHash, ValuesKeyEqual>> partitions_ht(columns_size);

  // rank count  : all count
  std::vector<std::unordered_map<ValuesKey, size_t, ValuesHash, ValuesKeyEqual>> ranks_ht(columns_size);

  size_t tuples_size = tuples.size();
  for (size_t i = 0; i < tuples_size; ++i) {
    auto &cur_tuple = tuples[i];
    std::vector<Value> values;
    values.reserve(GetOutputSchema().GetColumnCount());
    for (size_t j = 0; j < columns_size; ++j) {
      // get_colume_idx
      const auto &col_val = std::dynamic_pointer_cast<const ColumnValueExpression>(columns[j]);
      auto column_idx = col_val->GetColIdx();
      // std::cout << " column_idx: " << column_idx << std::endl;
      if (column_idx != static_cast<uint32_t>(-1)) {
        values.emplace_back(cur_tuple.GetValue(&GetOutputSchema(), column_idx));
        continue;
      }
      column_idx = j;
      auto w_f_it = window_functions.find(column_idx);
      auto &partition_by = w_f_it->second.partition_by_;
      auto &funtion = w_f_it->second.function_;
      auto &type = w_f_it->second.type_;
      std::vector<Value> key_values;
      key_values.reserve(partition_by.size());
      for (auto &expr : partition_by) {
        // std:: cout << expr->Evaluate(&ans_tuples_[i], GetOutputSchema()).ToString() << std::endl;
        key_values.emplace_back(expr->Evaluate(&cur_tuple, GetOutputSchema()));
      }
      ValuesKey key{key_values};
      auto new_val = funtion->Evaluate(&cur_tuple, GetOutputSchema());
      auto &part_ht = partitions_ht[column_idx];
      auto &r_ht = ranks_ht[column_idx];
      auto it = part_ht.find(key);
      if (it == part_ht.end()) {
        switch (type) {
          case WindowFunctionType::CountStarAggregate:
            part_ht[key] = ValueFactory::GetIntegerValue(1);
            break;
          case WindowFunctionType::CountAggregate:
            if (!new_val.IsNull()) {
              part_ht[key] = ValueFactory::GetIntegerValue(1);
            }
            break;
          case WindowFunctionType::SumAggregate:

          case WindowFunctionType::MinAggregate:

          case WindowFunctionType::MaxAggregate:
            if (!new_val.IsNull()) {
              part_ht[key] = new_val;
            }
            break;
          case WindowFunctionType::Rank:
            part_ht[key] = ValueFactory::GetIntegerValue(1);
            r_ht[key] = 1;
            break;
        }
      } else {
        switch (type) {
          case WindowFunctionType::CountStarAggregate:
            part_ht[key] = part_ht[key].Add(ValueFactory::GetIntegerValue(1));
            break;
          case WindowFunctionType::CountAggregate:
            if (!new_val.IsNull()) {
              part_ht[key] = part_ht[key].IsNull() ? ValueFactory::GetIntegerValue(1)
                                                   : part_ht[key].Add(ValueFactory::GetIntegerValue(1));
            }
            break;
          case WindowFunctionType::SumAggregate:
            if (!new_val.IsNull()) {
              part_ht[key] = part_ht[key].IsNull() ? new_val : part_ht[key].Add(new_val);
            }
            break;
          case WindowFunctionType::MinAggregate:
            if (!new_val.IsNull()) {
              part_ht[key] = part_ht[key].IsNull() ? new_val : part_ht[key].Min(new_val);
            }

            break;
          case WindowFunctionType::MaxAggregate:
            if (!new_val.IsNull()) {
              part_ht[key] = part_ht[key].IsNull() ? new_val : part_ht[key].Max(new_val);
            }
            break;
          case WindowFunctionType::Rank:
            auto &order_bys = w_f_it->second.order_by_;
            bool is_equal = true;
            for (auto &order_by : order_bys) {
              if (i == 0) {
                is_equal = false;
                break;
              }
              auto pre_value = order_by.second->Evaluate(&tuples[i - 1], GetOutputSchema());
              auto cur_value = order_by.second->Evaluate(&cur_tuple, GetOutputSchema());
              if (pre_value.CompareEquals(cur_value) != CmpBool::CmpTrue) {
                is_equal = false;
                break;
              }
            }
            if (!is_equal) {
              part_ht[key] = ValueFactory::GetIntegerValue(r_ht[key] + 1);
            }
            r_ht[key] += 1;
            break;
        }
      }
      values.emplace_back(part_ht[key]);
    }
    temp_tuples.emplace_back(values);
  }

  for (size_t i = 0; i < tuples_size; ++i) {
    auto &cur_tuple = tuples[i];
    std::vector<Value> values;
    values.reserve(GetOutputSchema().GetColumnCount());
    for (size_t j = 0; j < columns_size; ++j) {
      // get_colume_idx
      const auto &col_val = std::dynamic_pointer_cast<const ColumnValueExpression>(columns[j]);
      auto column_idx = col_val->GetColIdx();
      // std::cout << " column_idx: " << column_idx << std::endl;
      if (column_idx != static_cast<uint32_t>(-1)) {
        values.emplace_back(cur_tuple.GetValue(&GetOutputSchema(), column_idx));
        continue;
      }
      column_idx = j;
      auto w_f_it = window_functions.find(column_idx);
      auto &partition_by = w_f_it->second.partition_by_;
      // auto &funtion = w_f_it->second.function_;
      auto &order_by = w_f_it->second.order_by_;
      // auto &type = w_f_it->second.type_;
      std::vector<Value> key_values;
      key_values.reserve(partition_by.size());
      for (auto &expr : partition_by) {
        // std:: cout << expr->Evaluate(&ans_tuples_[i], GetOutputSchema()).ToString() << std::endl;
        key_values.push_back(expr->Evaluate(&cur_tuple, GetOutputSchema()));
      }
      ValuesKey key{key_values};
      auto &part_ht = partitions_ht[column_idx];
      // auto it = part_ht.find(key);
      if (!order_by.empty()) {
        values.emplace_back(temp_tuples[i][j]);
      } else {
        values.emplace_back(part_ht[key]);
      }
    }
    ans_tuples_.emplace_back(Tuple{values, &GetOutputSchema()});
  }
}

auto WindowFunctionExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (ans_index_ < ans_tuples_.size()) {
    *tuple = ans_tuples_[ans_index_++];
    return true;
  }
  return false;
}
}  // namespace bustub
