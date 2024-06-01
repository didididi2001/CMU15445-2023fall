//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.cpp
//
// Identification: src/buffer/lru_k_replacer.cpp
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_k_replacer.h"
#include "common/exception.h"

namespace bustub {

void LRUKNode::RecordAccess(size_t timestamps) {
  if (history_.size() < k_) {
    history_.push_back(timestamps);
  } else {
    history_.push_back(timestamps);
    history_.pop_front();
  }
}

auto LRUKNode::GetDistance() const -> std::pair<size_t, size_t> {
  if (history_.size() >= k_) {
    return {history_.front(), history_.front()};
  }
  if (history_.empty()) {
    return {0, 0};
  }
  return {0, history_.front()};
}
auto LRUKNode::operator>(const LRUKNode &other) const -> bool {
  std::pair<size_t, size_t> this_distance = this->GetDistance();
  std::pair<size_t, size_t> other_distance = other.GetDistance();
  if (this_distance.first == other_distance.first) {
    return this_distance.second < other_distance.second;
  }
  return this_distance.first < other_distance.first;
}
auto LRUKNode::operator<(const LRUKNode &other) const -> bool { return !(*this > other); }

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : curr_size_(num_frames), k_(k) {}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  std::lock_guard<std::mutex> latch(latch_);

  if (!l_list_.empty()) {
    for (auto it = l_list_.begin(); it != l_list_.end(); ++it) {
      if ((*it)->IsEvictable()) {
        *frame_id = (*it)->GetFrameId();
        new_node_store_.erase(*frame_id);
        l_list_.erase(it);
        replacer_size_--;
        return true;
      }
    }
  }

  for (auto it = r_list_.begin(); it != r_list_.end(); ++it) {
    if ((*it)->IsEvictable()) {
      *frame_id = (*it)->GetFrameId();
      new_node_store_.erase(*frame_id);
      r_list_.erase(it);
      replacer_size_--;
      return true;
    }
  }
  frame_id = nullptr;
  return false;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id, [[maybe_unused]] AccessType access_type) {
  std::lock_guard<std::mutex> latch(latch_);
  auto it = new_node_store_.find(frame_id);
  if (it != new_node_store_.end()) {
    bool is_r = it->second->GetHistorySzie() == k_;
    it->second->RecordAccess(++current_timestamp_);
    if (it->second->GetHistorySzie() == k_) {
      // from l_list or r_list remove
      if (!is_r) {
        for (auto it_l = l_list_.begin(); it_l != l_list_.end(); ++it_l) {
          if ((*it_l)->GetFrameId() == frame_id) {
            l_list_.erase(it_l);
            break;  // 找到并删除后可以跳出循环
          }
        }
        auto it_r = r_list_.begin();
        for (; it_r != r_list_.end(); ++it_r) {
          if (*(it->second.get()) > *((*it_r).get())) {
            break;
          }
        }
        r_list_.insert(it_r, it->second);

      } else {
        for (auto it_r = r_list_.begin(); it_r != r_list_.end(); ++it_r) {
          if ((*it_r)->GetFrameId() == frame_id) {
            r_list_.erase(it_r);
            break;  // 找到并删除后可以跳出循环
          }
        }
        // insert and update position
        auto r_it = r_list_.begin();
        for (; r_it != r_list_.end(); ++r_it) {
          if (*(it->second.get()) > *((*r_it).get())) {
            break;
          }
        }
        r_list_.insert(r_it, it->second);
      }
      // <= k 不用管
    }
  } else {
    auto ptr = std::make_shared<LRUKNode>(k_, frame_id);
    ptr->RecordAccess(++current_timestamp_);
    l_list_.emplace_back(ptr);
    new_node_store_.emplace(frame_id, ptr);
  }
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  std::lock_guard<std::mutex> latch(latch_);

  auto &temp = new_node_store_[frame_id];
  //   std::cout << replacer_size_ << std::endl;
  if (temp->IsEvictable() && !set_evictable) {
    replacer_size_--;
  }
  if (!temp->IsEvictable() && set_evictable) {
    replacer_size_++;
  }
  temp->SetEvictable(set_evictable);
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  std::lock_guard<std::mutex> latch(latch_);

  auto temp = new_node_store_[frame_id];
  if (temp == nullptr) {
    return;
  }
  if (temp->IsEvictable()) {
    replacer_size_--;
    bool is_r = temp->GetHistorySzie() == k_;
    if (!is_r) {
      for (auto it_l = l_list_.begin(); it_l != l_list_.end(); ++it_l) {
        if ((*it_l)->GetFrameId() == frame_id) {
          l_list_.erase(it_l);
          break;  // 找到并删除后可以跳出循环
        }
      }
    } else {
      for (auto it_r = r_list_.begin(); it_r != r_list_.end(); ++it_r) {
        if ((*it_r)->GetFrameId() == frame_id) {
          r_list_.erase(it_r);
          break;  // 找到并删除后可以跳出循环
        }
      }
    }
    new_node_store_.erase(frame_id);
  }
}

auto LRUKReplacer::Size() -> size_t { return replacer_size_; }

auto LRUKReplacer::CurSize() -> size_t { return curr_size_; }

void LRUKReplacer::PrintList() {
  for (auto it_r = l_list_.begin(); it_r != l_list_.end(); ++it_r) {
    std::cout << (*it_r)->GetFrameId() << " ";
  }
  std::cout << " ||";
  for (auto it_r = r_list_.begin(); it_r != r_list_.end(); ++it_r) {
    std::cout << (*it_r)->GetFrameId() << " ";
  }
  std::cout << "\n";
}
}  // namespace bustub
