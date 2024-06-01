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

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : curr_size_(num_frames), k_(k) {}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  {
    std::lock_guard<std::mutex> latch(latch_);
    frame_id_t max_frame_id = -1;
    LRUKNode *max_node;
    for (auto &it : node_store_) {
      // 检查当前节点的 frame_id 是否等于 frame_id，以及是否可被驱逐
      if (it.second.IsEvictable()) {
        if (max_frame_id == -1 || (it.second > *max_node)) {
          max_frame_id = it.first, max_node = &it.second;
        }
      }
    }
    if (max_frame_id != -1) {
      replacer_size_--;
      max_node->SetEvictable(false);
      max_node->GetHistory().clear();
      // node_store_.erase(max_frame_id);
      *frame_id = max_frame_id;
      return true;
    }
    frame_id = nullptr;
    return false;
  }
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id, [[maybe_unused]] AccessType access_type) {
  {
    std::lock_guard<std::mutex> latch(latch_);

    if (node_store_.find(frame_id) != node_store_.end()) {
      node_store_[frame_id].RecordAccess(++current_timestamp_);
    } else {
      LRUKNode temp(k_);
      temp.RecordAccess(++current_timestamp_);
      node_store_.insert(std::make_pair(frame_id, temp));
    }
  }
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  {
    std::lock_guard<std::mutex> latch(latch_);

    auto &temp = node_store_[frame_id];
    //   std::cout << replacer_size_ << std::endl;
    if (temp.IsEvictable() && !set_evictable) {
      replacer_size_--;
    }
    if (!temp.IsEvictable() && set_evictable) {
      replacer_size_++;
    }
    temp.SetEvictable(set_evictable);
  }
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  {
    std::lock_guard<std::mutex> latch(latch_);

    auto temp = node_store_[frame_id];
    if (temp.IsEvictable()) {
      // 删除当前节点
      replacer_size_--;
      node_store_.erase(frame_id);
      return;  // 删除成功，直接返回
    }
  }
}

auto LRUKReplacer::Size() -> size_t { return replacer_size_; }

auto LRUKReplacer::CurSize() -> size_t { return curr_size_; }

}  // namespace bustub
