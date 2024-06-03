//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager.h"

#include "common/exception.h"
#include "common/macros.h"
#include "storage/page/page_guard.h"

namespace bustub {

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager, size_t replacer_k,
                                     LogManager *log_manager)
    : pool_size_(pool_size), disk_scheduler_(std::make_unique<DiskScheduler>(disk_manager)), log_manager_(log_manager) {
  // TODO(students): remove this line after you have implemented the buffer pool manager
  // throw NotImplementedException(
  //     "BufferPoolManager is not implemented yet. If you have finished implementing BPM, please remove the throw "
  //     "exception line in `buffer_pool_manager.cpp`.");

  // we allocate a consecutive memory space for the buffer pool
  pages_ = new Page[pool_size_];
  replacer_ = std::make_unique<LRUKReplacer>(pool_size, replacer_k);

  // Initially, every page is in the free list.
  avaliable_ = std::vector<std::pair<std::condition_variable, bool>>(pool_size);
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
    avaliable_[i].second = true;
  }
}

BufferPoolManager::~BufferPoolManager() { delete[] pages_; }

auto BufferPoolManager::NewPage(page_id_t *page_id) -> Page * {
  std::unique_lock lock(latch_);
  frame_id_t frame_id(-1);
  if (!free_list_.empty()) {  // find from free list
    frame_id = free_list_.front();
    free_list_.pop_front();
  } else {  // find from replacer
    replacer_->Evict(&frame_id);
  }

  // allocate page
  if (frame_id >= 0) {
    // remove old page
    page_table_.erase(pages_[frame_id].page_id_);

    // insert new frame
    replacer_->RecordAccess(frame_id);
    replacer_->SetEvictable(frame_id, false);

    // allocate new page
    *page_id = AllocatePage();
    page_table_[*page_id] = frame_id;
    pages_[frame_id].pin_count_ = 1;

    if (pages_[frame_id].IsDirty()) {
      // dirty page, need to write back to disk
      FlushFrame(frame_id);
    }
    pages_[frame_id].page_id_ = *page_id;
    pages_[frame_id].ResetMemory();

    return &pages_[frame_id];
  }
  return nullptr;
}

auto BufferPoolManager::FetchPage(page_id_t page_id, [[maybe_unused]] AccessType access_type) -> Page * {
  std::unique_lock lock(latch_);
  frame_id_t frame_id;

  auto it = page_table_.find(page_id);
  // 如果页面存在于页面表中
  if (it != page_table_.end()) {
    // 获取页面所在的帧ID
    frame_id = it->second;
    // 记录帧的访问记录和设置其为不可淘汰的
    replacer_->RecordAccess(frame_id);
    replacer_->SetEvictable(frame_id, false);
    // 增加页面的固定计数
    ++pages_[frame_id].pin_count_;
    if (!avaliable_[frame_id].second) {
      avaliable_[frame_id].first.wait(lock, [this, frame_id]() { return avaliable_[frame_id].second; });
    }
    return pages_ + frame_id;
  }
  // 如果空闲列表中有可用的帧
  if (!free_list_.empty()) {
    // 从空闲列表中取出一个帧
    frame_id = free_list_.front();
    free_list_.pop_front();
    // 在帧上放置一个新页面
    auto &page = pages_[frame_id];
    page_table_.erase(page.page_id_);
    page_table_[page_id] = frame_id;
    page.pin_count_ = 1;
    replacer_->RecordAccess(frame_id);
    replacer_->SetEvictable(frame_id, false);
    if (page.IsDirty()) {
      FlushFrame(frame_id);
    }
    // 初始化新页面的属性
    page.page_id_ = page_id;
    ReadFrame(frame_id);

    return pages_ + frame_id;
  }
  if (replacer_->Evict(&frame_id)) {
    // 获取即将被淘汰的页面
    auto &page_out = pages_[frame_id];
    page_table_.erase(page_out.page_id_);
    page_table_[page_id] = frame_id;
    page_out.pin_count_ = 1;
    replacer_->RecordAccess(frame_id);
    replacer_->SetEvictable(frame_id, false);
    // 如果页面是脏页，需要将其写回磁盘
    if (page_out.IsDirty()) {
      FlushFrame(frame_id);
    }
    auto &page_in = pages_[frame_id];
    page_in.page_id_ = page_id;
    ReadFrame(frame_id);

    return pages_ + frame_id;
  }
  // 如果无法创建新页面，则返回空指针
  return nullptr;
}

auto BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty, [[maybe_unused]] AccessType access_type) -> bool {
  std::unique_lock lock(latch_);
  auto it = page_table_.find(page_id);
  if (it == page_table_.end()) {
    return false;
  }
  auto frame_id = it->second;
  auto &page = pages_[frame_id];
  if (page.GetPinCount() == 0) {
    return false;
  }
  page.pin_count_--;
  page.is_dirty_ |= is_dirty;
  if (page.GetPinCount() == 0) {
    replacer_->SetEvictable(frame_id, true);
  }
  return true;
}

auto BufferPoolManager::FlushPage(page_id_t page_id) -> bool {
  std::lock_guard lg(latch_);
  if (page_id == INVALID_PAGE_ID) {
    return false;
  }
  auto it = page_table_.find(page_id);
  if (it == page_table_.end()) {
    return false;
  }
  auto frame_id = it->second;
  FlushFrame(frame_id);
  return true;
}

void BufferPoolManager::FlushAllPages() {
  std::lock_guard lg(latch_);
  for (auto &it : page_table_) {
    FlushFrame(it.second);
  }
}

auto BufferPoolManager::DeletePage(page_id_t page_id) -> bool {
  std::lock_guard lg(latch_);
  auto it = page_table_.find(page_id);
  if (it == page_table_.end()) {
    return true;
  }
  auto frame_id = it->second;
  auto &page = pages_[frame_id];
  if (page.GetPinCount() != 0) {
    return false;
  }
  page_table_.erase(page_id);
  replacer_->Remove(frame_id);
  free_list_.push_back(frame_id);
  if (page.IsDirty()) {
    FlushFrame(frame_id);
  }
  // page.ResetMemory();
  this->DeallocatePage(page_id);

  return true;
}

void BufferPoolManager::FlushFrame(frame_id_t frame_id) {
  auto promise = disk_scheduler_->CreatePromise();
  auto future = promise.get_future();
  disk_scheduler_->Schedule({true, pages_[frame_id].GetData(), pages_[frame_id].GetPageId(), std::move(promise)});
  avaliable_[frame_id].second = false;
  latch_.unlock();
  BUSTUB_ENSURE(future.get(), "write page failed!");
  latch_.lock();
  avaliable_[frame_id].second = true;
  avaliable_[frame_id].first.notify_all();
  pages_[frame_id].is_dirty_ = false;
}

void BufferPoolManager::ReadFrame(frame_id_t frame_id) {
  auto promise = disk_scheduler_->CreatePromise();
  auto future = promise.get_future();
  disk_scheduler_->Schedule({false, pages_[frame_id].GetData(), pages_[frame_id].GetPageId(), std::move(promise)});
  avaliable_[frame_id].second = false;
  latch_.unlock();
  BUSTUB_ENSURE(future.get(), "read page failed!");
  latch_.lock();
  avaliable_[frame_id].second = true;
  avaliable_[frame_id].first.notify_all();
}

auto BufferPoolManager::AllocatePage() -> page_id_t { return next_page_id_++; }

auto BufferPoolManager::FetchPageBasic(page_id_t page_id) -> BasicPageGuard { return {this,
this->FetchPage(page_id)}; }

auto BufferPoolManager::FetchPageRead(page_id_t page_id) -> ReadPageGuard {
  auto basic_guard = FetchPageBasic(page_id);
  return basic_guard.UpgradeRead();
}

auto BufferPoolManager::FetchPageWrite(page_id_t page_id) -> WritePageGuard {
  auto basic_guard = FetchPageBasic(page_id);
  return basic_guard.UpgradeWrite();
}

auto BufferPoolManager::NewPageGuarded(page_id_t *page_id) -> BasicPageGuard { return {this, this->NewPage(page_id)};
}

}  // namespace bustub
