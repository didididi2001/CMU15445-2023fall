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
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManager::~BufferPoolManager() { delete[] pages_; }

auto BufferPoolManager::NewPage(page_id_t *page_id) -> Page * {
  {
    std::lock_guard<std::mutex> latch(latch_);
    frame_id_t frame_id;
    // 如果空闲列表中有可用的帧
    if (!free_list_.empty()) {
      // 从空闲列表中取出一个帧
      frame_id = free_list_.front();
      free_list_.pop_front();
    } else if (replacer_->Evict(&frame_id)) {
      // 获取即将被淘汰的页面
      auto &page_out = pages_[frame_id];
      // 如果页面是脏页，需要将其写回磁盘
      if (page_out.IsDirty()) {
        auto promise = disk_scheduler_->CreatePromise();
        auto future = promise.get_future();
        // 调度一个写入磁盘的请求
        disk_scheduler_->Schedule(
            {/*is_write=*/true, page_out.data_, /*page_id=*/page_out.page_id_, std::move(promise)});
        // 等待写入操作完成
        future.get();
      }
      // 从页面表中移除即将被淘汰的页面
      page_table_.erase(page_out.page_id_);
    } else {
      // 如果无法创建新页面，则返回空指针
      return nullptr;
    }

    // 获取新分配的页面
    auto &page = pages_[frame_id];
    // 重置页面的内存
    page.ResetMemory();
    // 初始化新页面的属性
    page.page_id_ = next_page_id_;
    page.pin_count_ = 1;
    page.is_dirty_ = false;
    // 更新页面表
    page_table_[page.page_id_] = frame_id;

    // 记录帧的访问记录和设置其为不可淘汰的
    replacer_->RecordAccess(frame_id);
    replacer_->SetEvictable(frame_id, false);

    // 分配一个新页面ID
    this->AllocatePage();

    // 设置page_id并返回页面指针
    *page_id = page.page_id_;
    return pages_ + frame_id;
  }
}

auto BufferPoolManager::FetchPage(page_id_t page_id, [[maybe_unused]] AccessType access_type) -> Page * {
  {
    std::lock_guard<std::mutex> latch(latch_);
    frame_id_t frame_id;

    // 如果页面存在于页面表中
    if (page_table_.find(page_id) != page_table_.end()) {
      // 获取页面所在的帧ID
      frame_id = page_table_[page_id];
      // 记录帧的访问记录和设置其为不可淘汰的
      replacer_->RecordAccess(frame_id);
      replacer_->SetEvictable(frame_id, false);
      // 增加页面的固定计数
      auto &page = pages_[frame_id];
      page.pin_count_++;
      return pages_ + frame_id;
    }
    // 如果空闲列表中有可用的帧
    if (!free_list_.empty()) {
      // 从空闲列表中取出一个帧
      frame_id = free_list_.front();
      free_list_.pop_front();
      // 在帧上放置一个新页面
      auto &page = pages_[frame_id];
      page.ResetMemory();
      // 初始化新页面的属性
      page.page_id_ = page_id;
      page.pin_count_ = 1;
      page.is_dirty_ = false;
      // 更新页面表
      page_table_[page.page_id_] = frame_id;
      // 调度一个从磁盘读取页面的请求
      auto promise = disk_scheduler_->CreatePromise();
      auto future = promise.get_future();
      disk_scheduler_->Schedule({/*is_write=*/false, page.data_, /*page_id=*/page_id, std::move(promise)});
      future.get();
      // 记录帧的访问记录和设置其为不可淘汰的
      replacer_->RecordAccess(frame_id);
      replacer_->SetEvictable(frame_id, false);
      return pages_ + frame_id;
    }
    if (replacer_->Evict(&frame_id)) {
      // 获取即将被淘汰的页面
      auto &page_out = pages_[frame_id];
      // 如果页面是脏页，需要将其写回磁盘
      if (page_out.IsDirty()) {
        auto promise = disk_scheduler_->CreatePromise();
        auto future = promise.get_future();
        // 调度一个写入磁盘的请求
        disk_scheduler_->Schedule(
            {/*is_write=*/true, page_out.data_, /*page_id=*/page_out.page_id_, std::move(promise)});
        future.get();
      }
      // 从页面表中移除即将被淘汰的页面
      page_table_.erase(page_out.page_id_);
      // 在帧上放置一个新页面
      auto &page_in = pages_[frame_id];
      auto promise = disk_scheduler_->CreatePromise();
      auto future = promise.get_future();
      page_in.ResetMemory();
      // 调度一个从磁盘读取页面的请求
      disk_scheduler_->Schedule({/*is_write=*/false, page_in.data_, /*page_id=*/page_id, std::move(promise)});
      future.get();
      // 初始化新页面的属性
      page_in.page_id_ = page_id;
      page_in.pin_count_ = 1;
      page_in.is_dirty_ = false;
      // 更新页面表
      page_table_[page_id] = frame_id;
      // 记录帧的访问记录和设置其为不可淘汰的
      replacer_->RecordAccess(frame_id);
      replacer_->SetEvictable(frame_id, false);
      return pages_ + frame_id;
    }
    // 如果无法创建新页面，则返回空指针
    return nullptr;
  }
}

auto BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty, [[maybe_unused]] AccessType access_type) -> bool {
  {
    std::lock_guard<std::mutex> latch(latch_);
    if (page_table_.find(page_id) == page_table_.end()) {
      return false;
    }
    auto frame_id = page_table_[page_id];
    auto &page = pages_[frame_id];
    if (page.GetPinCount() == 0) {
      return false;
    }
    page.pin_count_--;
    if (!page.is_dirty_) {
      page.is_dirty_ = is_dirty;
    }
    if (page.GetPinCount() == 0) {
      replacer_->SetEvictable(frame_id, true);
    }
    return true;
  }
}

auto BufferPoolManager::FlushPage(page_id_t page_id) -> bool {
  {
    std::lock_guard<std::mutex> latch(latch_);
    if (page_id == INVALID_PAGE_ID) {
      return false;
    }
    if (page_table_.find(page_id) == page_table_.end()) {
      return false;
    }
    auto frame_id = page_table_[page_id];
    auto &page = pages_[frame_id];
    auto promise = disk_scheduler_->CreatePromise();
    auto future = promise.get_future();
    disk_scheduler_->Schedule({/*is_write=*/true, page.data_, /*page_id=*/page.page_id_, std::move(promise)});
    future.get();
    page.is_dirty_ = false;
    return true;
  }
}

void BufferPoolManager::FlushAllPages() {
  std::lock_guard<std::mutex> latch(latch_);
  for (auto &it : page_table_) {
    auto &page = pages_[it.second];
    auto promise = disk_scheduler_->CreatePromise();
    auto future = promise.get_future();
    disk_scheduler_->Schedule({/*is_write=*/true, page.data_, /*page_id=*/page.page_id_, std::move(promise)});
    future.get();
    page.is_dirty_ = false;
  }
}

auto BufferPoolManager::DeletePage(page_id_t page_id) -> bool {
  {
    std::lock_guard<std::mutex> latch(latch_);
    if (page_table_.find(page_id) == page_table_.end()) {
      return true;
    }
    auto frame_id = page_table_[page_id];
    auto &page = pages_[frame_id];
    if (page.GetPinCount() != 0) {
      return false;
    }
    if (page.IsDirty()) {
      auto promise = disk_scheduler_->CreatePromise();
      auto future = promise.get_future();
      disk_scheduler_->Schedule({/*is_write=*/true, page.data_, /*page_id=*/page.page_id_, std::move(promise)});
      future.get();
    }
    page.ResetMemory();

    this->DeallocatePage(page_id);
    page_table_.erase(page_id);
    replacer_->Remove(frame_id);
    free_list_.push_back(frame_id);
    return true;
  }
}

auto BufferPoolManager::AllocatePage() -> page_id_t { return next_page_id_++; }

// auto BufferPoolManager::FetchPageBasic(page_id_t page_id) -> BasicPageGuard { return {this,
// this->FetchPage(page_id)}; }

// auto BufferPoolManager::FetchPageRead(page_id_t page_id) -> ReadPageGuard {
//   auto basic_guard = FetchPageBasic(page_id);
//   return basic_guard.UpgradeRead();
// }

// auto BufferPoolManager::FetchPageWrite(page_id_t page_id) -> WritePageGuard {
//   auto basic_guard = FetchPageBasic(page_id);
//   return basic_guard.UpgradeWrite();
// }

// auto BufferPoolManager::NewPageGuarded(page_id_t *page_id) -> BasicPageGuard { return {this, this->NewPage(page_id)};
// }

}  // namespace bustub
