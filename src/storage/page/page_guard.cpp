#include "storage/page/page_guard.h"
#include "buffer/buffer_pool_manager.h"

namespace bustub {

BasicPageGuard::BasicPageGuard(BasicPageGuard &&that) noexcept {
  // std::cout << "Function Signature: " << __PRETTY_FUNCTION__ << std::endl;
  is_dirty_ = that.is_dirty_;
  that.is_dirty_ = false;
  page_ = that.page_;
  that.page_ = nullptr;
  bpm_ = that.bpm_;
  that.bpm_ = nullptr;
}
void BasicPageGuard::Drop() {
  // std::cout << "Function Signature: " << __PRETTY_FUNCTION__ << std::endl;
  if (page_ != nullptr) {
    bpm_->UnpinPage(this->PageId(), this->is_dirty_);
    is_dirty_ = false;
    page_ = nullptr;
    bpm_ = nullptr;
  }
}

auto BasicPageGuard::operator=(BasicPageGuard &&that) noexcept -> BasicPageGuard & {
  // std::cout << "Function Signature: " << __PRETTY_FUNCTION__ << std::endl;
  if (page_ == nullptr || that.PageId() != PageId()) {
    Drop();
  } else {
    return *this;
  }
  is_dirty_ = that.is_dirty_;
  bpm_ = that.bpm_;
  page_ = that.page_;
  that.is_dirty_ = false;
  that.bpm_ = nullptr;
  that.page_ = nullptr;
  return *this;
}

BasicPageGuard::~BasicPageGuard() {
  // std::cout << "Function Signature: " << __PRETTY_FUNCTION__ << std::endl;
  // unpin once
  Drop();
}
auto BasicPageGuard::UpgradeRead() -> ReadPageGuard {
  // std::cout << "Function Signature: " << __PRETTY_FUNCTION__ << std::endl;
  if (page_ != nullptr) {
    page_->RLatch();
  }
  ReadPageGuard readpageguard(bpm_, page_);
  // this->Drop();
  bpm_ = nullptr;
  page_ = nullptr;
  return readpageguard;
}
auto BasicPageGuard::UpgradeWrite() -> WritePageGuard {
  // std::cout << "Function Signature: " << __PRETTY_FUNCTION__ << std::endl;
  if (page_ != nullptr) {
    page_->WLatch();
  }

  WritePageGuard writepageguard(bpm_, page_);
  // page_->RLatch();
  // this->Drop();
  bpm_ = nullptr;
  page_ = nullptr;
  return writepageguard;
}

ReadPageGuard::ReadPageGuard(ReadPageGuard &&that) noexcept {
  // std::cout << "Function Signature: " << __PRETTY_FUNCTION__ << std::endl;
  this->guard_ = std::move(that.guard_);
}

auto ReadPageGuard::operator=(ReadPageGuard &&that) noexcept -> ReadPageGuard & {
  // std::cout << "Function Signature: " << __PRETTY_FUNCTION__ << std::endl;
  Drop();
  this->guard_ = std::move(that.guard_);
  return *this;
}

void ReadPageGuard::Drop() {
  // std::cout << "Function Signature: " << __PRETTY_FUNCTION__ << std::endl;
  if (guard_.page_ != nullptr) {
    guard_.page_->RUnlatch();
  }
  guard_.Drop();
}

ReadPageGuard::~ReadPageGuard() {
  // std::cout << "Function Signature: " << __PRETTY_FUNCTION__ << std::endl;
  Drop();

  // guard_.bpm_->UnpinPage(guard_.PageId(), guard_.is_dirty_);
}

WritePageGuard::WritePageGuard(WritePageGuard &&that) noexcept {
  // std::cout << "Function Signature: " << __PRETTY_FUNCTION__ << std::endl;
  this->guard_ = std::move(that.guard_);
}

auto WritePageGuard::operator=(WritePageGuard &&that) noexcept -> WritePageGuard & {
  // std::cout << "Function Signature: " << __PRETTY_FUNCTION__ << std::endl;
  Drop();
  this->guard_ = std::move(that.guard_);
  return *this;
}

void WritePageGuard::Drop() {
  // std::cout << "Function Signature: " << __PRETTY_FUNCTION__ << std::endl;
  if (guard_.page_ != nullptr) {
    guard_.page_->WUnlatch();
  }
  // guard_.page_->RUnlatch();
  guard_.Drop();
}

WritePageGuard::~WritePageGuard() {
  // std::cout << "Function Signature: " << __PRETTY_FUNCTION__ << std::endl;

  Drop();
}
}  // namespace bustub
