#include "primer/trie.h"
#include <iostream>
#include <string_view>
namespace bustub {

template <class T>
auto Trie::Get(std::string_view key) const -> const T * {
  // throw NotImplementedException("Trie::Get is not implemented.");
  auto p = root_;
  for (const auto &c : key) {
    // auto rawPtr = p.get();
    // std::cout << "Address of the object: " << rawPtr << std::endl;
    if (p == nullptr) {
      return nullptr;
    }
    auto it = p->children_.find(c);
    if (it != p->children_.end()) {
      p = p->children_.at(c);
    } else {
      return nullptr;
    }
  }
  if (auto twd_ptr = std::dynamic_pointer_cast<const TrieNodeWithValue<T>>(p)) {
    // using ValueType = typename std::remove_reference<decltype(*TwdPtr->value_)>::type;
    // if (std::is_same<T, ValueType>::value) {
    //     return TwdPtr->value_.get();
    // } else {
    //     return nullptr;
    // }
    return twd_ptr->value_.get();
  }
  return nullptr;
  // You should walk through the trie to find the node corresponding to the key. If the node doesn't exist, return
  // nullptr. After you find the node, you should use `dynamic_cast` to cast it to `const TrieNodeWithValue<T> *`. If
  // dynamic_cast returns `nullptr`, it means the type of the value is mismatched, and you should return nullptr.
  // Otherwise, return the value.
}

template <class T>
auto Trie::Put(std::string_view key, T value) const -> Trie {
  /*if (auto existingValue = this->Get<T>(key); existingValue != nullptr && *existingValue == value) {
      return Trie(root_);
  }*/
  if (key.empty()) {
    if (root_ == nullptr) {
      return Trie(std::make_shared<const TrieNodeWithValue<T>>(std::map<char, std::shared_ptr<const TrieNode>>(),
                                                               std::move(std::make_shared<T>(std::move(value)))));
    }
    return Trie(std::make_shared<const TrieNodeWithValue<T>>(root_->children_,
                                                             std::move(std::make_shared<T>(std::move(value)))));
  }
  std::shared_ptr<TrieNode> pre;
  std::shared_ptr<TrieNode> root;
  std::shared_ptr<TrieNode> cur;
  size_t i = 0;
  if (root_ == nullptr) {
    cur = std::shared_ptr<TrieNode>();
  } else {
    // cur = std::shared_ptr<TrieNode>(std::move(root_->Clone()));
    cur = std::shared_ptr<TrieNode>(root_->Clone());
  }

  while (i < key.length() && cur != nullptr && cur->children_.find(key[i]) != cur->children_.end()) {
    if (pre != nullptr) {
      pre->children_[key[i - 1]] = cur;
    } else {
      root = cur;
    }
    pre = cur;
    // cur = std::shared_ptr<TrieNode>(std::move(cur->children_.at(key[i])->Clone()));
    cur = std::shared_ptr<TrieNode>(cur->children_.at(key[i])->Clone());
    i++;
  }
  std::shared_ptr<TrieNode> node = cur;
  if (node == nullptr) {
    node = std::make_shared<TrieNode>(std::map<char, std::shared_ptr<const TrieNode>>());
  }
  while (i < key.length()) {
    if (pre != nullptr) {
      pre->children_[key[i - 1]] = node;
      pre = node;
    } else {
      root = node;
      pre = node;
    }
    node = std::make_shared<TrieNode>(std::map<char, std::shared_ptr<const TrieNode>>());
    ++i;
  }
  auto last_char = key[key.size() - 1];
  if (pre->children_.find(last_char) != pre->children_.end()) {
    pre->children_[last_char] = std::make_shared<const TrieNodeWithValue<T>>(
        pre->children_[last_char]->children_, std::move(std::make_shared<T>(std::move(value))));
  } else {
    pre->children_[last_char] = std::make_shared<const TrieNodeWithValue<T>>(
        std::map<char, std::shared_ptr<const TrieNode>>(), std::move(std::make_shared<T>(std::move(value))));
  }
  return Trie(root);

  return {};
}

auto Trie::Remove(std::string_view key) const -> Trie {
  // Check if the trie is empty, return an empty trie if so
  if (root_ == nullptr) {
    return Trie(root_);
  }

  // Case where key is empty and root node is not a value node
  if (key.empty() && !root_->is_value_node_) {
    return Trie(root_);
  }
  if (key.empty() && root_->is_value_node_) {
    // Case where key is empty and root node is a value node
    // If root has no children, return an empty trie
    if (root_->children_.empty()) {
      return {};
    }
    // Otherwise, create a new trie with root's children as root node
    return Trie(std::make_shared<TrieNode>(root_->children_));
  }
  // Variable to store the index where we need to save
  size_t save_index = key.size() + 1;
  auto cur = root_;

  // Loop through the characters in the key
  for (size_t i = 0; i < key.size(); i++) {
    auto c = key[i];
    // Update save_index if current node is a value node or has more than one child
    if (cur->is_value_node_ || cur->children_.size() != 1) {
      save_index = i;
    }

    // If current node is null or does not contain the current character, return current trie
    if (cur == nullptr || cur->children_.find(c) == cur->children_.end()) {
      return Trie(root_);
    }
    cur = cur->children_.at(c);
  }

  // If the last node is not a value node, return current trie
  if (!cur->is_value_node_) {
    return Trie(root_);
  }
  // Update save_index if the last node has children
  if (!cur->children_.empty()) {
    save_index = key.size();
  }

  // If save_index indicates no need for removal, return an empty trie
  if (save_index == key.size() + 1) {
    return {};
  }
  // Remove nodes starting from save_index
  std::shared_ptr<TrieNode> pre;
  std::shared_ptr<TrieNode> root;
  // auto node = std::shared_ptr<TrieNode>(std::move(root_->Clone()));
  auto node = std::shared_ptr<TrieNode>(root_->Clone());
  // Traverse to save_index and update trie nodes accordingly
  for (size_t i = 0; i < save_index; i++) {
    auto c = key[i];
    if (node != nullptr && node->children_.find(c) != node->children_.end()) {
      if (pre == nullptr) {
        root = node;
      } else {
        pre->children_[key[i - 1]] = node;
      }
    }
    pre = node;
    // node = std::shared_ptr<TrieNode>(std::move(node->children_.at(c)->Clone()));
    node = std::shared_ptr<TrieNode>(node->children_.at(c)->Clone());
  }

  // Update trie structure based on save_index
  if (save_index == key.size()) {
    pre->children_[key[save_index - 1]] =
        std::make_shared<const TrieNode>(pre->children_[key[save_index - 1]]->children_);
  } else if (save_index == 0) {
    // root = std::shared_ptr<TrieNode>(std::move(root_->Clone()));
    root = std::shared_ptr<TrieNode>(root_->Clone());
    root->children_[key[save_index]].reset();
    root->children_.erase(key[save_index]);
  } else {
    node->children_[key[save_index]].reset();
    node->children_.erase(key[save_index]);
    pre->children_[key[save_index - 1]] = node;
  }
  return Trie(root);
  return {};
  // You should walk through the trie and remove nodes if necessary. If the node doesn't contain a value any more,
  // you should convert it to `TrieNode`. If a node doesn't have children any more, you should remove it.
}

// Below are explicit instantiation of template functions.
//
// Generally people would write the implementation of template classes and functions in the header file. However, we
// separate the implementation into a .cpp file to make things clearer. In order to make the compiler know the
// implementation of the template functions, we need to explicitly instantiate them here, so that they can be picked up
// by the linker.

template auto Trie::Put(std::string_view key, uint32_t value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const uint32_t *;

template auto Trie::Put(std::string_view key, uint64_t value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const uint64_t *;

template auto Trie::Put(std::string_view key, std::string value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const std::string *;

// If your solution cannot compile for non-copy tests, you can remove the below lines to get partial score.

using Integer = std::unique_ptr<uint32_t>;

template auto Trie::Put(std::string_view key, Integer value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const Integer *;

template auto Trie::Put(std::string_view key, MoveBlocked value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const MoveBlocked *;

}  // namespace bustub
