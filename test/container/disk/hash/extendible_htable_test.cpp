//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// extendible_htable_test.cpp
//
// Identification: test/container/disk/hash/extendible_htable_test.cpp
//
// Copyright (c) 2015-2023, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <thread>  // NOLINT
#include <vector>

#include "buffer/buffer_pool_manager.h"
#include "common/logger.h"
#include "container/disk/hash/disk_extendible_hash_table.h"
#include "gtest/gtest.h"
#include "murmur3/MurmurHash3.h"
#include "storage/disk/disk_manager_memory.h"

namespace bustub {

// NOLINTNEXTLINE
TEST(ExtendibleHTableTest, InsertTest1) {
  auto disk_mgr = std::make_unique<DiskManagerUnlimitedMemory>();
  auto bpm = std::make_unique<BufferPoolManager>(50, disk_mgr.get());

  DiskExtendibleHashTable<int, int, IntComparator> ht("blah", bpm.get(), IntComparator(), HashFunction<int>(), 0, 2, 2);
  // int a = 1;
  // std::cout << HashFunction<int>().GetHash(a) << std::endl;
  int num_keys = 8;
  // std::cout << "begin insert" << std::endl;
  // insert some values
  for (int i = 0; i < num_keys; i++) {
    // std::cout << "insert: " << i  << std::endl;
    bool inserted = ht.Insert(i, i);
    // ht.PrintHT();
    ASSERT_TRUE(inserted);
    std::vector<int> res;
    ht.GetValue(i, &res);
    ASSERT_EQ(1, res.size());
    ASSERT_EQ(i, res[0]);
  }
  // std::cout << "insert finish\n";
  ht.VerifyIntegrity();

  // attempt another insert, this should fail because table is full
  ASSERT_FALSE(ht.Insert(num_keys, num_keys));
  // std::cout << "InsertTest1 finish\n";
}

// NOLINTNEXTLINE
TEST(ExtendibleHTableTest, InsertTest2) {
  auto disk_mgr = std::make_unique<DiskManagerUnlimitedMemory>();
  auto bpm = std::make_unique<BufferPoolManager>(50, disk_mgr.get());

  DiskExtendibleHashTable<int, int, IntComparator> ht("blah", bpm.get(), IntComparator(), HashFunction<int>(), 0, 2, 2);
  // int a = 1;
  // std::cout << HashFunction<int>().GetHash(a) << std::endl;
  // std::cout << "begin insert" << std::endl;
  // insert some values
  std::vector<int> num_keys{4,5,6,14};
  for (auto &i : num_keys) {
    // std::cout << "insert: " << i << std::endl;
    bool inserted = ht.Insert(i, i);
    // ht.PrintHT();
    ASSERT_TRUE(inserted);
    // std::cout << "insert result: " << inserted << std::endl;
    std::vector<int> res;
    ht.GetValue(i, &res);
    ASSERT_EQ(1, res.size());
    ASSERT_EQ(i, res[0]);
  }
  // std::cout << "insert finish\n";
  ht.VerifyIntegrity();
}
// NOLINTNEXTLINE
TEST(ExtendibleHTableTest, InsertTest3) {
  auto disk_mgr = std::make_unique<DiskManagerUnlimitedMemory>();
  auto bpm = std::make_unique<BufferPoolManager>(50, disk_mgr.get());

  DiskExtendibleHashTable<int, int, IntComparator> ht("blah", bpm.get(), IntComparator(), HashFunction<int>(), 2, 3, 2);
  // ht.PrintHT();
  int num_keys = 5;

  // insert some values
  for (int i = 0; i < num_keys; i++) {
    bool inserted = ht.Insert(i, i);
    ASSERT_TRUE(inserted);
    std::vector<int> res;
    ht.GetValue(i, &res);
    ASSERT_EQ(1, res.size());
    ASSERT_EQ(i, res[0]);
  }

  ht.VerifyIntegrity();
  // check that they were actually inserted
  for (int i = 0; i < num_keys; i++) {
    std::vector<int> res;
    bool got_value = ht.GetValue(i, &res);
    ASSERT_TRUE(got_value);
    ASSERT_EQ(1, res.size());
    ASSERT_EQ(i, res[0]);
  }

  ht.VerifyIntegrity();
  // ht.PrintHT();
  // try to get some keys that don't exist/were not inserted
  for (int i = num_keys; i < 2 * num_keys; i++) {
    std::vector<int> res;
    bool got_value = ht.GetValue(i, &res);
    ASSERT_FALSE(got_value);
    ASSERT_EQ(0, res.size());
  }

  ht.VerifyIntegrity();
  // std::cout << "InsertTest2 finish\n";
}

// NOLINTNEXTLINE
TEST(ExtendibleHTableTest, RemoveTest1) {
  auto disk_mgr = std::make_unique<DiskManagerUnlimitedMemory>();
  auto bpm = std::make_unique<BufferPoolManager>(50, disk_mgr.get());

  DiskExtendibleHashTable<int, int, IntComparator> ht("blah", bpm.get(), IntComparator(), HashFunction<int>(), 2, 3, 2);

  int num_keys = 5;

  // insert some values
  for (int i = 0; i < num_keys; i++) {
    bool inserted = ht.Insert(i, i);
    ASSERT_TRUE(inserted);
    std::vector<int> res;
    ht.GetValue(i, &res);
    ASSERT_EQ(1, res.size());
    ASSERT_EQ(i, res[0]);
  }
  // std::cout << "insert before remove finish\n";
  ht.VerifyIntegrity();
  // ht.PrintHT();
  // check that they were actually inserted
  for (int i = 0; i < num_keys; i++) {
    std::vector<int> res;
    bool got_value = ht.GetValue(i, &res);
    ASSERT_TRUE(got_value);
    ASSERT_EQ(1, res.size());
    ASSERT_EQ(i, res[0]);
  }
  // std::cout << "get value before remove finish\n";
  ht.VerifyIntegrity();
  // ht.PrintHT();
  // try to get some keys that don't exist/were not inserted
  for (int i = num_keys; i < 2 * num_keys; i++) {
    std::vector<int> res;
    bool got_value = ht.GetValue(i, &res);
    ASSERT_FALSE(got_value);
    ASSERT_EQ(0, res.size());
  }
  // std::cout << "get invalid value before remove finish\n";
  ht.VerifyIntegrity();
  // ht.PrintHT();
  // remove the keys we inserted
  for (int i = 0; i < num_keys; i++) {
    // std::cout << "remove: " << i  << std::endl;
    bool removed = ht.Remove(i);
    // ht.PrintHT();
    ASSERT_TRUE(removed);
    std::vector<int> res;
    ht.GetValue(i, &res);
    ASSERT_EQ(0, res.size());
  }
  // std::cout << "remove finish\n";
  ht.VerifyIntegrity();

  // try to remove some keys that don't exist/were not inserted
  for (int i = num_keys; i < 2 * num_keys; i++) {
    bool removed = ht.Remove(i);
    ASSERT_FALSE(removed);

    std::vector<int> res;
    bool got_value = ht.GetValue(i, &res);
    ASSERT_FALSE(got_value);
    ASSERT_EQ(0, res.size());
  }

  ht.VerifyIntegrity();
}

TEST(ExtendibleHTableTest, RemoveTest10) {
  auto disk_mgr = std::make_unique<DiskManagerUnlimitedMemory>();
  auto bpm = std::make_unique<BufferPoolManager>(50, disk_mgr.get());
  DiskExtendibleHashTable<int, int, IntComparator> ht("blah", bpm.get(), IntComparator(), HashFunction<int>(), 0, 3, 2);
  int insertNum[]{4, 5, 6, 14};
  // insert some values
  for (int i = 0; i < 4; i++) {
    bool inserted = ht.Insert(insertNum[i], insertNum[i]);
    ASSERT_TRUE(inserted);
    std::vector<int> res;
    ht.GetValue(insertNum[i], &res);
    ASSERT_EQ(1, res.size());
    ASSERT_EQ(insertNum[i], res[0]);
  }
  ht.VerifyIntegrity();
  ht.PrintHT();
  int removeNum[]{5,14,4,6};
  for (int i = 0; i < 4; ++i) {
    std::cout << "remove: " << removeNum[i] << std::endl;
    bool removed = ht.Remove(removeNum[i]);
    ASSERT_TRUE(removed);
    ht.PrintHT();
    std::vector<int> res;
    ht.GetValue(removeNum[i], &res);
    ASSERT_EQ(0, res.size());
  }
  ht.VerifyIntegrity();
}

// NOLINTNEXTLINE
TEST(ExtendibleHTableTest, GrowShrinkTest) {
  auto disk_mgr = std::make_unique<DiskManagerUnlimitedMemory>();
  auto bpm = std::make_unique<BufferPoolManager>(3, disk_mgr.get());
  DiskExtendibleHashTable<int, int, IntComparator> ht("blah", bpm.get(), IntComparator(), HashFunction<int>(), 9, 9,
                                                      511);
  // insert some values
  for (int i = 0; i < 1000; i++) {
    bool inserted = ht.Insert(i, i);
    ASSERT_TRUE(inserted);
    std::vector<int> res;
    ht.GetValue(i, &res);
    ASSERT_EQ(1, res.size());
    ASSERT_EQ(i, res[0]);
  }
  ht.VerifyIntegrity();
  // ht.PrintHT();
  for (int i = 0; i < 500; i++) {
    bool inserted = ht.Remove(i);
    ASSERT_TRUE(inserted);
    std::vector<int> res;
    ht.GetValue(i, &res);
    ASSERT_EQ(0, res.size());
  }
  ht.VerifyIntegrity();
  // ht.PrintHT();
  for (int i = 1000; i < 2000; i++) {
    bool inserted = ht.Insert(i, i);
    ASSERT_TRUE(inserted);
    std::vector<int> res;
    ht.GetValue(i, &res);
    ASSERT_EQ(1, res.size());
    ASSERT_EQ(i, res[0]);
  }
  ht.VerifyIntegrity();
  // ht.PrintHT();
  for (int i = 500; i < 1500; i++) {
    // std::cout << "remove: " << i << std::endl;
    bool inserted = ht.Remove(i);
    // ht.PrintHT();
    ASSERT_TRUE(inserted);
    std::vector<int> res;
    ht.GetValue(i, &res);
    ASSERT_EQ(0, res.size());
  }
  for (int i = 0; i < 1500; i++) {
    // std::cout << "remove: " << i << std::endl;
    bool inserted = ht.Remove(i);
    ASSERT_FALSE(inserted);
    std::vector<int> res;
    ht.GetValue(i, &res);
    ASSERT_EQ(0, res.size());
  }
  ht.VerifyIntegrity();
}

}  // namespace bustub
