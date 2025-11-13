#pragma once

#include <folly/logging/xlog.h>
#include <folly/lang/Aligned.h>

#include <folly/synchronization/DistributedMutex.h>

#include <atomic>

#include "cachelib/common/Mutex.h"

namespace facebook {
namespace cachelib {

class AtomicFIFOHashTable {
 public:
  using Mutex = folly::DistributedMutex;
  using LockHolder = std::unique_lock<Mutex>;

  AtomicFIFOHashTable() = default;

  explicit AtomicFIFOHashTable(uint32_t fifoSize) noexcept {
    fifoSize_ = ((fifoSize >> 3) + 1) << 3;
    numElem_ = fifoSize_ * loadFactorInv_;
    initHashtable();
  }

  ~AtomicFIFOHashTable() { hashTable_ = nullptr; }

  bool initialized() const noexcept { return hashTable_ != nullptr; }

  void initHashtable() noexcept {
    auto hashTable = std::unique_ptr<uint64_t[]>(new uint64_t[numElem_]);
    memset(hashTable.get(), 0, numElem_ * sizeof(uint64_t));
    hashTable_ = std::move(hashTable);

    // printf("create table fifoSize_ %zu numElem_ %zu\n", fifoSize_, numElem_);
  }

  void setFIFOSize(uint32_t fifoSize) noexcept {
    fifoSize_ = ((fifoSize >> 3) + 1) << 3;
    numElem_ = fifoSize_ * loadFactorInv_;
  }

  int getFIFOSize() const noexcept { return fifoSize_; }


    bool contains(uint32_t key) noexcept {
    // LockHolder l(*mtx_);
    uint32_t bucketIdx = getBucketIdx(key);
    int64_t currTime = numInserts_.load();
    uint64_t zero = 0;

    for (uint32_t i = 0; i < nItemPerBucket_; i++) {
        // uint64_t valInTable = hashTable_[bucketIdx + i];
        uint64_t valInTable = __atomic_load_n(&hashTable_[bucketIdx + i], __ATOMIC_RELAXED);  
        int64_t age = currTime - getInsertionTime(valInTable);
        if (valInTable == 0) {
        continue;
        }
        if (age > fifoSize_) {
        __atomic_compare_exchange_n(&hashTable_[bucketIdx + i], &valInTable, 0,
                                    true, __ATOMIC_RELAXED, __ATOMIC_RELAXED);
        continue;
        }
        if (matchKey(valInTable, key)) {
        __atomic_compare_exchange_n(&hashTable_[bucketIdx + i], &valInTable, 0,
                                    true, __ATOMIC_RELAXED, __ATOMIC_RELAXED);
        return true;
        }
    }
    return false;
    }

    void insert(uint32_t key) noexcept {
      // LockHolder l(*mtx_);
      int64_t currTime = numInserts_++;
      if (currTime > UINT32_MAX) {
        numInserts_ = 0;
        currTime = 0;
      }
      size_t bucketIdx = getBucketIdx(key);
      uint64_t hashTableVal = genHashtableVal(key, currTime);

      for (size_t i = 0; i < nItemPerBucket_; i++) {
        uint64_t valInTable =
            __atomic_load_n(&hashTable_[bucketIdx + i], __ATOMIC_RELAXED);
        if (valInTable == 0) {
          if (__atomic_compare_exchange_n(&hashTable_[bucketIdx + i],
                                          &valInTable, hashTableVal, true,
                                          __ATOMIC_RELAXED, __ATOMIC_RELAXED)) {
            return;
          }
        }
      }
      // we do not find an empty slots, random choose and overwrite one
      numEvicts_++;
      __atomic_store_n(&hashTable_[key % numElem_], hashTableVal,
                       __ATOMIC_RELAXED);
      // hashTable_[key % numElem_] = hashTableVal;
    }

    void resizeFIFO(uint32_t newFifoSize) noexcept {
      // Acquire lock if needed (or guarantee single-threaded access)
      LockHolder l(*mtx_);

      // Compute new sizes
      size_t newFifoSizeAligned = ((newFifoSize >> 3) + 1) << 3;
      size_t newNumElem = newFifoSizeAligned * loadFactorInv_;

      // Allocate new table
      auto newTable = std::unique_ptr<uint64_t[]>(new uint64_t[newNumElem]);
      memset(newTable.get(), 0, newNumElem * sizeof(uint64_t));

      // Move live entries
      if (hashTable_) {
        int64_t currTime = numInserts_.load();
        for (size_t i = 0; i < numElem_; i++) {
          uint64_t val = __atomic_load_n(&hashTable_[i], __ATOMIC_RELAXED);
          if (val == 0)
            continue;

          // Expire old ones
          int64_t age = currTime - getInsertionTime(val);
          if (age > fifoSize_)
            continue;

          uint32_t key = static_cast<uint32_t>(val & keyMask_);
          size_t newBucketIdx = (size_t)key % newNumElem;
          newBucketIdx = newBucketIdx & bucketIdxMask_;

          for (size_t j = 0; j < nItemPerBucket_; j++) {
            uint64_t slot =
                __atomic_load_n(&newTable[newBucketIdx + j], __ATOMIC_RELAXED);
            if (slot == 0) {
              __atomic_store_n(&newTable[newBucketIdx + j], val,
                               __ATOMIC_RELAXED);
              break;
            }
          }
        }
      }

      printf("Original FIFO table numElem=%zu fifoSize=%zu\n", numElem_, fifoSize_);

      // Swap in new table
      hashTable_ = std::move(newTable);
      fifoSize_ = newFifoSizeAligned;
      numElem_ = newNumElem;

      printf("Resized FIFO table to fifoSize=%zu numElem=%zu\n", fifoSize_, numElem_);
    }

 private:
  size_t getBucketIdx(uint32_t key) {
    size_t bucketIdx = (size_t)key % numElem_;
    bucketIdx = bucketIdx & bucketIdxMask_;
    return bucketIdx;
  }

  bool matchKey(uint64_t hashTableVal, uint32_t key) {
    return (hashTableVal & keyMask_) == key;
  }

  uint32_t getInsertionTime(uint64_t hashTableVal) {
    return static_cast<uint32_t>((hashTableVal & valueMask_) >> 32);
  }

  uint64_t genHashtableVal(uint32_t key, uint32_t time) {
    uint64_t uKey = static_cast<uint64_t>(key);
    uint64_t uTime = static_cast<uint64_t>(time);

    return uKey | (uTime << 32);
  }

  static constexpr size_t loadFactorInv_{2};
  static constexpr size_t nItemPerBucket_{8};
  static constexpr size_t bucketIdxMask_{0xFFFFFFFFFFFFFFF8};

  constexpr static uint64_t keyMask_ = 0x00000000FFFFFFFF;
  constexpr static uint64_t valueMask_ = 0xFFFFFFFF00000000;

  size_t numElem_{0};
  // curr time - insert time > FIFO size => not valid
  size_t fifoSize_{0};
  std::atomic<int64_t> numInserts_{0};
  std::atomic<int64_t> numEvicts_{0};
  alignas(64) std::unique_ptr<uint64_t[]> hashTable_{nullptr};
  mutable folly::cacheline_aligned<Mutex> mtx_;
};

} // namespace cachelib
} // namespace facebook

// #include "cachelib/allocator/datastruct/AtomicFIFOHashTable-inl.h"
