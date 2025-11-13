/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#pragma once

#include <folly/MPMCQueue.h>
#include <folly/logging/xlog.h>

#include <algorithm>
#include <atomic>
#include <thread>

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wconversion"
#include "cachelib/allocator/serialize/gen-cpp2/objects_types.h"
#pragma GCC diagnostic pop

#include <folly/lang/Aligned.h>
#include <folly/synchronization/DistributedMutex.h>

#include "cachelib/allocator/datastruct/AtomicFIFOHashTable.h"
#include "cachelib/allocator/datastruct/DList.h"
#include "cachelib/common/BloomFilter.h"
#include "cachelib/common/CompilerUtils.h"
#include "cachelib/common/Mutex.h"

namespace facebook {
namespace cachelib {

template <typename T, DListHook<T> T::* HookPtr>
class S3FIFOList {
 public:
  using Mutex = folly::DistributedMutex;
  using LockHolder = std::unique_lock<Mutex>;
  using CompressedPtr = typename T::CompressedPtr;
  using PtrCompressor = typename T::PtrCompressor;
  using ADList = DList<T, HookPtr>;
  using RefFlags = typename T::Flags;
  using S3FIFOListObject = serialization::S3FIFOListObject;

  int tailSize{0};

  S3FIFOList() = default;
  S3FIFOList(const S3FIFOList&) = delete;
  S3FIFOList& operator=(const S3FIFOList&) = delete;
  ~S3FIFOList() {
    stop_ = true;
    // evThread_->join();
  }

  S3FIFOList(PtrCompressor compressor) noexcept {
    pfifo_ = std::make_unique<ADList>(compressor);
    mfifo_ = std::make_unique<ADList>(compressor);
  }

  void setTailSize(int size) noexcept { tailSize = size; }

  // Restore S3FIFOList from saved state.
  //
  // @param object              Save S3FIFOList object
  // @param compressor          PtrCompressor object
  S3FIFOList(const S3FIFOListObject& object, PtrCompressor compressor) {
    pfifo_ = std::make_unique<ADList>(*object.pfifo(), compressor);
    mfifo_ = std::make_unique<ADList>(*object.mfifo(), compressor);
  }

  /**
   * Exports the current state as a thrift object for later restoration.
   */
  S3FIFOListObject saveState() const {
    S3FIFOListObject state;
    *state.pfifo() = pfifo_->saveState();
    *state.mfifo() = mfifo_->saveState();
    return state;
  }

  ADList& getListProbationary() const noexcept { return *pfifo_; }

  ADList& getListMain() const noexcept { return *mfifo_; }

  // T* getTail() const noexcept { return pfifo_->getTail(); }

  size_t size() const noexcept { return pfifo_->size() + mfifo_->size(); }

  T* getEvictionCandidate() noexcept;

  void add(T& node) noexcept {
    LockHolder l(*mtx_);
    if (hist_.initialized() && hist_.contains(hashNode(node))) {
      markMain(node);
      unmarkProbationary(node);
      mfifo_->linkAtHead(node);
    } else {
      markProbationary(node);
      unmarkMain(node);
      pfifo_->linkAtHead(node);
    }
  }

  // Bit MM_BIT_1 is used to record if the item has been accessed since
  // being written in cache. Unaccessed items are ignored when determining
  // projected update time.
  void markAccessed(T& node) noexcept {
    node.template setFlag<RefFlags::kMMFlag1>();
  }

  void unmarkAccessed(T& node) noexcept {
    node.template unSetFlag<RefFlags::kMMFlag1>();
  }

  static bool isAccessed(const T& node) {
    return node.template isFlagSet<RefFlags::kMMFlag1>();
  }

  // Bit MM_BIT_0 is used to record if the item is hot.
  void markProbationary(T& node) noexcept {
    node.template setFlag<RefFlags::kMMFlag0>();
  }

  void unmarkProbationary(T& node) noexcept {
    node.template unSetFlag<RefFlags::kMMFlag0>();
  }

  bool isProbationary(const T& node) const noexcept {
    return node.template isFlagSet<RefFlags::kMMFlag0>();
  }

  // Bit MM_BIT_2 is used to record if the item is cold.
  void markMain(T& node) noexcept {
    node.template setFlag<RefFlags::kMMFlag2>();
  }

  void unmarkMain(T& node) noexcept {
    node.template unSetFlag<RefFlags::kMMFlag2>();
  }

  bool isMain(const T& node) const noexcept {
    return node.template isFlagSet<RefFlags::kMMFlag2>();
  }

  void remove(T& node) {
    LockHolder l(*mtx_);
    if (isMain(node)) {
      mfifo_->remove(node);
    } else {
      if (hist_.initialized()) {
        hist_.insert(hashNode(node));
      }
      pfifo_->remove(node);
    }
  }

  struct CandidateRef {
    T* node;
    bool fromProb;
  };

  CandidateRef getEvictionCandidateRef() noexcept {
    LockHolder l(*mtx_);
    CandidateRef ref{nullptr, false}; // default invalid return

    // total size
    size_t listSize = pfifo_->size() + mfifo_->size();
    if (listSize == 0) {
      return ref; // no candidate
    }

    // ensure history initialized once
    // Expectation: Eviction only when start to be full.
    // OR when slab rebalance is triggered.
    int fifoSize = listSize / 2;
    if (!hist_.initialized()) {
      printf("Initializing history table, listSize=%zu\n", listSize);
      hist_.setFIFOSize(fifoSize);
      hist_.initHashtable();
    }

    if (std::abs(fifoSize - hist_.getFIFOSize()) >= tailSize) {
      hist_.resizeFIFO(fifoSize);
    }

    while (true) {
      bool usePFifo =
          pfifo_->size() >
          static_cast<double>(pfifo_->size() + mfifo_->size()) * pRatio_;

      T* curr = usePFifo ? pfifo_->getTail() : mfifo_->getTail();

      if (curr == nullptr) {
        // sanity check
        if (usePFifo && pfifo_->size() != 0) {
          printf("pfifo_->size() = %zu\n", pfifo_->size());
          exit(1);
        } else if (!usePFifo && mfifo_->size() != 0) {
          printf("mfifo_->size() = %zu\n", mfifo_->size());
          exit(1);
        }
        continue;
      }

      // check access state
      if (usePFifo) {
        if (isAccessed(*curr)) {
          unmarkAccessed(*curr);
          unmarkProbationary(*curr);
          markMain(*curr);

          pfifo_->remove(*curr);
          mfifo_->linkAtHead(*curr);
          continue; // scan again
        } else {
          ref = {curr, true};
          break;
        }
      } else { // using mfifo
        if (isAccessed(*curr)) {
          unmarkAccessed(*curr);
          mfifo_->remove(*curr);
          mfifo_->linkAtHead(*curr);
          continue;
        } else {
          ref = {curr, false};
          break;
        }
      }
    }

    return ref;
  }

 private:
  static uint32_t hashNode(const T& node) noexcept {
    return static_cast<uint32_t>(
        folly::hasher<folly::StringPiece>()(node.getKey()));
  }

  std::unique_ptr<ADList> pfifo_;

  std::unique_ptr<ADList> mfifo_;

  std::unique_ptr<ADList[]> pfifoSublists_;
  std::unique_ptr<ADList[]> mfifoSublists_;

  mutable folly::cacheline_aligned<Mutex> mtx_;

  constexpr static double pRatio_ = 0.05;

  AtomicFIFOHashTable hist_;

  constexpr static size_t nMaxEvictionCandidates_ = 64;

  folly::MPMCQueue<T*> evictCandidateQueue_{nMaxEvictionCandidates_};

  std::unique_ptr<std::thread> evThread_{nullptr};

  std::atomic<bool> stop_{false};
};
} // namespace cachelib
} // namespace facebook

// #include "cachelib/allocator/datastruct/S3FIFOList-inl.h"
