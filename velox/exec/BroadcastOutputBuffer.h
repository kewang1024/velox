/*
 * Copyright (c) Facebook, Inc. and its affiliates.
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

#include "velox/core/PlanNode.h"
#include "velox/exec/OutputBuffer.h"

namespace facebook::velox::exec {

/// The class is used to buffer the output in broadcast fashion.
class BroadcastOutputBuffer : OutputBuffer {
 public:
  BroadcastOutputBuffer(
      std::shared_ptr<Task> task,
      int numDestinations,
      uint32_t numDrivers);

  void updateOutputBuffers(int numBuffers, bool noMoreBuffers);

  void updateNumDrivers(uint32_t newNumDrivers);

  bool enqueue(
      int destination,
      std::unique_ptr<SerializedPage> data,
      ContinueFuture* future);

  void noMoreData();

  void noMoreDrivers();

  bool isFinished();

  bool isFinishedLocked();

  void acknowledge(int destination, int64_t sequence);

  bool deleteResults(int destination);

  void getData(
      int destination,
      uint64_t maxSize,
      int64_t sequence,
      DataAvailableCallback notify);

  void terminate();

  std::string toString();

  double getUtilization() const {
    return totalSize_ / (double)maxSize_;
  };

  bool isOverutilized() const {
    return totalSize_ / (double)maxSize_;
  };

 private:
  void addOutputBuffersLocked(int numBuffers);
  void updateAfterAcknowledgeLocked(
      const std::vector<std::shared_ptr<SerializedPage>>& freed,
      std::vector<ContinuePromise>& promises);
  void enqueueBroadcastOutputLocked(
      std::unique_ptr<SerializedPage> data,
      std::vector<DataAvailable>& dataAvailableCbs);
  bool isFinishedLocked();

  const std::shared_ptr<Task> task_;
  /// If 'totalSize_' > 'maxSize_', each producer is blocked after adding
  /// data.
  const uint64_t maxSize_;
  /// When 'totalSize_' goes below 'continueSize_', blocked producers are
  /// resumed.
  const uint64_t continueSize_;

  std::mutex mutex_;
  // One buffer per destination.
  std::vector<std::unique_ptr<DestinationBuffer>> buffers_;
  // Current data size in 'buffers_'.
  uint64_t totalSize_ = 0;

  // Total number of drivers expected to produce results. This number will
  // decrease in the end of grouped execution, when we understand the real
  // number of producer drivers (depending on the number of split groups).
  uint32_t numDrivers_{0};
  uint32_t numDriversFinished_{0};
  std::vector<ContinuePromise> promises_;

  // If true, then we don't allow to add new destination buffers. This only
  // applies for non-partitioned output buffer type.
  bool noMoreBuffers_{false};
  // when noMoreBuffers_ is false, stores the enqueued data so that they can be
  // broadcast to later created destinations. Cleared after receiving
  // no-more-broadcast-buffers signal.
  std::vector<std::shared_ptr<SerializedPage>> dataToBroadcast_;

  int numFinalAcknowledges_ = 0;
  bool atEnd_ = false;
};

} // namespace facebook::velox::exec