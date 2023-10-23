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

/// The class is used to buffer the output in arbitrary fashion.
class ArbitraryOutputBuffer : OutputBuffer {
 public:
  ArbitraryOutputBuffer(
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

  double getUtilization() const;

  bool isOverutilized() const;
};

} // namespace facebook::velox::exec