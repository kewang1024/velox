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
#include "velox/exec/BroadcastOutputBuffer.h"
#include "velox/exec/Task.h"

namespace facebook::velox::exec {

void BroadcastOutputBuffer::updateOutputBuffers(int numBuffers, bool noMoreBuffers) {
  std::vector<ContinuePromise> promises;
  bool isFinished;
  {
    std::lock_guard<std::mutex> l(mutex_);

    if (numBuffers > buffers_.size()) {
      addOutputBuffersLocked(numBuffers);
    }

    if (!noMoreBuffers) {
      return;
    }

    noMoreBuffers_ = true;
    isFinished = isFinishedLocked();
    updateAfterAcknowledgeLocked(dataToBroadcast_, promises);
  }

  releaseAfterAcknowledge(dataToBroadcast_, promises);
  if (isFinished) {
    task_->setAllOutputConsumed();
  }
}

void BroadcastOutputBuffer::updateNumDrivers(uint32_t newNumDrivers) {
  bool isNoMoreDrivers{false};
  {
    std::lock_guard<std::mutex> l(mutex_);
    numDrivers_ = newNumDrivers;
    // If we finished all drivers, ensure we register that we are 'done'.
    if (numDrivers_ == numDriversFinished_) {
      isNoMoreDrivers = true;
    }
  }
  if (isNoMoreDrivers) {
    noMoreDrivers();
  }
}

bool BroadcastOutputBuffer::enqueue(
    int destination,
    std::unique_ptr<SerializedPage> data,
    ContinueFuture* future) {
  VELOX_CHECK_NOT_NULL(data);
  VELOX_CHECK(
      task_->isRunning(), "Task is terminated, cannot add data to output.");
  std::vector<DataAvailable> dataAvailableCallbacks;
  bool blocked = false;
  {
    std::lock_guard<std::mutex> l(mutex_);
    VELOX_CHECK_LT(destination, buffers_.size());

    totalSize_ += data->size();
    VELOX_CHECK_EQ(destination, 0, "Bad destination {}", destination);
    enqueueBroadcastOutputLocked(std::move(data), dataAvailableCallbacks);

    if (totalSize_ > maxSize_ && future) {
      promises_.emplace_back("OutputBuffer::enqueue");
      *future = promises_.back().getSemiFuture();
      blocked = true;
    }
  }

  // Outside mutex_.
  for (auto& callback : dataAvailableCallbacks) {
    callback.notify();
  }

  return blocked;
}

void BroadcastOutputBuffer::noMoreData() {

}

void BroadcastOutputBuffer::noMoreDrivers() {

}

bool BroadcastOutputBuffer::isFinished() {
  std::lock_guard<std::mutex> l(mutex_);
  return isFinishedLocked();
}

void BroadcastOutputBuffer::getData(
    int destination,
    uint64_t maxSize,
    int64_t sequence,
    DataAvailableCallback notify) {
  std::vector<std::unique_ptr<folly::IOBuf>> data;
  std::vector<std::shared_ptr<SerializedPage>> freed;
  std::vector<ContinuePromise> promises;
  {
    std::lock_guard<std::mutex> l(mutex_);

    if (destination >= buffers_.size()) {
      addOutputBuffersLocked(destination + 1);
    }

    VELOX_CHECK_LT(destination, buffers_.size());
    auto* buffer = buffers_[destination].get();
    VELOX_CHECK_NOT_NULL(
        buffer,
        "getData received after its buffer is deleted. Destination: {}, sequence: {}",
        destination,
        sequence);
    freed = buffer->acknowledge(sequence, true);
    updateAfterAcknowledgeLocked(freed, promises);
    data = buffer->getData(maxSize, sequence, notify, nullptr);
  }
  releaseAfterAcknowledge(freed, promises);
  if (!data.empty()) {
    notify(std::move(data), sequence);
  }
}

void BroadcastOutputBuffer::acknowledge(int destination, int64_t sequence) {
  std::vector<std::shared_ptr<SerializedPage>> freed;
  std::vector<ContinuePromise> promises;
  {
    std::lock_guard<std::mutex> l(mutex_);
    VELOX_CHECK_LT(destination, buffers_.size());
    auto* buffer = buffers_[destination].get();
    if (!buffer) {
      VLOG(1) << "Ack received after final ack for destination " << destination
              << " and sequence " << sequence;
      return;
    }
    freed = buffer->acknowledge(sequence, false);
    updateAfterAcknowledgeLocked(freed, promises);
  }
  releaseAfterAcknowledge(freed, promises);
}

bool BroadcastOutputBuffer::deleteResults(int destination) {
  std::vector<std::shared_ptr<SerializedPage>> freed;
  std::vector<ContinuePromise> promises;
  bool isFinished;
  DataAvailable dataAvailable;
  {
    std::lock_guard<std::mutex> l(mutex_);
    VELOX_CHECK_LT(destination, buffers_.size());
    auto* buffer = buffers_[destination].get();
    if (buffer == nullptr) {
      VLOG(1) << "Extra delete received for destination " << destination;
      return false;
    }
    freed = buffer->deleteResults();
    dataAvailable = buffer->getAndClearNotify();
    buffers_[destination] = nullptr;
    ++numFinalAcknowledges_;
    isFinished = isFinishedLocked();
    updateAfterAcknowledgeLocked(freed, promises);
  }

  // Outside of mutex.
  dataAvailable.notify();

  if (!promises.empty()) {
    VLOG(1) << "Delete of results unblocks producers. Can happen in early end "
            << "due to error or limit";
  }
  releaseAfterAcknowledge(freed, promises);
  if (isFinished) {
    task_->setAllOutputConsumed();
  }
  return isFinished;
}

void BroadcastOutputBuffer::terminate() {
  VELOX_CHECK(!task_->isRunning());

  std::vector<ContinuePromise> outstandingPromises;
  {
    std::lock_guard<std::mutex> l(mutex_);
    outstandingPromises.swap(promises_);
  }
  for (auto& promise : outstandingPromises) {
    promise.setValue();
  }
}

std::string BroadcastOutputBuffer::toString() {
  std::lock_guard<std::mutex> l(mutex_);
  std::stringstream out;
  out << "[BroadcastOutputBuffer totalSize_=" << totalSize_
      << "b, num producers blocked=" << promises_.size()
      << ", completed=" << numDriversFinished_ << "/" << numDrivers_ << ", "
      << (atEnd_ ? "at end, " : "") << "destinations: " << std::endl;
  for (auto i = 0; i < buffers_.size(); ++i) {
    auto buffer = buffers_[i].get();
    out << i << ": " << (buffer ? buffer->toString() : "none") << std::endl;
  }
  out << "]" << std::endl;
  return out.str();
}

void BroadcastOutputBuffer::addOutputBuffersLocked(int numBuffers) {
  VELOX_CHECK(!noMoreBuffers_);
  buffers_.reserve(numBuffers);
  for (int32_t i = buffers_.size(); i < numBuffers; ++i) {
    auto buffer = std::make_unique<DestinationBuffer>();
    for (const auto& data : dataToBroadcast_) {
      buffer->enqueue(data);
    }
    if (atEnd_) {
      buffer->enqueue(nullptr);
    }
    buffers_.emplace_back(std::move(buffer));
  }
}

void BroadcastOutputBuffer::updateAfterAcknowledgeLocked(
    const std::vector<std::shared_ptr<SerializedPage>>& freed,
    std::vector<ContinuePromise>& promises) {
  uint64_t totalFreed = 0;
  for (const auto& free : freed) {
    if (free.unique()) {
      totalFreed += free->size();
    }
  }
  if (totalFreed == 0) {
    return;
  }

  VELOX_CHECK_LE(
      totalFreed,
      totalSize_,
      "Output buffer size goes negative: released {} over {}",
      totalFreed,
      totalSize_);
  totalSize_ -= totalFreed;
  VELOX_CHECK_GE(totalSize_, 0);
  if (totalSize_ < continueSize_) {
    promises = std::move(promises_);
  }
}

void BroadcastOutputBuffer::enqueueBroadcastOutputLocked(
    std::unique_ptr<SerializedPage> data,
    std::vector<DataAvailable>& dataAvailableCbs) {
  VELOX_DCHECK(dataAvailableCbs.empty());

  std::shared_ptr<SerializedPage> sharedData(data.release());
  for (auto& buffer : buffers_) {
    if (buffer != nullptr) {
      buffer->enqueue(sharedData);
      dataAvailableCbs.emplace_back(buffer->getAndClearNotify());
    }
  }

  // NOTE: we don't need to add new buffer to 'dataToBroadcast_' if there is no
  // more output buffers.
  if (!noMoreBuffers_) {
    dataToBroadcast_.emplace_back(sharedData);
  }
}

bool OutputBuffer::isFinishedLocked() {
  // NOTE: for broadcast output buffer, we can only mark it as finished after
  // receiving the no more (destination) buffers signal.
  if (isBroadcast() && !noMoreBuffers_) {
    return false;
  }
  for (auto& buffer : buffers_) {
    if (buffer != nullptr) {
      return false;
    }
  }
  return true;
}
}
