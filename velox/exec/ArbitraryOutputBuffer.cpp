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
#include "velox/exec/ArbitraryOutputBuffer.h"

namespace facebook::velox::exec {

void ArbitraryOutputBuffer::updateOutputBuffers(int numBuffers, bool noMoreBuffers) {

}

void ArbitraryOutputBuffer::updateNumDrivers(uint32_t newNumDrivers) {

}

bool ArbitraryOutputBuffer::enqueue(
    int destination,
    std::unique_ptr<SerializedPage> data,
    ContinueFuture* future) {

}

void ArbitraryOutputBuffer::noMoreData() {

}

void ArbitraryOutputBuffer::noMoreDrivers() {

}

bool ArbitraryOutputBuffer::isFinished() {

}

bool ArbitraryOutputBuffer::isFinishedLocked() {

}

void ArbitraryOutputBuffer::acknowledge(int destination, int64_t sequence) {

}

// Deletes all data for 'destination'. Returns true if all
// destinations are deleted, meaning that the buffer is fully
// consumed and the producer can be marked finished and the buffers
// freed.
bool ArbitraryOutputBuffer::deleteResults(int destination) {

}

void ArbitraryOutputBuffer::getData(
    int destination,
    uint64_t maxSize,
    int64_t sequence,
    DataAvailableCallback notify) {

}

// Continues any possibly waiting producers. Called when the
// producer task has an error or cancellation.
void ArbitraryOutputBuffer::terminate() {

}

std::string ArbitraryOutputBuffer::toString() {

}

// Gets the memory utilization ratio in this output buffer.
double ArbitraryOutputBuffer::getUtilization() const {

}

// Indicates if this output buffer is over-utilized and thus blocks its
// producers.
bool ArbitraryOutputBuffer::isOverutilized() const {

}

}
