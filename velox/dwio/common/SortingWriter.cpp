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

#include "velox/dwio/common/SortingWriter.h"

namespace facebook::velox::dwio::common {

SortingWriter::SortingWriter(std::shared_ptr<memory::MemoryPool> pool) {}

void SortingWriter::write(const VectorPtr& slice) {
  sortBuffer_.add(slice);
}

void SortingWriter::flush() {}

void SortingWriter::close() {
  outputWriter_->write(sortBuffer_->output());
  outputWriter_->close();
}

void SortingWriter::abort() {
  outputWriter_->abort();
}

} // namespace facebook::velox::dwio::common
