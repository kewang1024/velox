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

#include "velox/dwio/common/Writer.h"
#include "velox/dwio/common/SortBuffer.h"

namespace facebook::velox::dwio::common {

/**
 * Sorting writer class.
 *
 * Sorting Writer object is used to write sorted data into a single file.
 *
 * Writer objects are created through factories implementing
 * WriterFactory interface.
 */
class SortingWriter : public Writer {

 public:
  SortingWriter(std::shared_ptr<memory::MemoryPool> pool);

  virtual void write(const VectorPtr& slice) override;

  virtual void flush() override;

  virtual void close() override;

  virtual void abort() override;

  const std::unique_ptr<Writer> outputWriter_;
  const SortBuffer sortBuffer_;
};

} // namespace facebook::velox::dwio::common