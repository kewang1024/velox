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

#include <vector/BaseVector.h>
#include "velox/exec/ContainerRowSerde.h"
#include "velox/exec/RowContainer.h"

namespace facebook::velox::dwio::common {

/**
 * Sorting.
 */
class SortBuffer {
 public:
  SortBuffer(
      std::vector<TypePtr> keyTypes,
      std::vector<TypePtr> dependentTypes);

  void add(const VectorPtr& slice);

  VectorPtr output();

  // The map from column channel in 'output_' to the corresponding one stored in
  // 'data_'. The column channel might be reordered to ensure the sorting key
  // columns stored first in 'data_'.
  std::vector<IdentityProjection> columnMap_;
  std::unique_ptr<exec::RowContainer> data_;
  velox::memory::MemoryPool* memoryPool_;
};

} // namespace facebook::velox::dwio::common