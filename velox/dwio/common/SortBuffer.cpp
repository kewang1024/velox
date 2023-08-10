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

#include "velox/dwio/common/SortBuffer.h"
#include <vector/BaseVector.h>

namespace facebook::velox::dwio::common {

SortBuffer::SortBuffer(
    std::vector<TypePtr> keyTypes,
    std::vector<TypePtr> dependentTypes) {
  data_ = std::make_unique<exec::RowContainer>(
      keyTypes, dependentTypes, memoryPool_);
}

void SortBuffer::add(const VectorPtr& slice) {
  SelectivityVector allRows(slice->size());
  std::vector<char*> rows(slice->size());
  for (int row = 0; row < slice->size(); ++row) {
    rows[row] = data_->newRow();
  }
  for (const auto& columnProjection : columnMap_) {
    DecodedVector decoded(slice.get(columnProjection.outputChannel), allRows);
    for (int i = 0; i < slice->size(); ++i) {
      data_->store(decoded, i, rows[i], columnProjection.inputChannel);
    }
  }
}

VectorPtr SortBuffer::output() {
  RowVectorPtr output;
  for (const auto& columnProjection : columnMap_) {
    data_->extractColumn(
        returningRows_.data() + numRowsReturned_,
        output->size(),
        columnProjection.inputChannel,
        output->childAt(columnProjection.outputChannel));
  }
  return output;
}

} // namespace facebook::velox::dwio::common