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
#include "velox/exec/fuzzer/WriterFuzzer.h"

#include <boost/random/uniform_int_distribution.hpp>

#include "velox/dwio/dwrf/reader/DwrfReader.h"

#include "velox/exec/tests/utils/PlanBuilder.h"
#include "velox/exec/tests/utils/TempDirectoryPath.h"

#include <unordered_set>
#include "velox/common/base/Fs.h"
#include "velox/common/encode/Base64.h"
#include "velox/common/file/FileSystems.h"
#include "velox/exec/fuzzer/DuckQueryRunner.h"
#include "velox/exec/fuzzer/PrestoQueryRunner.h"
#include "velox/exec/tests/utils/AssertQueryBuilder.h"
#include "velox/expression/fuzzer/FuzzerToolkit.h"
#include "velox/vector/VectorSaver.h"
#include "velox/vector/fuzzer/VectorFuzzer.h"

DEFINE_int32(steps, 10, "Number of plans to generate and test.");

DEFINE_int32(
    duration_sec,
    0,
    "For how long it should run (in seconds). If zero, "
    "it executes exactly --steps iterations and exits.");

DEFINE_int32(
    batch_size,
    100,
    "The number of elements on each generated vector.");

DEFINE_int32(num_batches, 10, "The number of generated vectors.");

DEFINE_double(
    null_ratio,
    0.1,
    "Chance of adding a null value in a vector "
    "(expressed as double from 0 to 1).");

namespace facebook::velox::exec::test {

namespace {

class WriterFuzzer {
 public:
  WriterFuzzer(
      size_t seed,
      std::unique_ptr<ReferenceQueryRunner> referenceQueryRunner);

  void go();

 private:
  static VectorFuzzer::Options getFuzzerOptions() {
    VectorFuzzer::Options opts;
    opts.vectorSize = FLAGS_batch_size;
    opts.stringLength = 10;
    opts.nullRatio = FLAGS_null_ratio;
    return opts;
  }

  void seed(size_t seed) {
    currentSeed_ = seed;
    vectorFuzzer_.reSeed(seed);
    rng_.seed(currentSeed_);
  }

  void reSeed() {
    seed(rng_());
  }

  // Generates at least one and up to 5 scalar columns to be used as regular
  // columns of table write, Column names are generated using template
  // '<prefix>N', where N is zero-based ordinal number of the column.
  std::vector<std::string> generateColumns(
      const std::string& prefix,
      std::vector<std::string>& names,
      std::vector<TypePtr>& types);

  // Generates at least one and up to 3 scalar columns to be used as partition
  // columns of table write, Column names are generated using template
  // '<prefix>N', where N is zero-based ordinal number of the column.
  std::vector<std::string> generatePartitionKeys(
      const std::string& prefix,
      std::vector<std::string>& names,
      std::vector<TypePtr>& types);

  // Generates input data for table write.
  std::vector<RowVectorPtr> generateInputData(
      std::vector<std::string> names,
      std::vector<TypePtr> types,
      size_t partitionOffset);

  void verifyWriter(
      const std::vector<RowVectorPtr>& input,
      const std::vector<std::string>& partitionKeys,
      const std::string& outputDirectoryPath);

  // Executes velox query plan and returns the result.
  RowVectorPtr execute(
      const core::PlanNodePtr& plan,
      int32_t maxDrivers = 2);

  RowVectorPtr veloxToPrestoResult(const RowVectorPtr& result);

  // Query Presto to find out table's location on disk.
  std::string getReferenceOutputDirectoryPath(int32_t layers);

  // Compares if two directories have same partitions
  bool comparePartitions(
      const std::string& outputDirectoryPath,
      const std::string& referenceOutputDirectoryPath);

  // Returns all the partition names in outputDirectoryPath.
  std::set<std::string> getPartitionNames(
      const std::string& outputDirectoryPath);

  FuzzerGenerator rng_;
  size_t currentSeed_{0};
  std::unique_ptr<ReferenceQueryRunner> referenceQueryRunner_;
  std::shared_ptr<memory::MemoryPool> rootPool_{
      memory::memoryManager()->addRootPool()};
  std::shared_ptr<memory::MemoryPool> pool_{rootPool_->addLeafChild("leaf")};
  VectorFuzzer vectorFuzzer_;
};
} // namespace

void writerFuzzer(
    size_t seed,
    std::unique_ptr<ReferenceQueryRunner> referenceQueryRunner) {
  auto writerFuzzer = WriterFuzzer(seed, std::move(referenceQueryRunner));
  writerFuzzer.go();
}

namespace {
template <typename T>
bool isDone(size_t i, T startTime) {
  if (FLAGS_duration_sec > 0) {
    std::chrono::duration<double> elapsed =
        std::chrono::system_clock::now() - startTime;
    return elapsed.count() >= FLAGS_duration_sec;
  }
  return i >= FLAGS_steps;
}

WriterFuzzer::WriterFuzzer(
    size_t initialSeed,
    std::unique_ptr<ReferenceQueryRunner> referenceQueryRunner)
    : referenceQueryRunner_{std::move(referenceQueryRunner)},
      vectorFuzzer_{getFuzzerOptions(), pool_.get()} {
  seed(initialSeed);
}

void WriterFuzzer::go() {
  VELOX_CHECK(
      FLAGS_steps > 0 || FLAGS_duration_sec > 0,
      "Either --steps or --duration_sec needs to be greater than zero.")

  auto startTime = std::chrono::system_clock::now();
  size_t iteration = 0;

  while (!isDone(iteration, startTime)) {
    LOG(INFO) << "==============================> Started iteration "
              << iteration << " (seed: " << currentSeed_ << ")";

    std::vector<std::string> names;
    std::vector<TypePtr> types;
    std::vector<std::string> partitionKeys;

    generateColumns("c", names, types);
    const auto partitionOffset = names.size();
    // 50% of times test partitioned write.
    if (vectorFuzzer_.coinToss(0.5)) {
      partitionKeys = generatePartitionKeys("p", names, types);
    }
    auto input = generateInputData(names, types, partitionOffset);

    auto tempDirPath = exec::test::TempDirectoryPath::create();
    verifyWriter(input, partitionKeys, tempDirPath->getPath());

    LOG(INFO) << "==============================> Done with iteration "
              << iteration++;
    reSeed();
  }
}

std::vector<std::string> WriterFuzzer::generateColumns(
    const std::string& prefix,
    std::vector<std::string>& names,
    std::vector<TypePtr>& types) {
  static const std::vector<TypePtr> kNonFloatingPointTypes{
      BOOLEAN(),
      TINYINT(),
      SMALLINT(),
      INTEGER(),
      BIGINT(),
      VARCHAR(),
      VARBINARY(),
      TIMESTAMP(),
  };

  const auto numColumns =
      boost::random::uniform_int_distribution<uint32_t>(1, 5)(rng_);
  std::vector<std::string> columns;
  for (auto i = 0; i < numColumns; ++i) {
    columns.push_back(fmt::format("{}{}", prefix, i));

    // Pick random, possibly complex, type.
    types.push_back(vectorFuzzer_.randType(kNonFloatingPointTypes, 2));
    names.push_back(columns.back());
  }
  return columns;
}

std::vector<std::string> WriterFuzzer::generatePartitionKeys(
    const std::string& prefix,
    std::vector<std::string>& names,
    std::vector<TypePtr>& types) {
  // Supported partition key column types
  // According to VectorHasher::typeKindSupportsValueIds and
  // https://github.com/prestodb/presto/blob/master/presto-hive/src/main/java/com/facebook/presto/hive/HiveUtil.java#L575
  static const std::vector<TypePtr> kSupportedKeyTypes{
      BOOLEAN(), TINYINT(), SMALLINT(), INTEGER(), BIGINT(), VARCHAR()};

  auto numKeys = boost::random::uniform_int_distribution<uint32_t>(1, 3)(rng_);
  std::vector<std::string> partitionKeys;
  for (auto i = 0; i < numKeys; ++i) {
    partitionKeys.push_back(fmt::format("{}{}", prefix, i));

    // Pick random scalar type.
    types.push_back(vectorFuzzer_.randType(kSupportedKeyTypes, 1));
    names.push_back(partitionKeys.back());
  }
  return partitionKeys;
}

std::vector<RowVectorPtr> WriterFuzzer::generateInputData(
    std::vector<std::string> names,
    std::vector<TypePtr> types,
    size_t partitionOffset) {
  const auto size = vectorFuzzer_.getOptions().vectorSize;
  auto inputType = ROW(std::move(names), std::move(types));
  std::vector<RowVectorPtr> input;

  // For partition keys, limit the distinct value to 4 to avoid exceeding
  // partition number limit of 100. Since we could have up to 3 partition
  // keys, it would generate up to 64 partitions.
  std::vector<VectorPtr> partitionValues;
  for (auto i = partitionOffset; i < inputType->size(); ++i) {
    partitionValues.push_back(vectorFuzzer_.fuzz(inputType->childAt(i), 4));
  }

  for (auto i = 0; i < FLAGS_num_batches; ++i) {
    std::vector<VectorPtr> children;
    for (auto j = children.size(); j < inputType->size(); ++j) {
      if (j < partitionOffset) {
        // For regular columns
        children.push_back(vectorFuzzer_.fuzz(inputType->childAt(j), size));
      } else {
        // TODO Add other encoding support here besides DictionaryVector.
        children.push_back(vectorFuzzer_.fuzzDictionary(
            partitionValues.at(j - partitionOffset), size));
      }
    }
    input.push_back(std::make_shared<RowVector>(
        pool_.get(), inputType, nullptr, size, std::move(children)));
  }

  return input;
}

void WriterFuzzer::verifyWriter(
    const std::vector<RowVectorPtr>& input,
    const std::vector<std::string>& partitionKeys,
    const std::string& outputDirectoryPath) {
  auto plan = PlanBuilder()
                  .values(input)
                  .tableWrite(outputDirectoryPath, partitionKeys)
                  .planNode();

  auto maxDrivers =
      boost::random::uniform_int_distribution<int32_t>(1, 16)(rng_);
  auto result = execute(plan, maxDrivers);

  const auto dropSql = "DROP TABLE IF EXISTS tmp_write";
  const auto sql = referenceQueryRunner_->toSql(plan).value();
  referenceQueryRunner_->execute(dropSql);
  std::multiset<std::vector<variant>> expectedResult;
  try {
    expectedResult =
        referenceQueryRunner_->execute(sql, input, plan->outputType());
  } catch (...) {
    LOG(WARNING) << "Query failed in the reference DB";
    return;
  }

  // 1. Verify inserted number of rows.
  auto insertedRows = veloxToPrestoResult(result);
  // Presto sql only produces one row which is how many rows are inserted.
  VELOX_CHECK_EQ(
      expectedResult.size(),
      1,
      "Query returned unexpected result in the reference DB");
  VELOX_CHECK(
      assertEqualResults(expectedResult, plan->outputType(), {insertedRows}),
      "Velox and reference DB results don't match");

  // 2. Verify directory layout.
  auto referencedOutputDirectoryPath =
      getReferenceOutputDirectoryPath(partitionKeys.size());
  comparePartitions(outputDirectoryPath, referencedOutputDirectoryPath);

  LOG(INFO) << "Verified results against reference DB";
}

RowVectorPtr WriterFuzzer::execute(
    const core::PlanNodePtr& plan,
    int32_t maxDrivers) {
  LOG(INFO) << "Executing query plan: " << std::endl
            << plan->toString(true, true);
  fuzzer::ResultOrError resultOrError;
  AssertQueryBuilder builder(plan);
  return builder.maxDrivers(maxDrivers).copyResults(pool_.get());
}

RowVectorPtr WriterFuzzer::veloxToPrestoResult(const RowVectorPtr& result) {
  // Velox TableWrite node produces results of following layout
  // row     fragments     context
  // X         null          X
  // null       X            X
  // null       X            X
  // Extract inserted rows from velox execution result.
  std::vector<VectorPtr> insertedRows = {result->childAt(0)->slice(0, 1)};
  return std::make_shared<RowVector>(
      pool_.get(),
      ROW({"count"}, {insertedRows[0]->type()}),
      nullptr,
      1,
      insertedRows);
}

std::string WriterFuzzer::getReferenceOutputDirectoryPath(int32_t layers) {
  auto filePath =
      referenceQueryRunner_->execute("SELECT \"$path\" FROM tmp_write");
  auto tableDirectoryPath =
      fs::path(extractSingleValue<StringView>(filePath)).parent_path();
  while (layers-- > 0) {
    tableDirectoryPath = tableDirectoryPath.parent_path();
  }
  return tableDirectoryPath.string();
}

bool WriterFuzzer::comparePartitions(
    const std::string& outputDirectoryPath,
    const std::string& referenceOutputDirectoryPath) {
  const auto partitions = getPartitionNames(outputDirectoryPath);
  LOG(INFO) << "Velox written partitions:" << std::endl;
  for (std::string p : partitions) {
    LOG(INFO) << p << std::endl;
  }

  const auto referencedPartitions =
      getPartitionNames(referenceOutputDirectoryPath);
  LOG(INFO) << "Presto written partitions:" << std::endl;
  for (std::string p : referencedPartitions) {
    LOG(INFO) << p << std::endl;
  }

  VELOX_CHECK(
      partitions == referencedPartitions,
      "Velox and reference DB output directory hierarchies don't match");
}

std::set<std::string> WriterFuzzer::getPartitionNames(
    const std::string& outputDirectoryPath) {
  auto fileSystem = filesystems::getFileSystem("/", nullptr);
  auto directories = fileSystem->listFolders(outputDirectoryPath);
  std::set<std::string> partitionNames;

  for (std::string directory : directories) {
    // Remove the path prefix to get the partition name
    // For example: /test/tmp_write/p0=1/p1=2020
    // partition name is /p0=1/p1=2020
    directory.erase(0, fileSystem->extractPath(outputDirectoryPath).length());

    // If it's a hidden directory, ignore
    if (directory.find("/.") != std::string::npos) {
      continue;
    }

    partitionNames.insert(directory);
  }

  return partitionNames;
}

} // namespace
} // namespace facebook::velox::exec::test