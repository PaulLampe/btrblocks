#include <arrow/array/array_binary.h>
#include <arrow/record_batch.h>
#include <arrow/type_fwd.h>
#include <tbb/blocked_range.h>
#include <tbb/enumerable_thread_specific.h>
#include <tbb/parallel_for.h>
#include <tbb/task_scheduler_init.h>
#include <algorithm>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <iostream>
#include <memory>
#include <string>
#include <tuple>
#include <unordered_map>
#include <unordered_set>
#include <vector>
#include "btrblocks.hpp"
#include "gtest/gtest.h"

const std::string folder = "parquet-to-btr";

TEST(LocalArrowScan, ReadMetaData) {
  btrblocks::LocalArrowReader reader(folder);

  std::cout << reader.getSchema()->ToString() << "\n";
}

TEST(LocalArrowScan, Scan) {
  btrblocks::LocalArrowReader reader(folder);

  tbb::enumerable_thread_specific<double> sums;

  reader.scan({"specialtydesc", "calculation3170826185505725"},[&](const std::shared_ptr<arrow::RecordBatch>& batch) {
    double sum = 0;

    auto calc = std::static_pointer_cast<arrow::DoubleArray>(batch->column(1));

    for (int64_t i = 0; i < batch->num_rows(); ++i) {
      sum += calc->Value(i);
    }

    sums.local() += sum;
  });

  double sum = 0;

  for (auto& part : sums) {
    sum += part;
  }

  std::cout << "Sum: " << std::to_string(sum) << "\n";
}