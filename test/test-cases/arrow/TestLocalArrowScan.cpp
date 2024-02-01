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

/*
SELECT
  "Medicare1_1"."SPECIALTY_DESC" AS "SPECIALTY_DESC",
  AVG(CAST("Medicare1_1"."Calculation_3170826185505725" AS double)) AS
"avg:Calculation_3170826185505725:ok" FROM "Medicare1_1"

GROUP BY "Medicare1_1"."SPECIALTY_DESC"
ORDER BY "avg:Calculation_3170826185505725:ok"
DESC LIMIT 15;
*/

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

TEST(LocalArrowScan, Query1) {
    btrblocks::LocalArrowReader reader(folder);

    std::hash<std::string> key_hasher;

    auto htSize = 256;
    auto numPartitions = 32;

    tbb::enumerable_thread_specific<std::vector<std::tuple<std::string, uint64_t>>>
        threadLocalCountHT(
            [&]() { return std::vector<std::tuple<std::string, uint64_t>>(htSize); });

    tbb::enumerable_thread_specific<std::vector<std::vector<std::tuple<std::string, uint64_t>>>>
        threadLocalCountPartitions([&]() {
          return std::vector<std::vector<std::tuple<std::string, uint64_t>>>(numPartitions);
        });

    tbb::enumerable_thread_specific<std::vector<std::tuple<std::string, double>>> threadLocalSumHT(
        [&]() { return std::vector<std::tuple<std::string, double>>(htSize); });

    tbb::enumerable_thread_specific<std::vector<std::vector<std::tuple<std::string, double>>>>
        threadLocalSumPartitions([&]() {
          return std::vector<std::vector<std::tuple<std::string, double>>>(numPartitions);
        });

    reader.scan({"specialtydesc", "calculation3170826185505725"},
                [&](const std::shared_ptr<arrow::RecordBatch>& batch) {
                  auto& countHT = threadLocalCountHT.local();
                  auto& countPartitions = threadLocalCountPartitions.local();

                  auto& sumHT = threadLocalSumHT.local();
                  auto& sumPartitions = threadLocalSumPartitions.local();

                  auto type = std::static_pointer_cast<arrow::StringArray>(batch->column(0));
                  auto calc = std::static_pointer_cast<arrow::DoubleArray>(batch->column(1));

                  for (int64_t i = 0; i < batch->num_rows(); ++i) {
                    auto key = type->GetString(i);
                    auto val = calc->Value(i);

                    auto hash = key_hasher(key);
                    auto slot = hash % htSize;

                    auto currCount = countHT[slot];
                    auto currSum = sumHT[slot];

                    if (std::get<0>(currCount) != key) {
                      if (std::get<0>(currCount) != "") {
                        auto partition = hash % numPartitions;
                        countPartitions[partition].push_back(currCount);
                        sumPartitions[partition].push_back(currSum);
                      }

                      countHT[slot] = {key, 1};
                      sumHT[slot] = {key, val};
                    } else {
                      countHT[slot] = {key, std::get<1>(currCount) + 1};
                      sumHT[slot] = {key, std::get<1>(currSum) + val};
                    }
                  }
                });

    tbb::parallel_for(threadLocalCountHT.range(), [&](const auto& range) {
      auto& countPartitions = threadLocalCountPartitions.local();

      for (auto& countHT : range) {
        for (auto& val : countHT) {
          auto key = std::get<0>(val);

          if (key != "") {
            auto hash = key_hasher(key);
            auto partition = hash % numPartitions;
            countPartitions[partition].push_back(val);
          }
        }
      }
    });

    tbb::parallel_for(threadLocalSumHT.range(), [&](const auto& range) {
      auto& sumPartitions = threadLocalSumPartitions.local();

      for (auto& vec : range) {
        for (auto& val : vec) {
          auto key = std::get<0>(val);

          if (key != "") {
            auto hash = key_hasher(key);
            auto partition = hash % numPartitions;

            sumPartitions[partition].push_back(val);
          }
        }
      }
    });

    tbb::enumerable_thread_specific<std::vector<std::tuple<std::string, double>>> kAverages;

    auto min_heap_compare = [](const std::tuple<std::string, double>& t1,
                               const std::tuple<std::string, double>& t2) {
      return std::get<1>(t1) > std::get<1>(t2);
    };

    auto max_heap_compare = [](const std::tuple<std::string, double>& t1,
                               const std::tuple<std::string, double>& t2) {
      return std::get<1>(t1) < std::get<1>(t2);
    };

    tbb::blocked_range<size_t> partitionRange(0, numPartitions);

    tbb::parallel_for(partitionRange, [&](const auto& range) {
      std::unordered_map<std::string, uint64_t> countHT{};
      std::unordered_map<std::string, double> sumHT{};
      std::unordered_set<std::string> keys{};

      for (auto partition_i = range.begin(); partition_i < range.end(); ++partition_i) {
        for (auto& vec : threadLocalCountPartitions) {
          for (auto& tuple : vec[partition_i]) {
            countHT[std::get<0>(tuple)] += std::get<1>(tuple);
            keys.insert(std::get<0>(tuple));
          }
        }
        for (auto& vec : threadLocalSumPartitions) {
          for (auto& tuple : vec[partition_i]) {
            sumHT[std::get<0>(tuple)] += std::get<1>(tuple);
          }
        }
      }

      auto& heap = kAverages.local();

      for (const auto& key : keys) {
        auto avg = sumHT[key] * 1.00 / countHT[key];

        if (heap.size() == 15) {
          std::make_heap(heap.begin(), heap.end(), min_heap_compare);
        }

        if (heap.size() < 15) {
          heap.emplace_back(key, avg);
        } else {
          auto new_tuple = std::tuple{key, avg};
          if (max_heap_compare(heap.front(), new_tuple)) {
            std::pop_heap(heap.begin(), heap.end(), min_heap_compare);
            heap[14] = new_tuple;
            std::push_heap(heap.begin(), heap.end(), min_heap_compare);
          }
        }
      }
    });

    tbb::parallel_for(kAverages.range(), [&](const auto& heaps) {
      for (auto& heap : heaps) {
        std::make_heap(heap.begin(), heap.end(), max_heap_compare);
      }
    });

    std::vector<std::vector<std::tuple<std::string, double>>> heapsHeap{kAverages.begin(),
                                                                        kAverages.end()};

    auto heap_heap_compare = [](const std::vector<std::tuple<std::string, double>>& t1,
                                const std::vector<std::tuple<std::string, double>>& t2) {
      return std::get<1>(t1.front()) < std::get<1>(t2.front());
    };

    std::make_heap(heapsHeap.begin(), heapsHeap.end(), heap_heap_compare);

    std::vector<std::tuple<std::string, double>> result;

    for (size_t i = 0; i < 15; ++i) {
      result.push_back(heapsHeap.front().front());

      std::pop_heap(heapsHeap.begin(), heapsHeap.end(), heap_heap_compare);

      std::pop_heap(heapsHeap.back().begin(), heapsHeap.back().end(), max_heap_compare);

      heapsHeap.back().pop_back();

      if (heapsHeap.back().empty()) {
        heapsHeap.pop_back();
      } else {
        std::push_heap(heapsHeap.begin(), heapsHeap.end(), heap_heap_compare);
      }
    }
}