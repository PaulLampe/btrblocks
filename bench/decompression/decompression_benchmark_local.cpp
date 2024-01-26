#include <arrow/api.h>
#include <arrow/io/file.h>
#include <oneapi/tbb/detail/_machine.h>
#include <parquet/arrow/reader.h>
#include <tbb/parallel_for.h>
#include <tbb/task.h>
#include <tbb/task_scheduler_init.h>
#include <chrono>
#include <functional>
#include <iostream>
#include <iterator>
#include <memory>
#include <string>
#include <vector>
#include "benchmark/benchmark.h"
#include "btrblocks.hpp"
#include "common/Units.hpp"
#include "common/Utils.hpp"
#include "compression/Datablock.hpp"
#include "scheme/SchemePool.hpp"

namespace btrbench {
using namespace std;

std::shared_ptr<arrow::Table> loadTableFromParquet(const std::string& path,
                                                   unordered_set<string> columns) {
  std::shared_ptr<arrow::io::RandomAccessFile> input =
      arrow::io::ReadableFile::Open(path).ValueOrDie();

  unique_ptr<parquet::arrow::FileReader> arrow_reader;

  std::unique_ptr<parquet::ParquetFileReader> parquet_reader = parquet::ParquetFileReader::OpenFile(
                        path,
                        false);

  auto arrow_reader_properties = parquet::default_arrow_reader_properties();
  arrow_reader_properties.set_use_threads(true);
  arrow_reader_properties.set_pre_buffer(true);
                
  parquet::arrow::FileReader::Make(arrow::default_memory_pool(), std::move(parquet_reader),arrow_reader_properties, &arrow_reader).ok();

  std::shared_ptr<arrow::Table> table;
  arrow_reader->ReadTable(&table).ok();

  vector<int> columnIndices{};

  for (int i = 0; i < table->num_columns(); ++i) {
    auto name = table->field(i)->name();
    if (columns.size() == 0 || columns.find(name) != columns.end()) {
      columnIndices.push_back(i);
    }
  }

  return table->SelectColumns(columnIndices).ValueOrDie();
}

void debugTables(shared_ptr<arrow::Table>& table, shared_ptr<arrow::Table>& originalTable) {
  cout << table->schema()->ToString(false) << "\n";
  cout << originalTable->schema()->ToString(false) << "\n";

  for (int i = 0; i < table->num_columns(); i++) {
    auto col = table->column(i);
    auto originalCol = table->column(i);

    for (int i = 0; i < 20; i++) {
      cout << col->GetScalar(i).ValueOrDie()->ToString() << "\n";
      cout << originalCol->GetScalar(i).ValueOrDie()->ToString() << "\n";
    }
  }
}

static void measure(benchmark::State& state, std::function<void()>&& fun, std::string&& key) {

    auto start_time = std::chrono::steady_clock::now();

    fun();

    auto end_time  = std::chrono::steady_clock::now();

    auto diff = std::chrono::duration_cast<std::chrono::microseconds>(end_time - start_time);

    state.counters[key] = diff.count();
}

const std::string decomp_bench_dataset = "decompression-bench-dataset/";

static void SetupAllDecompSchemes() {
  btrblocks::BtrBlocksConfig::get().integers.schemes = btrblocks::defaultIntegerSchemes();
  btrblocks::BtrBlocksConfig::get().doubles.schemes = btrblocks::defaultDoubleSchemes();
  btrblocks::BtrBlocksConfig::get().strings.schemes = btrblocks::defaultStringSchemes();

  btrblocks::SchemePool::refresh();
}

static void BtrBlocksDecompressionBenchmark(benchmark::State& state,
                                            const vector<btrblocks::ColumnType>& types) {
  SetupAllDecompSchemes();

  tbb::task_scheduler_init(32);

  for (auto _ : state) {
    vector<string> columnKeys{};

    vector<vector<vector<btrblocks::u8>>> compressedData{};
    vector<btrblocks::SIZE> selectedColumnIndices;

    std::vector<btrblocks::u8> raw_file_metadata;
    btrblocks::FileMetadata* file_metadata;

    measure(state, [&]() {
      auto metaLocalPath = decomp_bench_dataset + "metadata";

      btrblocks::Utils::readFileToMemory(metaLocalPath, raw_file_metadata);
      file_metadata = reinterpret_cast<btrblocks::FileMetadata*>(raw_file_metadata.data());

      compressedData.resize(file_metadata->num_columns);

      for (int i = 0; i < file_metadata->num_columns; ++i) {
        if (std::find(types.begin(),types.end(),file_metadata->parts[i].type) != types.end()) {
          selectedColumnIndices.push_back(i);
        }
      }

      tbb::parallel_for(btrblocks::u32(0), file_metadata->num_columns, [&](const auto& column_i) {
        
        if (std::find(selectedColumnIndices.begin(),selectedColumnIndices.end(), column_i) != selectedColumnIndices.end()) {
          
          compressedData[column_i].resize(file_metadata->parts[column_i].num_parts);

          tbb::parallel_for (btrblocks::u32(0), file_metadata->parts[column_i].num_parts, [&](const auto& part_i){
            auto columnPartPath =
                decomp_bench_dataset + "column" + to_string(column_i) + "_part" + to_string(part_i);

            btrblocks::Utils::readFileToMemory(columnPartPath, compressedData[column_i][part_i]);
          });
        }
      });
    }, "file_read_time");

    measure(state, [&]() {
      auto table = btrblocks::ArrowColumnwiseTableCompressor::decompress(
        file_metadata, compressedData, selectedColumnIndices);
    }, "decompression_time");


    /*measure(state, [&]() {
      auto originalTable = loadTableFromParquet(decomp_bench_dataset + "original.parquet", {});
    }, "parquet_decompression_time");*/

    // debugTables(table, originalTable);
  }
}
}  // namespace btrbench