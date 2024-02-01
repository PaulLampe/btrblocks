#include <arrow/io/file.h>
#include <arrow/type_fwd.h>
#include <parquet/arrow/reader.h>
#include <filesystem>
#include <unordered_set>
#include <vector>
#include "arrow/columnwise/ArrowColumnwiseTableCompressor.hpp"
#include "btrblocks.hpp"
#include "gflags/gflags.h"
#include "scheme/SchemePool.hpp"

using namespace std;

DEFINE_string(dir, "dir", "Directory with parquet file as original.parquet");

std::shared_ptr<arrow::Table> loadTableFromParquet(const std::string& path,
                                                   unordered_set<string> columns) {
  std::shared_ptr<arrow::io::RandomAccessFile> input =
      arrow::io::ReadableFile::Open(path).ValueOrDie();

  unique_ptr<parquet::arrow::FileReader> arrow_reader;

  std::unique_ptr<parquet::ParquetFileReader> parquet_reader =
      parquet::ParquetFileReader::OpenFile(path, false);

  auto arrow_reader_properties = parquet::default_arrow_reader_properties();
  arrow_reader_properties.set_use_threads(true);
  arrow_reader_properties.set_pre_buffer(true);

  parquet::arrow::FileReader::Make(arrow::default_memory_pool(), std::move(parquet_reader),
                                   arrow_reader_properties, &arrow_reader)
      .ok();

  std::shared_ptr<arrow::Table> table;
  arrow_reader->ReadTable(&table).ok();

  vector<int> columnIndices{};

  for (int i = 0; i < table->num_columns(); ++i) {
    auto name = table->field(i)->name();

    auto type = table->field(i)->type();

    if (type->Equals(arrow::int32()) || type->Equals(arrow::float64()) ||
                       type->Equals(arrow::utf8())) {
      if (columns.empty() || columns.find(name) != columns.end()) {
        columnIndices.push_back(i);
      }
    }
  }

  return table->SelectColumns(columnIndices).ValueOrDie();
}

void SetupSchemes() {
  btrblocks::BtrBlocksConfig::get().integers.schemes = btrblocks::defaultIntegerSchemes();
  btrblocks::BtrBlocksConfig::get().doubles.schemes = btrblocks::defaultDoubleSchemes();
  btrblocks::BtrBlocksConfig::get().strings.schemes = btrblocks::defaultStringSchemes();

  btrblocks::SchemePool::refresh();
}

int main(int argc, char** argv) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);

  SetupSchemes();

  std::filesystem::path directory = FLAGS_dir;

  std::cout << "Loading parquet file ... \n";

  auto table = loadTableFromParquet(directory.string() + "/original.parquet", {});

  std::cout << "Compressing table ... \n";

  auto compressedData = btrblocks::ArrowColumnwiseTableCompressor::compress(table);

  std::cout << "Writing data ... \n";

  btrblocks::ArrowWriter::writeBtrBlocks(directory, compressedData);
}