#include <arrow/chunked_array.h>
#include <arrow/scalar.h>
#include <arrow/type_fwd.h>
#include <iterator>
#include <memory>
#include <vector>
#include "arrow/chunkwise/ArrowChunkwiseTableCompressor.hpp"
#include "arrow/columnwise/ArrowColumnwiseTableCompressor.hpp"
#include "test-cases/TestHelper.hpp"

void printAverageCompressionRatio(
    std::shared_ptr<vector<tuple<OutputBlockStats, vector<u8>>>>& output) {
  double compressionRatioSum =
      std::accumulate(output->begin(), output->end(), 0.00,
                      [](auto& agg, auto& p) { return agg + std::get<0>(p).compression_ratio; });

  std::cout << "average compression ratio: "
            << static_cast<double>(compressionRatioSum) / static_cast<double>(output->size())
            << "\n";
}

void checkCompressTableChunkwise(std::shared_ptr<arrow::Table>& table) {
  auto output = ArrowChunkwiseTableCompressor::compress(table);

  auto compressed_data = std::make_shared<vector< vector<u8> > >();

  for (auto& tuple : *output) {
    compressed_data->push_back(std::move(std::get<1>(tuple)));
  }

  auto decompressedOutput = ArrowChunkwiseTableCompressor::decompress(compressed_data);

  printAverageCompressionRatio(output);

  // TODO: Save column name in meta
  for (int i = 0; i < table->schema()->num_fields(); ++i) {
    auto originalColumn = table->column(i);
    auto decompressedColumn = decompressedOutput->column(i);

    assert(originalColumn->Equals(decompressedColumn));
  }
}

void compareArrays(shared_ptr<arrow::ChunkedArray> arr1, shared_ptr<arrow::ChunkedArray> arr2) {
  for (int i = 0; i < arr1->length(); i++) {
    auto val1 = arr1->GetScalar(i).ValueOrDie()->ToString();
    auto val2 = arr2->GetScalar(i).ValueOrDie()->ToString();
  }
}

void checkCompressTableColumnwise(std::shared_ptr<arrow::Table>& table) {
  auto [fileMeta, compressedData] = ArrowColumnwiseTableCompressor::compress(table);

  vector< vector< vector<u8> > > writtenParts{};
  vector<SIZE> columnIndices{};
  SIZE i = 0;
  for (auto& column : compressedData) {
    columnIndices.push_back(i++);
    writtenParts.emplace_back();
    for (auto& part : column) {
      writtenParts.back().push_back(part.writeToByteVector());
    }
  }

  auto decompressedOutput = ArrowColumnwiseTableCompressor::decompress(fileMeta.get(), writtenParts, columnIndices);

  // TODO: Save column name in meta
  for (int i = 0; i < table->schema()->num_fields(); ++i) {
    auto originalColumn = table->column(i);
    auto decompressedColumn = decompressedOutput->column(i);

    compareArrays(originalColumn, decompressedColumn);

    assert(originalColumn->Equals(decompressedColumn));
  }
}