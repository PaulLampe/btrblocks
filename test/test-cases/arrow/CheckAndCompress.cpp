#include <arrow/scalar.h>
#include <arrow/type_fwd.h>
#include "arrow/ArrowTableCompressor.hpp"
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

void checkCompressTable(std::shared_ptr<arrow::Table>& table) {
  auto output = ArrowTableCompressor::compress(table);
  auto decompressedOutput = ArrowTableCompressor::decompress(output);

  printAverageCompressionRatio(output);

  // TODO: Save column name in meta
  for (int i = 0; i < table->schema()->num_fields(); ++i) {
    auto originalColumn = table->column(i);
    auto decompressedColumn = decompressedOutput->column(i);

    assert(originalColumn->Equals(decompressedColumn));
  }
}