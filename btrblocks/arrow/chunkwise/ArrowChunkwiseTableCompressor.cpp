#include <arrow/record_batch.h>
#include <arrow/table.h>
#include <arrow/type_fwd.h>
#include <memory>
#include <tuple>
#include <vector>
#include "arrow/api.h"
#include "ArrowChunkwiseTableCompressor.hpp"
#include "../ArrowTableWrapper.hpp"
#include "ArrowTableChunkCompressor.hpp"
#include "common/Units.hpp"
#include "compression/Compressor.hpp"
#include "storage/MMapVector.hpp"

namespace btrblocks {

std::shared_ptr< vector< tuple< OutputBlockStats, vector<u8> > > > ArrowChunkwiseTableCompressor::compress(std::shared_ptr<arrow::Table> batch) {
  ArrowTableWrapper wrapper(std::move(batch));
  vector<ArrowTableChunk> chunks = wrapper.generateChunks();

  auto output = std::make_shared<vector< tuple< OutputBlockStats, vector<u8> > > >();

  for (auto& chunk : chunks) {
    output->push_back(ArrowTableChunkCompressor::compress(chunk));
  }

  return output;
}

std::shared_ptr<arrow::Table> ArrowChunkwiseTableCompressor::decompress(std::shared_ptr< vector< vector<u8> > >& compressedBatch) {
  vector<ArrowTableChunk> chunks{};

  for (auto& compressedChunk : *compressedBatch) {
    chunks.push_back(ArrowTableChunkCompressor::decompress(compressedChunk));
  }

  auto columnCount = chunks[0].columnCount();

  std::vector<std::shared_ptr<arrow::Field>> fields(columnCount);
  std::vector<std::vector<std::shared_ptr<arrow::Array>>> chunkedColumns(columnCount);

  for (SIZE i = 0; i < columnCount; ++i) {
      auto [field, data] = chunks[0].getSchemeAndColumn(i);
      fields[i] = field;
  }

  for (auto& decompressedChunk : chunks) {
    for (SIZE i = 0; i < columnCount; ++i) {
      auto [type, data] = decompressedChunk.getSchemeAndColumn(i);

      assert(fields[i]->Equals(type));

      chunkedColumns[i].push_back(data);
    }
  }

  std::vector<std::shared_ptr<arrow::ChunkedArray>> columns{};

  for (SIZE i = 0; i < columnCount; ++i) {
    columns.push_back(std::make_shared<arrow::ChunkedArray>(std::move(chunkedColumns[i])));
  }

  return arrow::Table::Make(arrow::schema(fields), columns);
}


}