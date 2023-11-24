#include "arrow/api.h"
#include "ArrowTableCompressor.hpp"
#include "ArrowTableWrapper.hpp"
#include "ArrowTableChunkCompressor.hpp"

namespace btrblocks {

void ArrowTableCompressor::compress(std::shared_ptr<arrow::RecordBatch> batch) {
  ArrowTableWrapper wrapper(batch);
  vector<ArrowTableChunk> chunks = wrapper.generateChunks();

  auto output = make_unique<vector<uint8_t>>();

  for (auto& chunk : chunks) {
    auto res = ArrowTableChunkCompressor::compress(chunk);
    output->insert(output->end(), res.begin(), res.end());
  }
}

}