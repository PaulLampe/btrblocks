#include "ArrowTableWrapper.hpp"

using namespace btrblocks;

std::vector<ArrowTableChunk> ArrowTableWrapper::generateChunks() {
  std::vector<ArrowTableChunk> chunks{};

  auto& config = BtrBlocksConfig::get();
  auto blockSize = static_cast<int64_t>(config.block_size);

  int64_t offset = 0;

  while (offset + blockSize < batch->num_rows()) {
    chunks.push_back(generateChunk(offset, blockSize));
    offset += blockSize;
  }

  chunks.push_back(generateChunk(offset, batch->num_rows() - offset));
  return chunks;
}

ArrowTableChunk ArrowTableWrapper::generateChunk(int64_t offset, int64_t blockSize) {
  arrow::ArrayVector columns{};

  for (auto& column : batch->columns()) {
    auto slice = column->Slice(offset, blockSize);
    columns.push_back(std::move(slice));
  }

  return ArrowTableChunk(batch->schema(), std::move(columns));
}