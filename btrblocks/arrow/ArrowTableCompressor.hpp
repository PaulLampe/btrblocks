#pragma once
#include <arrow/api.h>
#include <cstdint>
#include <memory>
#include <vector>
#include "ArrowTableChunk.hpp"
#include "btrblocks.hpp"

namespace btrblocks {
// Orchestrates all compression related tasks
struct ArrowTableCompressor {
 public:
  ArrowTableCompressor() = default;

  static std::shared_ptr< vector< tuple< OutputBlockStats, vector<u8> > > > compress(std::shared_ptr<arrow::Table> batch);

  static std::shared_ptr<arrow::Table> decompress(std::shared_ptr<vector< tuple< OutputBlockStats, vector<u8> > >>& compressedBatch);
};

}  // namespace btrblocks