#pragma once
#include <arrow/api.h>
#include "ArrowTableChunk.hpp"
#include "btrblocks.hpp"

namespace btrblocks {
// Orchestrates all compression related tasks
struct ArrowTableCompressor {
 public:
  ArrowTableCompressor() = default;

  static void compress(std::shared_ptr<arrow::RecordBatch> batch);
};

}  // namespace btrblocks