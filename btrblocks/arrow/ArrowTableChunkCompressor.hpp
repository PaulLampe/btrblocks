#pragma once
#include <arrow/api.h>
#include "ArrowTableChunk.hpp"
#include "btrblocks.hpp"

namespace btrblocks {

// Compresses one chunk to a BytesArray and handles metadata
struct ArrowTableChunkCompressor {
 public:
  ArrowTableChunkCompressor() = default;

  static std::vector<uint8_t> compress(ArrowTableChunk& chunk);
};

}  // namespace btrblocks