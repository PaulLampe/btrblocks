#pragma once
#include <arrow/api.h>
#include "ArrowTableChunk.hpp"
#include "btrblocks.hpp"

namespace btrblocks {

// Compresses one chunk to a BytesArray and handles metadata
struct ArrowTableChunkCompressor {
public:
  ArrowTableChunkCompressor() = default;

  static std::tuple<OutputBlockStats, std::vector<u8>> compress(ArrowTableChunk& chunk);

  static ArrowTableChunk decompress(std::vector<u8>& compressed_data);

private:

  static OutputBlockStats initOutputStats_(SIZE columnCount, SIZE datablockMetaBufferSize);
};

}  // namespace btrblocks