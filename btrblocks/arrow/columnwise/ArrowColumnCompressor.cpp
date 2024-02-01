#include "ArrowColumnCompressor.hpp"
#include <arrow/array/array_base.h>
#include <arrow/type.h>
#include <arrow/type_fwd.h>
#include <cassert>
#include <cstdint>
#include <memory>
#include <stdexcept>
#include <vector>
#include "ArrowColumnChunkCompressor.hpp"
#include "ArrowColumnChunkDecompressor.hpp"
#include "arrow/columnwise/ArrowColumn.hpp"
#include "common/Exceptions.hpp"
#include "common/Units.hpp"
#include "compression/Datablock.hpp"
#include "storage/Chunk.hpp"
#include "tbb/parallel_for.h"

using namespace std;

namespace btrblocks {

vector<ColumnPart> ArrowColumnCompressor::compressColumn(ArrowColumn& column) {
  vector<ColumnPart> parts{ColumnPart()};

  for (auto& chunk : column.chunks) {
    vector<u8> compressedChunk;

    switch (column.type) {
      case ColumnType::INTEGER:
        compressedChunk = ArrowColumnChunkCompressor<ColumnType::INTEGER>::compress(chunk);
        break;
      case ColumnType::DOUBLE:
        compressedChunk = ArrowColumnChunkCompressor<ColumnType::DOUBLE>::compress(chunk);
        break;
      case ColumnType::STRING:
        compressedChunk = ArrowColumnChunkCompressor<ColumnType::STRING>::compress(chunk);
        break;
      default:
        throw runtime_error("Not implemented");
    }

    if (!parts.back().canAdd(compressedChunk.size())) {
      parts.emplace_back();
    }
    parts.back().addCompressedChunk(std::move(compressedChunk));
  }

  return parts;
}

ArrowColumn ArrowColumnCompressor::decompressColumn(vector<vector<u8>>& parts,
                                                    ArrowMetaData::ArrowColumnMetaData& columnInfo) {
  vector<SIZE> partOffsets(parts.size() + 1);
  partOffsets[0] = 0;

  for (SIZE i = 1; i <= parts.size(); ++i) {
    partOffsets[i] =
        partOffsets[i - 1] + reinterpret_cast<ColumnPartMetadata*>(parts[i - 1].data())->num_chunks;
  }

  arrow::ArrayVector chunks(partOffsets[parts.size()]);

  tbb::parallel_for(SIZE(0), parts.size(), [&](const auto& part_i) {
    auto meta = reinterpret_cast<ColumnPartMetadata*>(parts[part_i].data());

    tbb::parallel_for(u32(0), meta->num_chunks, [&](const auto& chunk_i) {
      auto columnChunk = reinterpret_cast<ColumnChunkMeta*>(parts[part_i].data() + meta->offsets[chunk_i]);

      chunks[partOffsets[part_i] + chunk_i] = ArrowColumnChunkDecompressor::decompress(columnChunk);
    });
  });

  return ArrowColumn(columnInfo.type, chunks);
}

}  // namespace btrblocks