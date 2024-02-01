#include <arrow/api.h>
#include "compression/Datablock.hpp"
#include "ArrowColumnChunkDecompressor.hpp"
#include "ArrowColumnChunkCompressor.hpp"

namespace btrblocks {

using namespace std;

shared_ptr<arrow::Array> ArrowColumnChunkDecompressor::decompress(ColumnChunkMeta* columnChunk) {
  switch (columnChunk->type) {
        case ColumnType::INTEGER:
          return ArrowColumnChunkCompressor<ColumnType::INTEGER>::decompress(columnChunk);
        case ColumnType::DOUBLE:
          return ArrowColumnChunkCompressor<ColumnType::DOUBLE>::decompress(columnChunk);
        case ColumnType::STRING:
          return ArrowColumnChunkCompressor<ColumnType::STRING>::decompress(columnChunk);
        default:
          throw runtime_error("Not implemented");
      }
};

} // namespace btrblocks