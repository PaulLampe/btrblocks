#include <arrow/record_batch.h>
#include <arrow/type.h>
#include <arrow/type_fwd.h>
#include <sys/types.h>
#include <cstddef>
#include <memory>
#include <span>
#include <unordered_map>
#include <utility>
#include "ArrowColumnChunkDecompressor.hpp"
#include "storage/Chunk.hpp"

namespace btrblocks {

using namespace std;

using ColumnIndex = size_t;
using PartInternalOffset = size_t;
using CompressedDataType = std::span<uint8_t>;

class ArrowRowGroupDecompressor {
 public:
  static shared_ptr<arrow::RecordBatch> decompressRowGroup(
      shared_ptr<arrow::Schema>& schema,
      map<ColumnIndex, std::pair<CompressedDataType, PartInternalOffset>> columnData) {
    auto numColumnsToDecompress = columnData.size();

    arrow::ArrayVector columns(numColumnsToDecompress);

    for (size_t column_i = 0; column_i < numColumnsToDecompress; ++column_i) {
      auto [data, partInternalOffset] = columnData[column_i];

      auto* dataPtr = data.data();

      auto columnPart = reinterpret_cast<ColumnPartMetadata*>(dataPtr);

      columns[column_i] = ArrowColumnChunkDecompressor::decompress(
          reinterpret_cast<ColumnChunkMeta*>(dataPtr + columnPart->offsets[partInternalOffset]));
    }

    return arrow::RecordBatch::Make(schema, columns[0]->length(), columns);
  }
};
}  // namespace btrblocks