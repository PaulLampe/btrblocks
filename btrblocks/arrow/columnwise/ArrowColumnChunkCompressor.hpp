
#include <arrow/array/array_base.h>
#include <memory>
#include <vector>
#include "common/Units.hpp"
#include "compression/Datablock.hpp"

namespace btrblocks {

using namespace std;

template <ColumnType COLUMN_TYPE>
class ArrowColumnChunkCompressor {
public:
  static vector<u8> compress(const shared_ptr<arrow::Array>& chunk);

  static std::shared_ptr<arrow::Array> decompress(ColumnChunkMeta* columnChunk);

 private:
  static SIZE getChunkByteSize(const shared_ptr<arrow::Array>& chunk);

  static SIZE compress(const shared_ptr<arrow::Array>& chunk, ColumnChunkMeta* meta);
};

template <ColumnType COLUMN_TYPE>
vector<u8> ArrowColumnChunkCompressor<COLUMN_TYPE>::compress(const shared_ptr<arrow::Array>& chunk) {
  const SIZE tupleCount = chunk->length();

  const u32 initialOutputSize =
      sizeof(ColumnChunkMeta) + 10 * getChunkByteSize(chunk) + sizeof(BITMAP) * tupleCount;

  vector<u8> output(initialOutputSize);

  auto meta = reinterpret_cast<ColumnChunkMeta*>(output.data());
  meta->tuple_count = tupleCount;
  meta->type = COLUMN_TYPE;

  auto size = compress(chunk, meta);

  output.resize(size);
  
  return output;
}

}  // namespace btrblocks