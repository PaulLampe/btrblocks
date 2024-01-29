
#include <arrow/array/array_base.h>
#include <arrow/buffer.h>
#include <arrow/type_fwd.h>
#include <memory>
#include <stdexcept>
#include <vector>
#include "arrow/columnwise/ArrowColumn.hpp"
#include "common/Units.hpp"
#include "compression/Datablock.hpp"
#include "extern/RoaringBitmap.hpp"

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
  static shared_ptr<arrow::Buffer> decompressBitmap(ColumnChunkMeta* meta);
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

  auto compressedSize = compress(chunk, meta);

  auto nullMapSize = 0u;

  if(chunk->null_bitmap_data() != nullptr) {
    meta->nullmap_offset = compressedSize;
    auto [size, type] = bitmap::RoaringBitmap::compressArrowBitmap(chunk, reinterpret_cast<u8*>(meta->data) + meta->nullmap_offset);
    meta->nullmap_type = type;
    nullMapSize = size;
  }
  output.resize(sizeof(ColumnChunkMeta) + compressedSize + nullMapSize);
  
  return output;
}

template<ColumnType COLUMN_TYPE>
shared_ptr<arrow::Buffer> ArrowColumnChunkCompressor<COLUMN_TYPE>::decompressBitmap(ColumnChunkMeta* columnChunk) {
  if (columnChunk->nullmap_type == BitmapType::ALLONES) {
    return nullptr;
  }

  auto nullmapBuffer = arrow::AllocateEmptyBitmap(columnChunk->tuple_count).ValueOrDie();

  if(columnChunk->nullmap_type == BitmapType::ALLZEROS) {
    return nullmapBuffer;
  }
  
  auto nullmapDest = nullmapBuffer->mutable_data();
  auto bitmap_out = make_unique<BitmapWrapper>(reinterpret_cast<u8*>(columnChunk->data) + columnChunk->nullmap_offset, columnChunk->nullmap_type, columnChunk->tuple_count);
  bitmap_out->writeArrowBITMAP(nullmapDest);
  return nullmapBuffer;
}

}  // namespace btrblocks