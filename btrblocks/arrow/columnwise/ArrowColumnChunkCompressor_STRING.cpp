#include <arrow/api.h>
#include <arrow/array/array_base.h>
#include <arrow/type_fwd.h>
#include <memory>
#include <vector>
#include "ArrowColumnChunkCompressor.hpp"
#include "btrblocks.hpp"
#include "common/Units.hpp"
#include "compression/Datablock.hpp"
#include "compression/SchemePicker.hpp"
#include "../ArrowStringArrayViewer.cpp"

namespace btrblocks {

using namespace std;

template <>
SIZE ArrowColumnChunkCompressor<ColumnType::STRING>::getChunkByteSize(
    const shared_ptr<arrow::Array>& chunk) {
  return sizeof(DOUBLE) * chunk->length();
}

template <>
SIZE ArrowColumnChunkCompressor<ColumnType::STRING>::compress(const shared_ptr<arrow::Array>& chunk,
                                                              ColumnChunkMeta* meta) {
  auto& config = BtrBlocksConfig::get();

  auto array = std::static_pointer_cast<arrow::StringArray>(chunk);

  auto tupleCount = array->length();

  auto arrowStringArrayViewer = ArrowStringArrayViewer(array);

  auto dataSize = array->value_offsets()->size() + array->total_values_length();


  StringStats stats =
      StringStats::generateStats(arrowStringArrayViewer,
                                 array->null_bitmap_data(), tupleCount, dataSize );

  StringScheme& preferred_scheme =
      StringSchemePicker::chooseScheme(stats, config.strings.max_cascade_depth);

  meta->compression_type = static_cast<u8>(preferred_scheme.schemeType());

  auto compressedColumnSize = preferred_scheme.compress(arrowStringArrayViewer, array->null_bitmap_data(), meta->data, stats);

  return sizeof(ColumnChunkMeta) + compressedColumnSize;
}

template <>
shared_ptr<arrow::Array> ArrowColumnChunkCompressor<ColumnType::STRING>::decompress(
    ColumnChunkMeta* columnChunk) {
  const auto used_compression_scheme = static_cast<StringSchemeType>(columnChunk->compression_type);

  auto& scheme = SchemePool::available_schemes->string_schemes[used_compression_scheme];

  auto size =
      scheme->getDecompressedSizeNoCopy(columnChunk->data, columnChunk->tuple_count, nullptr);

  SIZE destinationSize = size + 8 + SIMD_EXTRA_BYTES + 4096;

  auto destination = makeBytesArray(destinationSize);

  scheme->decompressNoCopy(destination.get(), nullptr, columnChunk->data, columnChunk->tuple_count,
                           0);

  auto pointerViewer = StringPointerArrayViewer(destination.get());

  arrow::StringBuilder stringBuilder;
  stringBuilder.Resize(columnChunk->tuple_count).ok();

  for (SIZE i = 0; i < columnChunk->tuple_count; ++i) {
    stringBuilder.Append(pointerViewer(i)).ok();
  }

  return stringBuilder.Finish().ValueOrDie();
}

}  // namespace btrblocks