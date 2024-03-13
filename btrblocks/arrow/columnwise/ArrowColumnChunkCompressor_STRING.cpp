#include <arrow/api.h>
#include <arrow/array/array_base.h>
#include <arrow/array/array_binary.h>
#include <arrow/type_fwd.h>
#include <cstdint>
#include <memory>
#include <vector>
#include "../ArrowStringArrayViewer.cpp"
#include "ArrowColumnChunkCompressor.hpp"
#include "btrblocks.hpp"
#include "common/Units.hpp"
#include "compression/Datablock.hpp"
#include "compression/SchemePicker.hpp"
#include "extern/RoaringBitmap.hpp"
#include "scheme/SchemeType.hpp"
#include "storage/StringArrayViewer.hpp"
#include "storage/StringPointerArrayViewer.hpp"

namespace btrblocks {

using namespace std;

template <>
SIZE ArrowColumnChunkCompressor<ColumnType::STRING>::getChunkByteSize(
    const shared_ptr<arrow::Array>& chunk) {
  auto array = static_pointer_cast<arrow::StringArray>(chunk);
  return array->total_values_length() + array->length() * (sizeof(u32) + 1);
}

template <>
SIZE ArrowColumnChunkCompressor<ColumnType::STRING>::compress(const shared_ptr<arrow::Array>& chunk,
                                                              ColumnChunkMeta* meta) {
  auto& config = BtrBlocksConfig::get();

  auto array = std::static_pointer_cast<arrow::StringArray>(chunk);

  auto dataSize = (array->length() + 1) * sizeof(u32) + array->total_values_length();

  StringStats stats = StringStats::generateArrowStats(array, dataSize);

  StringScheme& preferred_scheme =
      StringSchemePicker::chooseScheme(stats, config.strings.max_cascade_depth);

  auto arrowStringArrayViewer = ArrowStringArrayViewer(array);

  u32 compressedColumnSize = 0;

  meta->compression_type = static_cast<u8>(preferred_scheme.schemeType());

  if (preferred_scheme.schemeType() == StringSchemeType::UNCOMPRESSED) {
    // Uncompressed string arrow arrays are not completely compatible, therefore here have to provide the values in the StringArrayViewer format
    arrowStringArrayViewer.transformToRegularStringArrayViewer();
  }

  compressedColumnSize = preferred_scheme.compress(arrowStringArrayViewer,
                                                   array->null_bitmap_data(), meta->data, stats);

  return compressedColumnSize;
}

template <>
shared_ptr<arrow::Array> ArrowColumnChunkCompressor<ColumnType::STRING>::decompress(
    ColumnChunkMeta* columnChunk) {
  auto tupleCount = columnChunk->tuple_count;

  const auto used_compression_scheme = static_cast<StringSchemeType>(columnChunk->compression_type);

  unique_ptr<StringScheme>& scheme =
      SchemePool::available_schemes->string_schemes[used_compression_scheme];

  auto size =
      scheme->getDecompressedSizeNoCopy(columnChunk->data, columnChunk->tuple_count, nullptr);
  SIZE destinationSize = size + 8 + SIMD_EXTRA_BYTES + 4096;
  auto destination = makeBytesArray(destinationSize);

  auto bitmap = decompressBitmap(columnChunk);
  auto bitmapData = bitmap == nullptr ? nullptr : bitmap->data();

  // TODO: Write a wrapper to turn the string pointer arrays to different arrow array types
  // (DictArray, REE Array, etc.) so we dont have to build these huge copied arrays
  scheme->decompressNoCopy(destination.get(), nullptr, columnChunk->data, tupleCount, 0);

  auto pointerViewer = StringPointerArrayViewer(destination.get());

  arrow::StringBuilder stringBuilder;
  stringBuilder.Resize(tupleCount).ok();

  for (SIZE i = 0; i < tupleCount; ++i) {
    if (bitmap == nullptr || (bitmapData[i / 8] & (1 << (i & 0x07))) != 0) {
      stringBuilder.Append(pointerViewer(i)).ok();
    } else {
      stringBuilder.AppendNull().ok();
    }
  }

  return stringBuilder.Finish().ValueOrDie();
}

}  // namespace btrblocks