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

  auto slotsSize = static_cast<INTEGER>((tupleCount + 1) * sizeof(StringArrayViewer::Slot));

  auto resultingSize = slotsSize + array->total_values_length();

  auto* arrayCopy = malloc(resultingSize);

  auto offsetPtr = reinterpret_cast<StringArrayViewer::Slot*>(arrayCopy);

  auto valueOffset = 0;

  for (int64_t i = 0; i < tupleCount; ++i) {
    (offsetPtr + i)->offset = valueOffset + slotsSize;
    valueOffset += array->value_length(i);
  }

  auto lastPtr = (offsetPtr + tupleCount);

  lastPtr->offset = valueOffset + slotsSize;

  auto dataPtr = reinterpret_cast<u8*>(offsetPtr + tupleCount + 1);

  // Data pointers do not care about being a slice and just return the original array's pointers
  // As the value_offset does the same like this we get the pointer of the data in this slice/ if we
  // have a copied array just get the raw data pointer as value_offset returns 0
  auto arrowDataPtr = array->raw_data() + array->value_offset(0);

  memcpy(dataPtr, arrowDataPtr, array->total_values_length());

  StringStats stats =
      StringStats::generateStats(StringArrayViewer(reinterpret_cast<u8*>(arrayCopy)),
                                 array->null_bitmap_data(), tupleCount, resultingSize);

  StringScheme& preferred_scheme =
      StringSchemePicker::chooseScheme(stats, config.strings.max_cascade_depth);

  meta->compression_type = static_cast<u8>(preferred_scheme.schemeType());

  const StringArrayViewer str_viewer(reinterpret_cast<u8*>(offsetPtr));

  auto compressedColumnSize = preferred_scheme.compress(str_viewer, array->null_bitmap_data(), meta->data, stats);

  free(arrayCopy);

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