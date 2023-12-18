#include "ArrowTableChunkCompressor.hpp"
#include <arrow/array/array_binary.h>
#include <arrow/type.h>
#include <arrow/type_fwd.h>
#include <sys/mman.h>
#include <cstdint>
#include <cstring>
#include <memory>
#include <tuple>
#include <vector>
#include "btrblocks.hpp"
#include "common/Units.hpp"
#include "compression/Compressor.hpp"
#include "compression/Datablock.hpp"
#include "compression/SchemePicker.hpp"
#include "scheme/SchemeType.hpp"
#include "storage/StringArrayViewer.hpp"

namespace btrblocks {

std::tuple<OutputBlockStats, std::vector<u8>> ArrowTableChunkCompressor::compress(ArrowTableChunk& chunk) {
  auto& config = BtrBlocksConfig::get();

  SIZE columnCount = chunk.columnCount();
  SIZE chunkSizeBytes = chunk.approxDataSizeBytes();
  const u32 datablockMetaBufferSize = sizeof(DatablockMeta) + (columnCount * sizeof(ColumnMeta));

  std::vector<u8> output(datablockMetaBufferSize + chunkSizeBytes * 2);
  SIZE outputWriteOffset = datablockMetaBufferSize;

  // Initilize datablock meta
  auto datablockMeta = reinterpret_cast<DatablockMeta*>(output.data());
  datablockMeta->count = chunk.tupleCount();
  datablockMeta->column_count = chunk.columnCount();

  // Init the output block statistics
  auto outputBlockStats = initOutputStats_(columnCount, datablockMetaBufferSize);

  // Iterate columns and compress while updating meta info
  for (SIZE i = 0, limit = chunk.columnCount(); i < limit; ++i) {
    auto& columnMeta = datablockMeta->attributes_meta[i];
    auto [field, column] = chunk.getSchemeAndColumn(i);

    u32 compressedColumnSize = 0;

    // Actually compress column

    // INTEGER
    if (field->type()->Equals(arrow::int32())) {
      columnMeta.column_type = ColumnType::INTEGER;

      auto array = std::static_pointer_cast<arrow::Int32Array>(column);
      btrblocks::IntegerSchemePicker::compress(
          array->raw_values(), array->null_bitmap_data(), output.data() + outputWriteOffset,
          array->length(), 3, compressedColumnSize, columnMeta.compression_type);
    }

    // DOUBLE
    if (field->type()->Equals(arrow::float64())) {
      columnMeta.column_type = ColumnType::DOUBLE;

      auto array = std::static_pointer_cast<arrow::DoubleArray>(column);
      btrblocks::DoubleSchemePicker::compress(
          array->raw_values(), array->null_bitmap_data(), output.data() + outputWriteOffset,
          array->length(), 3, compressedColumnSize, columnMeta.compression_type);
    }

    // STRINGS
    if (field->type()->Equals(arrow::utf8())) {
      columnMeta.column_type = ColumnType::STRING;

      auto array = std::static_pointer_cast<arrow::StringArray>(column);

      auto tupleCount = array->length();

      auto slotsSize = static_cast<INTEGER>((tupleCount + 1) * sizeof(StringArrayViewer::Slot));

      auto resultingSize = slotsSize + array->total_values_length();

      auto* arrayCopy = malloc(resultingSize);

      auto offsetPtr = reinterpret_cast<StringArrayViewer::Slot*>(arrayCopy);

      for (int64_t i = 0; i < tupleCount; ++i) {
        auto offset = static_cast<INTEGER>(array->value_offset(i));
        (offsetPtr + i)->offset = offset + slotsSize;
      }

      auto lastPtr = (offsetPtr + tupleCount);

      lastPtr->offset = array->value_offset(tupleCount - 1) + array->value_length(tupleCount - 1) + slotsSize;

      auto dataPtr = reinterpret_cast<u8*>(offsetPtr + tupleCount + 1);

      memcpy(dataPtr, array->raw_data(), array->total_values_length());
      
      StringStats stats = StringStats::generateStats(
            StringArrayViewer(reinterpret_cast<u8*>(arrayCopy)), array->null_bitmap_data(),
            tupleCount, resultingSize);

      StringScheme& preferred_scheme =
          StringSchemePicker::chooseScheme(stats, config.strings.max_cascade_depth);
      
      columnMeta.compression_type = static_cast<u8>(preferred_scheme.schemeType());

      //ThreadCache::get().compression_level++;

      const StringArrayViewer str_viewer(reinterpret_cast<u8*>(offsetPtr));

      compressedColumnSize = preferred_scheme.compress(str_viewer, array->null_bitmap_data(),
                                                    output.data() + outputWriteOffset, stats);

      //ThreadCache::get().compression_level--;

      free(arrayCopy);
    }

    // Update meta info
    columnMeta.offset = outputWriteOffset;
    outputWriteOffset += compressedColumnSize;

    // Update null bitmap info
    if (column->null_bitmap_data() != nullptr) {
      columnMeta.nullmap_offset = outputWriteOffset;

      auto [nullmapSize, bitmapType] = bitmap::RoaringBitmap::compress(
          column->null_bitmap_data(), output.data() + outputWriteOffset, column->length());

      columnMeta.bitmap_type = bitmapType;
      outputWriteOffset += nullmapSize;

      outputBlockStats.nullmap_sizes[i] = nullmapSize;
      outputBlockStats.total_nullmap_size += nullmapSize;
    }

    // Update meta info
    outputBlockStats.used_compression_schemes[i] = columnMeta.compression_type;
    outputBlockStats.data_sizes[i] = compressedColumnSize;
    outputBlockStats.total_data_size += compressedColumnSize;
  }

  // Datablock size has been initialized with meta size
  outputBlockStats.total_db_size += outputBlockStats.total_nullmap_size + outputBlockStats.total_data_size; 
  outputBlockStats.compression_ratio = static_cast<double>(chunkSizeBytes) / static_cast<double>(outputBlockStats.total_data_size);

  datablockMeta->size = outputBlockStats.total_db_size;

  // Resize output to fit actual block size
  output.resize(outputBlockStats.total_db_size);

  return std::make_tuple(outputBlockStats, output);
}

OutputBlockStats ArrowTableChunkCompressor::initOutputStats_(SIZE columnCount,
                                                             SIZE datablockMetaBufferSize) {
  return {.data_sizes = vector<SIZE>(columnCount, 0),
          .nullmap_sizes = vector<SIZE>(columnCount, 0),
          .used_compression_schemes = vector<u8>(columnCount, 255),
          .total_data_size = 0,
          .total_nullmap_size = 0,
          .total_db_size = datablockMetaBufferSize,  // Will be updated when compressing the columns
          .compression_ratio = 0};
}

void handleColumnNullmap(ColumnMeta& meta,
                         OutputBlockStats& outputStats,
                         BITMAP* nullBitmap,
                         SIZE tupleCount,
                         SIZE& write_offset,
                         u8* output) {
  if (nullBitmap != nullptr) {
    meta.nullmap_offset = write_offset;
    auto [nullmap_size, bitmap_type] =
        bitmap::RoaringBitmap::compress(nullBitmap, output + write_offset, tupleCount);
    meta.bitmap_type = bitmap_type;
    write_offset += nullmap_size;
  }
}

}  // namespace btrblocks
