#include <arrow/array/array_base.h>
#include <arrow/array/array_binary.h>
#include <arrow/array/builder_binary.h>
#include <arrow/type.h>
#include <arrow/type_fwd.h>
#include <memory>
#include <stdexcept>
#include <tuple>
#include <vector>
#include "ArrowTableChunkCompressor.hpp"
#include "arrow/chunkwise/ArrowTableChunk.hpp"
#include "btrblocks.hpp"
#include "common/Units.hpp"
#include "compression/Compressor.hpp"
#include "compression/Datablock.hpp"
#include "compression/SchemePicker.hpp"
#include "scheme/SchemeType.hpp"
#include "storage/StringArrayViewer.hpp"

namespace btrblocks {

ArrowTableChunk ArrowTableChunkCompressor::decompress(std::vector<u8>& compressed_data) {
  auto& blockMeta = *reinterpret_cast<DatablockMeta*>(compressed_data.data());

  auto& tupleCount = blockMeta.count;
  auto columnCount = blockMeta.column_count;

  // Prepare arrow schema
  std::vector<std::shared_ptr<arrow::Field>> fields(columnCount);
  std::vector<std::shared_ptr<arrow::Array>> columns(columnCount);

  for (SIZE columnIndex = 0; columnIndex < columnCount; ++columnIndex) {
    auto columnMeta = blockMeta.attributes_meta[columnIndex];

    switch (columnMeta.column_type) {
      case ColumnType::INTEGER: {
        fields[columnIndex] = arrow::field("int_col", arrow::int32());

        auto& compressionScheme =
            IntegerSchemePicker::MyTypeWrapper::getScheme(columnMeta.compression_type);

        auto newBuffer = arrow::AllocateBuffer(tupleCount * sizeof(INTEGER) + SIMD_EXTRA_BYTES);
        shared_ptr<arrow::Buffer> dataBuffer{newBuffer.ValueOrDie().release()};
        auto dest = dataBuffer->mutable_data();

        compressionScheme.decompress(reinterpret_cast<INTEGER*>(dest), nullptr,
                                     compressed_data.data() + columnMeta.offset, tupleCount, 0);

        auto nullmapBuffer = decompressBitmap(columnMeta, tupleCount,
                                              compressed_data.data() + columnMeta.nullmap_offset);

        std::vector<std::shared_ptr<arrow::Buffer>> buffers = {nullmapBuffer, dataBuffer};
        auto arrayData = arrow::ArrayData::Make(arrow::int32(), tupleCount, buffers);

        columns[columnIndex] = arrow::MakeArray(arrayData);

        break;
      }

      case ColumnType::DOUBLE: {
        fields[columnIndex] = arrow::field("double_col", arrow::float64());

        auto& compressionScheme =
            DoubleSchemePicker::MyTypeWrapper::getScheme(columnMeta.compression_type);

        auto newBuffer = arrow::AllocateBuffer(tupleCount * sizeof(DOUBLE) + SIMD_EXTRA_BYTES);
        shared_ptr<arrow::Buffer> dataBuffer{newBuffer.ValueOrDie().release()};
        auto dest = dataBuffer->mutable_data();

        compressionScheme.decompress(reinterpret_cast<DOUBLE*>(dest), nullptr,
                                     compressed_data.data() + columnMeta.offset, tupleCount, 0);

        auto nullmapBuffer = decompressBitmap(columnMeta, tupleCount,
                                              compressed_data.data() + columnMeta.nullmap_offset);

        std::vector<std::shared_ptr<arrow::Buffer>> buffers = {nullmapBuffer, dataBuffer};
        auto arrayData = arrow::ArrayData::Make(arrow::float64(), tupleCount, buffers);

        columns[columnIndex] = arrow::MakeArray(arrayData);

        break;
      }

      case ColumnType::STRING: {
        fields[columnIndex] = arrow::field("string_col", arrow::utf8());

        const auto used_compression_scheme =
            static_cast<StringSchemeType>(columnMeta.compression_type);

        auto& scheme = SchemePool::available_schemes->string_schemes[used_compression_scheme];

        auto size = scheme->getDecompressedSizeNoCopy(compressed_data.data() + columnMeta.offset,
                                                      tupleCount, nullptr);

        SIZE destinationSize = size + 8 + SIMD_EXTRA_BYTES + 4096;

        auto destination = makeBytesArray(destinationSize);

        scheme->decompressNoCopy(destination.get(), nullptr,
                                 compressed_data.data() + columnMeta.offset, tupleCount, 0);

        auto pointerViewer = StringPointerArrayViewer(destination.get());

        arrow::StringBuilder stringBuilder;
        stringBuilder.Resize(tupleCount).ok();

        auto nullmapBuffer = decompressBitmap(columnMeta, tupleCount,
                                              compressed_data.data() + columnMeta.nullmap_offset);
        auto bitmap = nullmapBuffer == nullptr ? nullptr : nullmapBuffer->data();

        for (SIZE i = 0; i < tupleCount; ++i) {
          if (bitmap == nullptr || (bitmap[i / 8] & (1 << (i & 0x07))) != 0) {
            stringBuilder.Append(pointerViewer(i)).ok();
          } else {
            stringBuilder.AppendNull().ok();
          }
        }

        auto a = stringBuilder.Finish().ValueOrDie();

        columns[columnIndex] = a;

        break;
      }

      default: {
        throw std::runtime_error("Only INT and DOUBLE are allowed with arrow atm");
      }
    }
  }

  std::shared_ptr<arrow::Schema> schema = arrow::schema(fields);

  return ArrowTableChunk(schema, columns);
}

shared_ptr<arrow::Buffer> ArrowTableChunkCompressor::decompressBitmap(ColumnMeta& columnChunk,
                                                                      SIZE tupleCount,
                                                                      u8* src) {
  if (columnChunk.bitmap_type == BitmapType::ALLONES) {
    return nullptr;
  }

  auto nullmapBuffer = arrow::AllocateEmptyBitmap(tupleCount).ValueOrDie();

  if (columnChunk.bitmap_type == BitmapType::ALLZEROS) {
    return nullmapBuffer;
  }

  auto nullmapDest = nullmapBuffer->mutable_data();
  auto bitmap_out = make_unique<BitmapWrapper>(src, columnChunk.bitmap_type, tupleCount);

  bitmap_out->writeArrowBITMAP(nullmapDest);
  return nullmapBuffer;
}

}  // namespace btrblocks
