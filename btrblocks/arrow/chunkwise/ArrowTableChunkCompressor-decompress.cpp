#include "ArrowTableChunkCompressor.hpp"
#include <arrow/array/array_base.h>
#include <arrow/array/array_binary.h>
#include <arrow/array/builder_binary.h>
#include <arrow/type.h>
#include <arrow/type_fwd.h>
#include <memory>
#include <stdexcept>
#include <tuple>
#include <vector>
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

  auto& blockMeta= *reinterpret_cast<DatablockMeta*>(compressed_data.data());

  auto& tupleCount = blockMeta.count;
  auto columnCount = blockMeta.column_count;

  // Prepare arrow schema
  std::vector<std::shared_ptr<arrow::Field>> fields(columnCount);
  std::vector<std::shared_ptr<arrow::Array>> columns(columnCount);

  for (SIZE columnIndex = 0; columnIndex < columnCount; ++columnIndex) {

    auto columnMeta = blockMeta.attributes_meta[columnIndex];

    // TODO: Bitmap stuff

    switch (columnMeta.column_type) {

      case ColumnType::INTEGER: {

        fields[columnIndex] = arrow::field("int_col", arrow::int32());

        auto& compressionScheme = IntegerSchemePicker::MyTypeWrapper::getScheme(columnMeta.compression_type);

        auto destination = makeBytesArray(tupleCount * sizeof(INTEGER) + SIMD_EXTRA_BYTES);

        compressionScheme.decompress(
          reinterpret_cast<INTEGER *>(destination.get()), 
          nullptr,
          compressed_data.data() + columnMeta.offset,
          tupleCount, 
          0);


        arrow::Int32Builder int32builder;
        int32builder.Resize(tupleCount).ok();

        auto values = std::vector<INTEGER>(reinterpret_cast<INTEGER *>(destination.get()), reinterpret_cast<INTEGER *>(destination.get()) + tupleCount);

        int32builder.AppendValues(values).ok();

        columns[columnIndex] = int32builder.Finish().ValueOrDie();

        break;
      }

      case ColumnType::DOUBLE: {
        fields[columnIndex] = arrow::field("double_col", arrow::int32());

        auto& compressionScheme = DoubleSchemePicker::MyTypeWrapper::getScheme(columnMeta.compression_type);

        auto destination = makeBytesArray(tupleCount * sizeof(DOUBLE) + SIMD_EXTRA_BYTES);

        compressionScheme.decompress(
          reinterpret_cast<DOUBLE*>(destination.get()), 
          nullptr,
          compressed_data.data() + columnMeta.offset,
          tupleCount,
          0);


        arrow::DoubleBuilder doubleBuilder;
        doubleBuilder.Resize(tupleCount).ok();

        auto values = std::vector<DOUBLE>(reinterpret_cast<DOUBLE *>(destination.get()), reinterpret_cast<DOUBLE *>(destination.get()) + tupleCount);

        doubleBuilder.AppendValues(values).ok();

        columns[columnIndex] = doubleBuilder.Finish().ValueOrDie();

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

        scheme->decompressNoCopy(destination.get(), nullptr, compressed_data.data() + columnMeta.offset, tupleCount, 0);
        
        auto pointerViewer = StringPointerArrayViewer(destination.get());

        arrow::StringBuilder stringBuilder;
        stringBuilder.Resize(tupleCount).ok();

        for (SIZE i = 0; i < tupleCount; ++i) {
          stringBuilder.Append(pointerViewer(i)).ok();
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

}  // namespace btrblocks
