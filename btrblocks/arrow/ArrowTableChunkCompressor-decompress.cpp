#include "ArrowTableChunkCompressor.hpp"
#include <arrow/array/array_base.h>
#include <arrow/type.h>
#include <arrow/type_fwd.h>
#include <memory>
#include <stdexcept>
#include <tuple>
#include <vector>
#include "arrow/ArrowTableChunk.hpp"
#include "btrblocks.hpp"
#include "common/Units.hpp"
#include "compression/Compressor.hpp"
#include "compression/Datablock.hpp"
#include "compression/SchemePicker.hpp"
#include "scheme/SchemeType.hpp"

namespace btrblocks {

ArrowTableChunk ArrowTableChunkCompressor::decompress(std::tuple<OutputBlockStats, std::vector<u8>>& compressed_chunk) {

  auto& [blockStats, data] = compressed_chunk;
  auto& blockMeta= *reinterpret_cast<DatablockMeta*>(data.data());

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

        std::vector<INTEGER> destination(tupleCount);

        compressionScheme.decompress(
          destination.data(), 
          nullptr, 
          data.data() + columnMeta.offset,
          tupleCount, 
          0);


        arrow::Int32Builder int32builder;
        auto status = int32builder.Resize(tupleCount);

        status = int32builder.AppendValues(destination);

        columns[columnIndex] = int32builder.Finish().ValueOrDie();

        break;
      }

      case ColumnType::DOUBLE: {
        fields[columnIndex] = arrow::field("double_col", arrow::int32());

        auto& compressionScheme = DoubleSchemePicker::MyTypeWrapper::getScheme(columnMeta.compression_type);

        std::vector<DOUBLE> destination(tupleCount);

        compressionScheme.decompress(
          destination.data(), 
          nullptr,
          data.data() + columnMeta.offset,
          tupleCount,
          0);


        arrow::DoubleBuilder dobuleBuilder;
        auto status = dobuleBuilder.Resize(tupleCount);

        status = dobuleBuilder.AppendValues(destination);

        columns[columnIndex] = dobuleBuilder.Finish().ValueOrDie();

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
