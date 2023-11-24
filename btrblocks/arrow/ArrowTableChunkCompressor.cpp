#include "ArrowTableChunkCompressor.hpp"
#include "btrblocks.hpp"
#include "compression/SchemePicker.hpp"

namespace btrblocks {

std::vector<uint8_t> ArrowTableChunkCompressor::compress(ArrowTableChunk& chunk) {
  std::vector<uint8_t> output(8 * chunk.tupleCount());

  // iterate columns and compress
  for (size_t i = 0, limit = chunk.columnCount(); i < limit; ++i) {
    auto [field, column] = chunk.getSchemeAndColumn(i);

    if (field->type()->Equals(arrow::int32())) {
      auto array = std::static_pointer_cast<arrow::Int32Array>(column);
      uint32_t after_size;
      uint8_t scheme_code;
      btrblocks::IntegerSchemePicker::compress(array->raw_values(),
                                               array->null_bitmap_data(), output.data(),
                                               array->length(), 3, after_size, scheme_code);
    }
  }
  return output;
}

}  // namespace btrblocks
