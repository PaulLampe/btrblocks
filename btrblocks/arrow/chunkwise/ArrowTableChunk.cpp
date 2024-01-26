#include "ArrowTableChunk.hpp"

namespace btrblocks {

std::pair<std::shared_ptr<arrow::Field>, std::shared_ptr<arrow::Array>>
ArrowTableChunk::getSchemeAndColumn(size_t i) {
  return make_pair(schema->field(i), columns[i]);
}

SIZE ArrowTableChunk::approxDataSizeBytes() {
  SIZE size = 0;
  for (SIZE i = 0; i < columns.size(); ++i) {
    auto[field, array] = getSchemeAndColumn(i);

    if (field->type()->Equals(arrow::int32())) {
      size += sizeof(INTEGER) * array->length();
    }
    if (field->type()->Equals(arrow::float64())) {
      size += sizeof(DOUBLE) * array->length();
    }

    if (field->type()->Equals(arrow::utf8())) {
      auto strArray = std::static_pointer_cast<arrow::StringArray>(array);
      size += strArray->total_values_length() + strArray->length() * sizeof(INTEGER);
    } // bitmap
  }
  return size;
}

}  // namespace btrblocks