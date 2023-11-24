#include "ArrowTableChunk.hpp"

namespace btrblocks {

std::pair<std::shared_ptr<arrow::Field>, std::shared_ptr<arrow::Array>>
ArrowTableChunk::getSchemeAndColumn(size_t i) {
  return make_pair(schema->field(i), columns[i]);
}

}  // namespace btrblocks