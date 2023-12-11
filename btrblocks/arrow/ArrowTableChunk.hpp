#pragma once
#include <arrow/api.h>
#include "common/Units.hpp"

namespace btrblocks {

struct ArrowTableChunk {
 public:
  explicit ArrowTableChunk(const std::shared_ptr<arrow::Schema> schema, arrow::ArrayVector columns)
      : schema(schema), columns(std::move(columns)) {}

  std::pair<std::shared_ptr<arrow::Field>, std::shared_ptr<arrow::Array>> getSchemeAndColumn(
      size_t i);

  SIZE tupleCount() {
    return columns[0]->length();
  };

  SIZE columnCount() {
    return columns.size();
  }
 private:
  const std::shared_ptr<arrow::Schema> schema;  // Original table for metadata access
  const arrow::ArrayVector columns;             // Slices of the original table's columns
};

}  // namespace btrblocks