#pragma once
#include <arrow/api.h>
#include <arrow/array/array_base.h>
#include <arrow/array/array_binary.h>
#include <arrow/type_fwd.h>
#include <memory>
#include <numeric>
#include "common/Units.hpp"

namespace btrblocks {

struct ArrowTableChunk {
 public:
  explicit ArrowTableChunk(std::shared_ptr<arrow::Schema> schema, arrow::ArrayVector columns)
      : schema(std::move(schema)), columns(std::move(columns)) {}

  std::pair<std::shared_ptr<arrow::Field>, std::shared_ptr<arrow::Array>> getSchemeAndColumn(
      size_t i);

  SIZE tupleCount() {
    return columns[0]->length();
  };

  SIZE columnCount() {
    return columns.size();
  }

  SIZE approxDataSizeBytes();
  
 private:
  const std::shared_ptr<arrow::Schema> schema;  // Original table for metadata access
  const arrow::ArrayVector columns;             // Slices of the original table's columns
};

}  // namespace btrblocks