#pragma once
#include <arrow/api.h>

namespace btrblocks {

struct ArrowTableChunk {
 public:
  explicit ArrowTableChunk(const std::shared_ptr<arrow::Schema>& schema, arrow::ArrayVector columns)
      : schema(schema), columns(columns) {}

  std::pair<std::shared_ptr<arrow::Field>, std::shared_ptr<arrow::Array>> getSchemeAndColumn(
      size_t i);

  size_t tupleCount() {
    return std::accumulate(columns.begin(), columns.end(), 0,
                           [](const auto& agg, const auto& col) { return agg + col->length(); });
  };

  size_t columnCount() {
    return columns.size();
  }
 private:
  const std::shared_ptr<arrow::Schema> schema;  // Original table for metadata access
  const arrow::ArrayVector columns;             // Slices of the original table's columns
};

}  // namespace btrblocks