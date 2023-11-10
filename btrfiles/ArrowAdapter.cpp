#include "arrow/api.h"
#include "storage/Relation.hpp"

using namespace btrblocks;

bool arrowDataTypeIsBtrInt(const std::shared_ptr<arrow::DataType>& type) {
  std::vector v = {arrow::int32(), arrow::int8(), arrow::int16()};
  return std::any_of(v.begin(), v.end(), [&](const auto& t) { return type->Equals(t); });
}
bool arrowDataTypeIsBtrDouble(const std::shared_ptr<arrow::DataType>& type) {
  std::vector v = {arrow::float16(), arrow::float32(), arrow::float64()};
  return std::any_of(v.begin(), v.end(), [&](const auto& t) { return type->Equals(t); });
}

ColumnType getColumnTypeFromArrowType(const std::shared_ptr<arrow::DataType>& type) {
  if (arrowDataTypeIsBtrInt(type)) {
    return ColumnType::INTEGER;
  }
  if (arrowDataTypeIsBtrDouble(type)) {
    return ColumnType::DOUBLE;
  }
  return ColumnType::UNDEFINED;
}

template <typename T>
btrblocks::Vector<T> vectorFromChunkedArray(const std::shared_ptr<arrow::ChunkedArray>& chunkedArray, const std::shared_ptr<arrow::DataType>& dataType) {
  btrblocks::Vector<T> data(chunkedArray->length());
  size_t g = 0;
  for (auto& array : chunkedArray->chunks()) {
    for (int64_t i = 0, limit = array->length(); i < limit; ++i) {

      if(dataType->Equals(arrow::int32())) {
        data[g + i] = static_cast<T>(std::static_pointer_cast<arrow::Int32Array>(array)->Value(i));
      }
      if(dataType->Equals(arrow::int16())) {
        data[g + i] = static_cast<T>(std::static_pointer_cast<arrow::Int16Array>(array)->Value(i));
      }
    }
    g += array->length();
  }
  return data;
}
namespace btrblocks::files {

Relation readArrowTable(std::shared_ptr<arrow::Table>& table) {
  Relation result;
  result.columns.reserve(table->num_columns());
  const auto& columns = table->schema()->fields();

  for (u32 column_i = 0; column_i < columns.size(); ++column_i) {
    const auto& field = columns[column_i];

    const auto column_name = field->name();
    const auto& column_type = field->type();

    ColumnType btr_column_type = getColumnTypeFromArrowType(column_type);

    auto& column = table->columns()[column_i];

    if (btr_column_type == ColumnType::INTEGER) {
      result.addColumn({column_name, vectorFromChunkedArray<INTEGER>(column, column_type)});
    }
  }
  return result;
}
}  // namespace btrblocks::files