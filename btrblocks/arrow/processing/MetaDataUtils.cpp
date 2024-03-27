#include "MetaDataUtils.hpp"
#include <arrow/api.h>
#include "../ArrowTypeUtils.hpp"
#include "arrow/ArrowMetaData.hpp"
#include "common/Units.hpp"

namespace btrblocks {

using namespace btrblocks;
using namespace std;

vector<SIZE> MetaDataUtils::resolveColumnIndices(ArrowMetaData& metadata,
                                                 const vector<string>& columns) {
  vector<SIZE> columnIndices{};

  for (auto& column : columns) {
    auto columnIndex = find_if(metadata.columnMeta.begin(), metadata.columnMeta.end(),
                               [&](const auto& columnMeta) { return columnMeta.name == column; });

    columnIndices.push_back(columnIndex - metadata.columnMeta.begin());
  }

  return columnIndices;
}

shared_ptr<arrow::Schema> MetaDataUtils::resolveSchema(ArrowMetaData& metadata,
                                                       vector<SIZE>& columnIndices) {
  arrow::FieldVector fields{};

  for (auto& columnIndex : columnIndices) {
    auto columnMeta = metadata.columnMeta[columnIndex];

    fields.push_back(
        arrow::field(columnMeta.name, ArrowTypeUtils::arrowTypeFromColumnType(columnMeta.type)));
  }

  return arrow::schema(fields);
}

}  // namespace btrblocks