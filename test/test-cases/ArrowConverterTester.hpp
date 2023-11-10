#include <arrow/api.h>
#include "TestHelper.hpp"
#include "btrfiles.hpp"
#include "compression/Datablock.hpp"
#include "gtest/gtest.h"
#include "scheme/SchemePool.hpp"
#include "storage/Relation.hpp"

template <typename CT, typename T>
void checkIntegerArrowTableToRelationConversion(std::vector<T>& expected_result,
                                         const std::shared_ptr<arrow::DataType>& arrowColumnType) {
  arrow::MemoryPool* pool = arrow::default_memory_pool();

  arrow::AdaptiveIntBuilder int_builder(sizeof(T), pool);

  for (T& val : expected_result) {
    arrow::Status s = int_builder.Append(val);
  }

  std::shared_ptr<arrow::Array> int_array;
  arrow::Status s = int_builder.Finish(&int_array);

  std::vector<std::shared_ptr<arrow::Field>> schema_vector{arrow::field("col", arrowColumnType)};

  auto schema = std::make_shared<arrow::Schema>(schema_vector);

  std::shared_ptr<arrow::Table> table = arrow::Table::Make(schema, {int_array});

  btrblocks::Relation relation = btrblocks::files::readArrowTable(table);

  for (auto& c : relation.columns) {
    auto& vec = std::get<btrblocks::Vector<CT>>(c.data);
    for (size_t i = 0, limit = expected_result.size(); i < limit; ++i) {
      ASSERT_TRUE(expected_result[i] == static_cast<T>(vec[i]));
    }
  }

  btrblocks::Datablock data_block(relation);

  TestHelper::CheckRelationCompression(relation, data_block);
}