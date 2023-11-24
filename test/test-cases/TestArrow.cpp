#include "btrblocks.hpp"
#include "gtest/gtest.h"
#include "scheme/SchemePool.hpp"
#include "arrow/api.h"
#include "arrow/ArrowTableCompressor.hpp"
// -------------------------------------------------------------------------------------
using namespace btrblocks;
// -------------------------------------------------------------------------------------

TEST(Arrow, SingleIntegerColumn) {
  BtrBlocksConfig::get().integers.schemes = {IntegerSchemeType::UNCOMPRESSED, IntegerSchemeType::ONE_VALUE};
  SchemePool::refresh();
  int64_t numRows = 1000000;
  arrow::Int32Builder builder;
  for (int i = 0; i < numRows; ++i) {
    arrow::Status s = builder.Append(i / 1000);
  }

  arrow::Status s = builder.AppendEmptyValue();

  std::shared_ptr<arrow::Array> array = builder.Finish().ValueOrDie();

  std::shared_ptr<arrow::Schema> schema =
      arrow::schema({arrow::field("int_col", arrow::int32())});

  std::shared_ptr<arrow::RecordBatch> batch = arrow::RecordBatch::Make(schema,numRows, {array});

  ArrowTableCompressor::compress(batch);


}