#include <iostream>
#include "ArrowConverterTester.hpp"
#include "TestHelper.hpp"
#include "gtest/gtest.h"
#include "scheme/SchemePool.hpp"

TEST(Arrow, Begin) {
  BtrBlocksConfig::get().integers.schemes = defaultIntegerSchemes();
  BtrBlocksConfig::get().doubles.schemes = defaultDoubleSchemes();
  BtrBlocksConfig::get().strings.schemes = defaultStringSchemes();
  btrblocks::SchemePool::refresh();
}

TEST(Arrow, ArrowInt16ConversionTest) {
  auto length = std::numeric_limits<SMALLINT>::max();
  std::vector<SMALLINT> expected_result(length);
  for (SMALLINT i = 0; i < length; ++i) {
    expected_result[i] = i % 100;
  }
  checkIntegerArrowTableToRelationConversion<INTEGER, SMALLINT>(expected_result, arrow::int16());
}

TEST(Arrow, ArrowInt32ConversionTest) {
  auto length = 640000;
  std::vector<INTEGER> expected_result(length);
  for (INTEGER i = 0; i < length; ++i) {
    expected_result[i] = i % 100;
  }
  checkIntegerArrowTableToRelationConversion<INTEGER, INTEGER>(expected_result, arrow::int32());
}