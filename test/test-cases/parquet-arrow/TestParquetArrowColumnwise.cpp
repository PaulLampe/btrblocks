#include "TestParquetArrowDataLoader.hpp"
#include "arrow/api.h"
#include "btrblocks.hpp"
#include "common/Units.hpp"
#include "gtest/gtest.h"
#include "scheme/SchemePool.hpp"
#include "scheme/SchemeType.hpp"
#include "test-cases/TestHelper.hpp"
#include "../arrow/CheckAndCompress.hpp"

TEST(ParquetArrowColumnwise, Begin) {
  BtrBlocksConfig::get().integers.schemes = defaultIntegerSchemes();
  /*  
  {IntegerSchemeType::UNCOMPRESSED, IntegerSchemeType::ONE_VALUE, IntegerSchemeType::DICT,
   IntegerSchemeType::RLE,          IntegerSchemeType::PFOR,      IntegerSchemeType::BP};
  */
  BtrBlocksConfig::get().doubles.schemes = defaultDoubleSchemes(); 
  /*
  {DoubleSchemeType::UNCOMPRESSED, DoubleSchemeType::ONE_VALUE,
   DoubleSchemeType::DICT,         DoubleSchemeType::RLE,
   DoubleSchemeType::FREQUENCY,    DoubleSchemeType::PSEUDODECIMAL};
  */
  BtrBlocksConfig::get().strings.schemes = defaultStringSchemes();
  /*
  {StringSchemeType::UNCOMPRESSED, StringSchemeType::ONE_VALUE, StringSchemeType::DICT,
   StringSchemeType::FSST};
  */
  SchemePool::refresh();
}

TEST(ParquetArrowColumnwise, TestNonNullIntegerColumns) {
  // auto table = loadTableFromParquet({std::string(TEST_DATASET("parquet/YaleLanguages_1_none.parquet"))}, {"bibid", "mfhdid"});

  // checkCompressTable(table);

  auto table = loadTableFromParquet({std::string(TEST_DATASET("parquet/parquet_none_arrow_0.parquet"))}, {"npi", "totaldaysupply"});

  checkCompressTableColumnwise(table);
}

TEST(ParquetArrowColumnwise, TestNonNullDoubleColumns) {
  auto table = loadTableFromParquet({std::string(TEST_DATASET("parquet/parquet_none_arrow_0.parquet"))}, {"calculation3170826185505725", "calculation7130826185400024", "totaldrugcost"});

  checkCompressTableColumnwise(table);
}

TEST(ParquetArrowColumnwise, TestNonNullVarcharColumns) {
  auto table = loadTableFromParquet({std::string(TEST_DATASET("parquet/YaleLanguages_1_none.parquet"))}, {"bibformat"});

  checkCompressTableColumnwise(table);
}