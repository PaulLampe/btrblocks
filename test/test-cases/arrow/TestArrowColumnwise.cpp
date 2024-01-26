#include "./TestArrowDataLoader.hpp"
#include "arrow/api.h"
#include "btrblocks.hpp"
#include "common/Units.hpp"
#include "gtest/gtest.h"
#include "scheme/SchemePool.hpp"
#include "scheme/SchemeType.hpp"
#include "test-cases/TestHelper.hpp"
#include "CheckAndCompress.hpp"
// -------------------------------------------------------------------------------------
using namespace btrblocks;
// -------------------------------------------------------------------------------------

TEST(ArrowColumnwise, Begin) {
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


TEST(ArrowColumnwise, IntegerOneValue) {
  EnforceScheme<IntegerSchemeType> enforcer(IntegerSchemeType::ONE_VALUE);
  auto table = loadTableFromFiles({std::string(TEST_DATASET("integer/ONE_VALUE.integer"))});

  checkCompressTableColumnwise(table);
}

TEST(ArrowColumnwise, IntegerDynamicDict) {
  EnforceScheme<IntegerSchemeType> enforcer(IntegerSchemeType::DICT);
  auto table = loadTableFromFiles({std::string(TEST_DATASET("integer/DICTIONARY_16.integer"))});

  checkCompressTableColumnwise(table);
}

TEST(ArrowColumnwise, IntegerRLE)
{
  EnforceScheme<IntegerSchemeType> enforcer(IntegerSchemeType::RLE);
  auto table = loadTableFromFiles({std::string(TEST_DATASET("integer/RLE.integer"))});

  checkCompressTableColumnwise(table);
}

TEST(ArrowColumnwise, IntegerMixed) {
  auto table = loadTableFromFiles({std::string(TEST_DATASET("integer/ONE_VALUE.integer")),
                                   std::string(TEST_DATASET("integer/DICTIONARY_8.integer")),
                                   std::string(TEST_DATASET("integer/DICTIONARY_16.integer"))
                                   });

  checkCompressTableColumnwise(table);
}

TEST(ArrowColumnwise, DoubleOneValue) {
  auto table = loadTableFromFiles({std::string(TEST_DATASET("double/ONE_VALUE.double"))});
  checkCompressTableColumnwise(table);
}


TEST(ArrowColumnwise, DoubleRandom) {
  auto table = loadTableFromFiles({std::string(TEST_DATASET("double/RANDOM.double"))});
  checkCompressTableColumnwise(table);
}

TEST(ArrowColumnwise, DoubleRLE)
{
  EnforceScheme<DoubleSchemeType> enforcer(DoubleSchemeType::RLE);
  auto table = loadTableFromFiles({std::string(TEST_DATASET("double/RANDOM.double"))});

  checkCompressTableColumnwise(table);
}

TEST(ArrowColumnwise, DoubleDecimal) {
  EnforceScheme<DoubleSchemeType> enforcer(DoubleSchemeType::PSEUDODECIMAL);
  auto table = loadTableFromFiles({std::string(TEST_DATASET("double/DICTIONARY_8.double"))});

  checkCompressTableColumnwise(table);
}

TEST(ArrowColumnwise, DoubleDynamicDict) {
  EnforceScheme<DoubleSchemeType> enforcer(DoubleSchemeType::DICT);
  auto table = loadTableFromFiles({std::string(TEST_DATASET("double/DICTIONARY_8.double"))});

  checkCompressTableColumnwise(table);
}

TEST(ArrowColumnwise, DoubleFrequency) {
  EnforceScheme<DoubleSchemeType> enforcer(DoubleSchemeType::FREQUENCY);
  auto table = loadTableFromFiles({std::string(TEST_DATASET("double/FREQUENCY.double"))});

  checkCompressTableColumnwise(table);
}

TEST(ArrowColumnwise, StringOneValue) {
  EnforceScheme<StringSchemeType> enforcer(StringSchemeType::ONE_VALUE);
  auto table = loadTableFromFiles({std::string(TEST_DATASET("string/ONE_VALUE.string"))});
  checkCompressTableColumnwise(table);
}

TEST(ArrowColumnwise, StringCompressedDictionary)
{
  EnforceScheme<StringSchemeType> enforcer(StringSchemeType::DICT);
  auto table = loadTableFromFiles({std::string(TEST_DATASET("string/COMPRESSED_DICTIONARY.string"))});

  checkCompressTableColumnwise(table);
}