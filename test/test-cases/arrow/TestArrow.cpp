#include "./TestArrowDataLoader.hpp"
#include "arrow/ArrowTableCompressor.hpp"
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

TEST(Arrow, Begin) {
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

TEST(Arrow, IntegerOneValue) {
  EnforceScheme<IntegerSchemeType> enforcer(IntegerSchemeType::ONE_VALUE);
  auto table = loadTableFromFiles({std::string(TEST_DATASET("integer/ONE_VALUE.integer"))});

  checkCompressTable(table);
}

TEST(Arrow, IntegerDynamicDict) {
  EnforceScheme<IntegerSchemeType> enforcer(IntegerSchemeType::DICT);
  auto table = loadTableFromFiles({std::string(TEST_DATASET("integer/DICTIONARY_16.integer"))});

  checkCompressTable(table);
}

TEST(Arrow, IntegerRLE)
{
  EnforceScheme<IntegerSchemeType> enforcer(IntegerSchemeType::RLE);
  auto table = loadTableFromFiles({std::string(TEST_DATASET("integer/RLE.integer"))});

  checkCompressTable(table);
}

TEST(Arrow, IntegerMixed) {
  auto table = loadTableFromFiles({std::string(TEST_DATASET("integer/ONE_VALUE.integer")),
                                   std::string(TEST_DATASET("integer/DICTIONARY_8.integer")),
                                   std::string(TEST_DATASET("integer/DICTIONARY_16.integer"))
                                   });

  checkCompressTable(table);
}

TEST(Arrow, DoubleOneValue) {
  auto table = loadTableFromFiles({std::string(TEST_DATASET("double/ONE_VALUE.double"))});
  checkCompressTable(table);
}

TEST(Arrow, DoubleRandom) {
  auto table = loadTableFromFiles({std::string(TEST_DATASET("double/RANDOM.double"))});
  checkCompressTable(table);
}

TEST(Arrow, DoubleRLE)
{
  EnforceScheme<DoubleSchemeType> enforcer(DoubleSchemeType::RLE);
  auto table = loadTableFromFiles({std::string(TEST_DATASET("double/RANDOM.double"))});

  checkCompressTable(table);
}

TEST(Arrow, DoubleDecimal) {
  EnforceScheme<DoubleSchemeType> enforcer(DoubleSchemeType::PSEUDODECIMAL);
  auto table = loadTableFromFiles({std::string(TEST_DATASET("double/DICTIONARY_8.double"))});

  checkCompressTable(table);
}

TEST(Arrow, DoubleDynamicDict) {
  EnforceScheme<DoubleSchemeType> enforcer(DoubleSchemeType::DICT);
  auto table = loadTableFromFiles({std::string(TEST_DATASET("double/DICTIONARY_8.double"))});

  checkCompressTable(table);
}

TEST(Arrow, DoubleFrequency) {
  EnforceScheme<DoubleSchemeType> enforcer(DoubleSchemeType::FREQUENCY);
  auto table = loadTableFromFiles({std::string(TEST_DATASET("double/FREQUENCY.double"))});

  checkCompressTable(table);
}

TEST(Arrow, StringOneValue) {
  EnforceScheme<StringSchemeType> enforcer(StringSchemeType::ONE_VALUE);
  auto table = loadTableFromFiles({std::string(TEST_DATASET("string/ONE_VALUE.string"))});
  checkCompressTable(table);
}

TEST(Arrow, StringCompressedDictionary)
{
  EnforceScheme<StringSchemeType> enforcer(StringSchemeType::DICT);
  auto table = loadTableFromFiles({std::string(TEST_DATASET("string/COMPRESSED_DICTIONARY.string"))});

  checkCompressTable(table);
}