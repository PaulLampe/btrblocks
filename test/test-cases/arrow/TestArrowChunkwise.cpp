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

TEST(ArrowChunkwise, Begin) {
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

TEST(ArrowChunkwise, IntegerOneValue) {
  EnforceScheme<IntegerSchemeType> enforcer(IntegerSchemeType::ONE_VALUE);
  auto table = loadTableFromFiles({std::string(TEST_DATASET("integer/ONE_VALUE.integer"))});

  checkCompressTableChunkwise(table);
}

TEST(ArrowChunkwise, IntegerDynamicDict) {
  EnforceScheme<IntegerSchemeType> enforcer(IntegerSchemeType::DICT);
  auto table = loadTableFromFiles({std::string(TEST_DATASET("integer/DICTIONARY_16.integer"))});

  checkCompressTableChunkwise(table);
}

TEST(ArrowChunkwise, IntegerRLE)
{
  EnforceScheme<IntegerSchemeType> enforcer(IntegerSchemeType::RLE);
  auto table = loadTableFromFiles({std::string(TEST_DATASET("integer/RLE.integer"))});

  checkCompressTableChunkwise(table);
}

TEST(ArrowChunkwise, IntegerMixed) {
  auto table = loadTableFromFiles({std::string(TEST_DATASET("integer/ONE_VALUE.integer")),
                                   std::string(TEST_DATASET("integer/DICTIONARY_8.integer")),
                                   std::string(TEST_DATASET("integer/DICTIONARY_16.integer"))
                                   });

  checkCompressTableChunkwise(table);
}

TEST(ArrowChunkwise, DoubleOneValue) {
  auto table = loadTableFromFiles({std::string(TEST_DATASET("double/ONE_VALUE.double"))});
  
  checkCompressTableChunkwise(table);
}

TEST(ArrowChunkwise, DoubleRandom) {
  auto table = loadTableFromFiles({std::string(TEST_DATASET("double/RANDOM.double"))});
  
  checkCompressTableChunkwise(table);
}

TEST(ArrowChunkwise, DoubleRLE)
{
  EnforceScheme<DoubleSchemeType> enforcer(DoubleSchemeType::RLE);
  auto table = loadTableFromFiles({std::string(TEST_DATASET("double/RANDOM.double"))});

  checkCompressTableChunkwise(table);
}

TEST(ArrowChunkwise, DoubleDecimal) {
  EnforceScheme<DoubleSchemeType> enforcer(DoubleSchemeType::PSEUDODECIMAL);
  auto table = loadTableFromFiles({std::string(TEST_DATASET("double/DICTIONARY_8.double"))});

  checkCompressTableChunkwise(table);
}

TEST(ArrowChunkwise, DoubleDynamicDict) {
  EnforceScheme<DoubleSchemeType> enforcer(DoubleSchemeType::DICT);
  auto table = loadTableFromFiles({std::string(TEST_DATASET("double/DICTIONARY_8.double"))});

  checkCompressTableChunkwise(table);
}

TEST(ArrowChunkwise, DoubleFrequency) {
  EnforceScheme<DoubleSchemeType> enforcer(DoubleSchemeType::FREQUENCY);
  auto table = loadTableFromFiles({std::string(TEST_DATASET("double/FREQUENCY.double"))});

  checkCompressTableChunkwise(table);
}

TEST(ArrowChunkwise, StringOneValue) {
  EnforceScheme<StringSchemeType> enforcer(StringSchemeType::ONE_VALUE);
  auto table = loadTableFromFiles({std::string(TEST_DATASET("string/ONE_VALUE.string"))});
  
  checkCompressTableChunkwise(table);
}

TEST(ArrowChunkwise, StringCompressedDictionary)
{
  EnforceScheme<StringSchemeType> enforcer(StringSchemeType::DICT);
  auto table = loadTableFromFiles({std::string(TEST_DATASET("string/COMPRESSED_DICTIONARY.string"))});

  checkCompressTableChunkwise(table);
}