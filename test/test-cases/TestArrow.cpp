#include "arrow/ArrowTableCompressor.hpp"
#include "arrow/api.h"
#include "btrblocks.hpp"
#include "gtest/gtest.h"
#include "scheme/SchemePool.hpp"
#include "TestArrowDataLoader.hpp"
#include "test-cases/TestHelper.hpp"
// -------------------------------------------------------------------------------------
using namespace btrblocks;
// -------------------------------------------------------------------------------------

void printAverageCompressionRatio(
    std::shared_ptr<vector<tuple<OutputBlockStats, vector<u8>>>>& output) {
  double compressionRatioSum =
      std::accumulate(output->begin(), output->end(), 0.00,
                      [](auto& agg, auto& p) { return agg + std::get<0>(p).compression_ratio; });

  std::cout << "average compression ratio: "
            << static_cast<double>(compressionRatioSum) / static_cast<double>(output->size())
            << "\n";
}

void checkCompressTable(std::shared_ptr<arrow::Table>& table) {
  auto output = ArrowTableCompressor::compress(table);
  auto decompressedOutput = ArrowTableCompressor::decompress(output);

  printAverageCompressionRatio(output);

  // TODO: Save column name in meta
  for (int i = 0; i < table->schema()->num_fields(); ++i) {
    auto originalColumn = table->column(0);
    auto decompressedColumn = decompressedOutput->column(0);

    assert(originalColumn->Equals(decompressedColumn));
  }
}

TEST(Arrow, Begin) {
  BtrBlocksConfig::get().integers.schemes = defaultIntegerSchemes();
  BtrBlocksConfig::get().doubles.schemes = defaultDoubleSchemes();
  BtrBlocksConfig::get().strings.schemes = defaultStringSchemes();
  SchemePool::refresh();
}

TEST(Arrow, SingleIntegerColumnOneValue) {
  auto table = loadTableFromFiles({std::string(TEST_DATASET("integer/ONE_VALUE.integer"))});

  checkCompressTable(table);
}

TEST(Arrow, SingleIntegerColumnDictionary8) {
  auto table = loadTableFromFiles({std::string(TEST_DATASET("integer/DICTIONARY_8.integer"))});
  
  checkCompressTable(table);
}

TEST(Arrow, SingleIntegerColumnDictionary16) {
  auto table = loadTableFromFiles({std::string(TEST_DATASET("integer/DICTIONARY_16.integer"))});

  checkCompressTable(table);
}

TEST(Arrow, IntegerColumnsDictionaries) {
  auto table = loadTableFromFiles({std::string(TEST_DATASET("integer/DICTIONARY_8.integer")),
                                   std::string(TEST_DATASET("integer/DICTIONARY_16.integer"))});

  checkCompressTable(table);
}

TEST(Arrow, IntegerColumns) {

  auto table = loadTableFromFiles({std::string(TEST_DATASET("integer/ONE_VALUE.integer")),
                                   std::string(TEST_DATASET("integer/DICTIONARY_8.integer")),
                                   std::string(TEST_DATASET("integer/DICTIONARY_16.integer")),});

  checkCompressTable(table);
}
/*
TEST(Arrow, IntegerRLE)
{
  EnforceScheme<IntegerSchemeType> enforcer(IntegerSchemeType::RLE);
  auto table = loadTableFromFiles({std::string(TEST_DATASET("integer/RLE.integer"))});

  checkCompressTable(table);
}
// -------------------------------------------------------------------------------------
TEST(Arrow, DoubleRLE)
{
  EnforceScheme<DoubleSchemeType> enforcer(DoubleSchemeType::RLE);
  auto table = loadTableFromFiles({std::string(TEST_DATASET("double/RANDOM.double"))});

  checkCompressTable(table);
}
*/
// -------------------------------------------------------------------------------------
TEST(Arrow, IntegerDynamicDict)
{
  EnforceScheme<IntegerSchemeType> enforcer(IntegerSchemeType::DICT);
  auto table = loadTableFromFiles({std::string(TEST_DATASET("integer/DICTIONARY_16.integer"))});

  checkCompressTable(table);
}
// -------------------------------------------------------------------------------------
TEST(Arrow, DoubleDecimal)
{
  EnforceScheme<DoubleSchemeType> enforcer(DoubleSchemeType::PSEUDODECIMAL);
  auto table = loadTableFromFiles({std::string(TEST_DATASET("double/DICTIONARY_8.double"))});

  checkCompressTable(table);
}
// -------------------------------------------------------------------------------------
TEST(Arrow, DoubleDynamicDict)
{
  EnforceScheme<DoubleSchemeType> enforcer(DoubleSchemeType::DICT);
  auto table = loadTableFromFiles({std::string(TEST_DATASET("double/DICTIONARY_8.double"))});

  checkCompressTable(table);
}

TEST(Arrow, DoubleFrequency)
{
  EnforceScheme<DoubleSchemeType> enforcer(DoubleSchemeType::FREQUENCY);
  auto table = loadTableFromFiles({std::string(TEST_DATASET("double/FREQUENCY.double"))});

  checkCompressTable(table);
}