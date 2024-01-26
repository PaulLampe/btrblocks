// ---------------------------------------------------------------------------
// BtrBlocks
// ---------------------------------------------------------------------------
#include "benchmark/benchmark.h"
#include "common/Units.hpp"
#include "scheme/SchemePool.hpp"
#include "bench-cases/regression_benchmark.cpp"
#include "decompression/decompression_benchmark_local.cpp"
#include "storage/Column.hpp"
// ---------------------------------------------------------------------------
using namespace btrblocks;
// ---------------------------------------------------------------------------
int main(int argc, char** argv) {
#ifdef BTR_USE_SIMD
  std::cout << "\033[0;35mSIMD ENABLED\033[0m" << std::endl;
#else
  std::cout << "\033[0;31mSIMD DISABLED\033[0m" << std::endl;
#endif
  btrbench::RegisterSingleBenchmarks();
  benchmark::RegisterBenchmark("LOCAL_DECOMPRESSION",btrbench::BtrBlocksDecompressionBenchmark, vector<ColumnType>{ColumnType::DOUBLE});
  benchmark::Initialize(&argc, argv);
  benchmark::RunSpecifiedBenchmarks();
}
// ---------------------------------------------------------------------------
