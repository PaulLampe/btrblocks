# ---------------------------------------------------------------------------
# BtrBlocks Benchmarks
# ---------------------------------------------------------------------------

set(BTR_BENCH_DIR ${CMAKE_CURRENT_LIST_DIR})


# ---------------------------------------------------------------------------
# Benchmark
# ---------------------------------------------------------------------------

add_executable(benchmarks ${BTR_BENCH_DIR}/benchmarks.cpp)
target_link_libraries(benchmarks btrblocks gbenchmark gtest gmock Threads::Threads Parquet tbb)
target_include_directories(benchmarks PRIVATE ${BTR_INCLUDE_DIR})
target_include_directories(benchmarks PRIVATE ${BTR_BENCH_DIR})

enable_testing()

# ---------------------------------------------------------------------------
# Bench Dataset Downloader
# ---------------------------------------------------------------------------

add_executable(bench_dataset_downloader ${BTR_BENCH_DIR}/AwsDatasetDownloader.cpp)
target_link_libraries(bench_dataset_downloader btrblocks gflags Threads::Threads libaws-cpp-sdk-core libaws-cpp-sdk-s3 libaws-cpp-sdk-transfer)
target_include_directories(bench_dataset_downloader PRIVATE ${BTR_INCLUDE_DIR})
target_compile_options(bench_dataset_downloader PUBLIC -Wno-deprecated-declarations)


add_executable(decomp_bench_dataset_downloader ${BTR_BENCH_DIR}/AwsDecompressionDatasetDownloader.cpp)
target_link_libraries(decomp_bench_dataset_downloader btrblocks gflags Threads::Threads libaws-cpp-sdk-core libaws-cpp-sdk-s3 libaws-cpp-sdk-transfer)
target_include_directories(decomp_bench_dataset_downloader PRIVATE ${BTR_INCLUDE_DIR})
target_compile_options(decomp_bench_dataset_downloader PUBLIC -Wno-deprecated-declarations)

# ---------------------------------------------------------------------------
# Linting
# ---------------------------------------------------------------------------

add_clang_tidy_target(lint_bench "${BENCH_CC}")
add_dependencies(lint_bench gtest)
add_dependencies(lint_bench gbenchmark)
list(APPEND lint_targets lint_bench)
