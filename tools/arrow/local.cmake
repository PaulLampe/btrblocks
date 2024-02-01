set(BTR_ARROW_DIR ${CMAKE_CURRENT_LIST_DIR})

add_executable(parquettobtr ${BTR_ARROW_DIR}/parquetToBtr.cpp)
target_include_directories(parquettobtr PRIVATE ${BTR_INCLUDE_DIR})
target_link_libraries(parquettobtr btrblocks gflags tbb Arrow Parquet)