# ---------------------------------------------------------------------------
# btrblocks
# ---------------------------------------------------------------------------

include(ExternalProject)
find_package(Git REQUIRED)

# Get arrow
ExternalProject_Add(
        arrow_src
        PREFIX "vendor/arrow"
        GIT_REPOSITORY "https://github.com/apache/arrow"
        SOURCE_SUBDIR cpp
        GIT_TAG ba537483618196f50c67a90a473039e4d5dc35e0
        TIMEOUT 10
        CMAKE_ARGS
        -DCMAKE_INSTALL_PREFIX=${CMAKE_BINARY_DIR}/vendor/arrow
        -DCMAKE_C_COMPILER=${CMAKE_C_COMPILER}
        -DCMAKE_CXX_COMPILER=${CMAKE_CXX_COMPILER}
        -DCMAKE_CXX_FLAGS=${CMAKE_CXX_FLAGS}
        -DARROW_PARQUET=ON
        -DARROW_BUILD_STATIC=ON
        UPDATE_COMMAND ""
)

# Prepare arrow
ExternalProject_Get_Property(arrow_src install_dir)
set(ARROW_INCLUDE_DIR ${install_dir}/include)
set(ARROW_LIBRARY_PATH ${install_dir}/lib/libarrow_bundled_dependencies.a)
file(MAKE_DIRECTORY ${ARROW_INCLUDE_DIR})

add_library(Arrow STATIC IMPORTED)
set_property(TARGET Arrow PROPERTY IMPORTED_LOCATION ${ARROW_LIBRARY_PATH})
set_property(TARGET Arrow APPEND PROPERTY INTERFACE_INCLUDE_DIRECTORIES ${ARROW_INCLUDE_DIR})

set(ARROW_PARQUET_LIBRARY_PATH ${install_dir}/lib/libparquet.a)
add_library(Parquet STATIC IMPORTED)
set_property(TARGET Parquet PROPERTY IMPORTED_LOCATION ${ARROW_PARQUET_LIBRARY_PATH})
set_property(TARGET Parquet APPEND PROPERTY INTERFACE_INCLUDE_DIRECTORIES ${ARROW_INCLUDE_DIR})

set(THREADS_PREFER_PTHREAD_FLAG ON)
find_package(Threads REQUIRED)
target_link_libraries(Arrow INTERFACE Threads::Threads)
target_link_libraries(Parquet INTERFACE Threads::Threads)

# Dependencies
add_dependencies(Arrow arrow_src)
