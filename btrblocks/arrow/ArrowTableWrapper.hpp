#pragma once
#include <arrow/api.h>
#include <arrow/table.h>
#include "ArrowTableChunk.hpp"
#include "btrblocks.hpp"

namespace btrblocks {

// Wraps a table and enables creating chunks of the contained data, which are zero copy slices of
// the original data
struct ArrowTableWrapper {
 public:
  explicit ArrowTableWrapper(std::shared_ptr<arrow::Table> table) : table(std::move(table)) {}

  // Generates chunks for compression
  std::vector<ArrowTableChunk> generateChunks();

 private:
  // Generates one chunk of the given table at the given offset by (zero-copy) slicing the relevant
  // parts from each column
  ArrowTableChunk generateChunk(int64_t offset, int64_t blockSize);
  const std::shared_ptr<arrow::Table> table;
};

}  // namespace btrblocks