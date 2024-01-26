#include "ArrowTableWrapper.hpp"
#include <arrow/array/array_base.h>
#include <arrow/array/builder_base.h>
#include <arrow/array/builder_primitive.h>
#include <arrow/array/concatenate.h>
#include <arrow/type_fwd.h>
#include <cstdint>
#include <memory>
#include <vector>
#include "columnwise/ArrowColumn.hpp"
#include "common/Units.hpp"

using namespace btrblocks;

std::vector<ArrowTableChunk> ArrowTableWrapper::generateChunks() {
  std::vector<ArrowTableChunk> chunks{};

  auto& config = BtrBlocksConfig::get();
  auto blockSize = static_cast<int64_t>(config.block_size);

  int64_t offset = 0;

  while (offset + blockSize < table->num_rows()) {
    chunks.push_back(generateChunk(offset, blockSize));
    offset += blockSize;
  }

  chunks.push_back(generateChunk(offset, table->num_rows() - offset));
  return chunks;
}

ArrowTableChunk ArrowTableWrapper::generateChunk(int64_t offset, int64_t blockSize) {
  arrow::ArrayVector columns{};

  for (auto& column : table->columns()) {
    auto slice = column->Slice(offset, blockSize);
    if (slice->num_chunks() == 1) {
      columns.push_back(slice->chunk(0));
    } else {
      // If we have to read past chunk boundaries, we have to unify the single chunks for compression
      auto array = arrow::Concatenate(slice->chunks()).ValueOrDie();
      columns.push_back(array);
    }
  }

  return ArrowTableChunk(table->schema(), std::move(columns));
}

vector<ArrowColumn> ArrowTableWrapper::generateColumns() {
  vector<ArrowColumn> columns{};

  for (int i = 0; i < table->num_columns(); ++i) {
    ArrowColumn column{
      table->field(i),
      table->column(i)
    };

    columns.push_back(column);
  }

  return columns;
}