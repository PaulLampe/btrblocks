#pragma once
#include <arrow/array/array_base.h>
#include <arrow/array/concatenate.h>
#include <arrow/chunked_array.h>
#include <arrow/type.h>
#include <arrow/type_fwd.h>
#include <math.h>
#include <cstdint>
#include <memory>
#include <vector>
#include "common/Units.hpp"
#include "storage/Chunk.hpp"

namespace btrblocks {

using namespace std;

// When compressing, this represents one column of the full table that is to be compressed.
// When decompressing this represents one Column of the table that is returned in the end.

struct ArrowColumn {
 public:
  explicit ArrowColumn(shared_ptr<arrow::Field>&& field, shared_ptr<arrow::ChunkedArray>&& data);

  explicit ArrowColumn(ColumnType type, arrow::ArrayVector& chunks);

  shared_ptr<arrow::Field> field;
  shared_ptr<arrow::ChunkedArray> data;
  arrow::ArrayVector chunks;
  ColumnType type;

  static shared_ptr<arrow::Field> arrowFieldFromColumnType(const ColumnType& type);

 private:
  static vector<shared_ptr<arrow::Array>> getChunks(shared_ptr<arrow::ChunkedArray>& data);
  static shared_ptr<arrow::Array> generateChunk(std::shared_ptr<arrow::ChunkedArray>& data, int64_t offset, int64_t& blockSize);
  static shared_ptr<arrow::ChunkedArray> assembleChunks(const arrow::ArrayVector& chunks, const std::shared_ptr<arrow::DataType>& type);

  static ColumnType columnTypeFromArrowField(shared_ptr<arrow::Field>& field);
};

}  // namespace btrblocks