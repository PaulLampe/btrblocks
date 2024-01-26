#include "ArrowColumn.hpp"
#include <arrow/array/array_base.h>
#include <arrow/array/concatenate.h>
#include <arrow/chunked_array.h>
#include <arrow/status.h>
#include <arrow/table.h>
#include <arrow/type.h>
#include <arrow/type_fwd.h>
#include <memory>
#include <stdexcept>
#include <string>
#include "btrblocks.hpp"
#include "common/Units.hpp"
#include "storage/Chunk.hpp"

using namespace std;

namespace btrblocks {

ArrowColumn::ArrowColumn(shared_ptr<arrow::Field>&& _field, shared_ptr<arrow::ChunkedArray>&& _data)
    : field(std::move(_field)), data(std::move(_data)) {
  chunks = getChunks(data), type = columnTypeFromArrowField(field);
}

ArrowColumn::ArrowColumn(ColumnType type, arrow::ArrayVector& chunks) : chunks(chunks), type(type) {
  field = arrowFieldFromColumnType(type);
  data = assembleChunks(chunks);
}

ColumnType ArrowColumn::columnTypeFromArrowField(shared_ptr<arrow::Field>& field) {
  auto type = field->type();

  if (type->Equals(arrow::int32())) {
    return ColumnType::INTEGER;
  }

  if (type->Equals(arrow::float64())) {
    return ColumnType::DOUBLE;
  }

  if (type->Equals(arrow::utf8())) {
    return ColumnType::STRING;
  }

  throw std::runtime_error("Undhandled column. Unsupported type");
}

// TODO: Get column name from somewhere
shared_ptr<arrow::Field> ArrowColumn::arrowFieldFromColumnType(ColumnType& type) {
  switch (type) {
    case ColumnType::INTEGER:
      return arrow::field("int_col", arrow::int32());
    case ColumnType::DOUBLE:
      return arrow::field("double_col", arrow::float64());
    case ColumnType::STRING:
      return arrow::field("string_col", arrow::utf8());
    default:
      throw std::runtime_error("Undhandled column. Unsupported type");
  }
}

vector<shared_ptr<arrow::Array>> ArrowColumn::getChunks(shared_ptr<arrow::ChunkedArray>& data) {
  auto& config = BtrBlocksConfig::get();
  auto blockSize = static_cast<int64_t>(config.block_size);

  auto tupleCount = data->length();

  int64_t numberOfChunks = tupleCount / blockSize + ((tupleCount % blockSize) > 0);

  vector<shared_ptr<arrow::Array>> chunks(numberOfChunks);

  for (int64_t chunk_i = 0; chunk_i < numberOfChunks; ++chunk_i) {
    chunks[chunk_i] = generateChunk(data, chunk_i * blockSize, blockSize);
  }

  return chunks;
}

shared_ptr<arrow::ChunkedArray> ArrowColumn::assembleChunks(arrow::ArrayVector& chunks) {
  return arrow::ChunkedArray::Make(chunks).ValueOrDie();
}

shared_ptr<arrow::Array> ArrowColumn::generateChunk(std::shared_ptr<arrow::ChunkedArray>& data,
                                                    int64_t offset,
                                                    int64_t& blockSize) {
  auto slice = data->Slice(offset, blockSize);
  if (slice->num_chunks() == 1) {
    return slice->chunk(0);
  } else {
    // If we have to read past chunk boundaries, we have to unify the single chunks for compression
    return arrow::Concatenate(slice->chunks()).ValueOrDie();
  }
}

}  // namespace btrblocks