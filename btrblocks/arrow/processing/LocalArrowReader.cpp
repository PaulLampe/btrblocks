#include "LocalArrowReader.hpp"
#include <arrow/record_batch.h>
#include <arrow/type.h>
#include <arrow/type_fwd.h>
#include <tbb/blocked_range.h>
#include <tbb/parallel_for.h>
#include <algorithm>
#include <memory>
#include <numeric>
#include <string>
#include <vector>
#include "../ArrowTypeUtils.hpp"
#include "../columnwise/ArrowColumn.hpp"
#include "../columnwise/ArrowColumnChunkDecompressor.hpp"
#include "arrow/ArrowMetaData.hpp"
#include "common/Units.hpp"
#include "common/Utils.hpp"
#include "compression/Datablock.hpp"
#include "storage/Chunk.hpp"

namespace btrblocks {

vector<SIZE> LocalArrowReader::resolveColumnIndices(const vector<string>& columns) {
  vector<SIZE> columnIndices{};

  for (auto& column : columns) {
    auto columnIndex = find_if(metadata.columnMeta.begin(), metadata.columnMeta.end(),
                               [&](const auto& columnMeta) { return columnMeta.name == column; });

    columnIndices.push_back(columnIndex - metadata.columnMeta.begin());
  }

  return columnIndices;
}

vector<vector<vector<u8>>> LocalArrowReader::mmapColumns(vector<SIZE>& columnIndices) {
  vector<vector<vector<u8>>> output{columnIndices.size()};

  SIZE column_i = 0;

  for (auto& columnIndex : columnIndices) {
    auto columnMeta = metadata.columnMeta[columnIndex];

    output[column_i].resize(columnMeta.numParts);

    auto filePrefix = folder + "/column" + to_string(columnIndex) + "_part";

    for (SIZE part_i = 0; part_i < columnMeta.numParts; part_i++) {
      auto filePath = filePrefix + to_string(part_i) + ".btr";

      output[column_i][part_i] = Utils::mmapFile(filePath);
    }

    column_i++;
  }

  return output;
}

vector<vector<tuple<SIZE, SIZE>>> LocalArrowReader::resolveOffsets(vector<SIZE>& columnIndices) {
  vector<vector<tuple<SIZE, SIZE>>> offsets(
      metadata.numChunks, vector<tuple<SIZE, SIZE>>(columnIndices.size()));  // row_group_i -> column_i -> part_i + chunk_i

  SIZE column_i = 0;

  for (auto& columnIndex : columnIndices) {
    SIZE chunk_offset = 0;

    auto columnMeta = metadata.columnMeta[columnIndex];

    for (SIZE part_i = 0; part_i < columnMeta.numParts; part_i++) {
      SIZE local_chunk_i = 0;

      while (local_chunk_i < columnMeta.chunksPerPart[part_i]) {
        offsets[chunk_offset + local_chunk_i][column_i] = {part_i, local_chunk_i};
        local_chunk_i++;
      }
      chunk_offset += local_chunk_i;
    }
    column_i++;
  }

  return offsets;
}

shared_ptr<arrow::Schema> LocalArrowReader::resolveSchema(vector<SIZE>& columnIndices) {
  arrow::FieldVector fields{};

  for (auto& columnIndex : columnIndices) {
    auto columnMeta = metadata.columnMeta[columnIndex];

    fields.push_back(
        arrow::field(columnMeta.name, ArrowTypeUtils::arrowTypeFromColumnType(columnMeta.type)));
  }

  return arrow::schema(fields);
}

shared_ptr<arrow::Schema> LocalArrowReader::getSchema() {
  vector<SIZE> columnIndices(metadata.numColumns);

  iota(columnIndices.begin(), columnIndices.end(), 0);

  return resolveSchema(columnIndices);
}

ArrowMetaData LocalArrowReader::getMetaData() {
  return metadata;
}

void LocalArrowReader::scan(const vector<string>& columns,
                            const function<void(shared_ptr<arrow::RecordBatch>)>& callback) {
  auto numColumnsToDecompress = columns.size();

  auto columnIndices = resolveColumnIndices(columns);

  auto columnParts = mmapColumns(columnIndices);

  auto chunkOffsets = resolveOffsets(columnIndices);

  auto schema = resolveSchema(columnIndices);

  tbb::blocked_range<u32> range(0, metadata.numChunks);

  tbb::parallel_for(range, [&](const auto& r) {
    for (auto chunk_i = r.begin(); chunk_i < r.end(); chunk_i++) {
      arrow::ArrayVector columns(numColumnsToDecompress);

      auto offsets = chunkOffsets[chunk_i];

      for (SIZE column_i = 0; column_i < numColumnsToDecompress; ++column_i) {
        auto [part_i, chunk_i] = offsets[column_i];

        auto* dataPtr = columnParts[column_i][part_i].data();

        auto columnPart = reinterpret_cast<ColumnPartMetadata*>(dataPtr);

        columns[column_i] = ArrowColumnChunkDecompressor::decompress(
            reinterpret_cast<ColumnChunkMeta*>(dataPtr + columnPart->offsets[chunk_i]));
      }

      auto batch = arrow::RecordBatch::Make(schema, columns[0]->length(), columns);

      callback(batch);
    }
  });
}

}  // namespace btrblocks