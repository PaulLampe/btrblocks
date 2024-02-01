#include <arrow/record_batch.h>
#include <arrow/table.h>
#include <arrow/type.h>
#include <arrow/type_fwd.h>
#include <algorithm>
#include <cstring>
#include <functional>
#include <memory>
#include <string>
#include <tuple>
#include <vector>
#include "ArrowColumn.hpp"
#include "../ArrowTableWrapper.hpp"
#include "arrow/ArrowMetaData.hpp"
#include "btrblocks.hpp"
#include "common/Exceptions.hpp"
#include "common/Units.hpp"
#include "compression/Datablock.hpp"
#include "storage/Chunk.hpp"
#include "storage/MMapVector.hpp"
#include "arrow/api.h"
#include "ArrowColumnwiseTableCompressor.hpp"
#include "ArrowColumnCompressor.hpp"
#include "tbb/parallel_for.h"
#include <chrono>

namespace btrblocks {

using namespace std;

tuple< ArrowMetaData, vector< vector< ColumnPart > > > ArrowColumnwiseTableCompressor::compress(shared_ptr<arrow::Table>& table) {
  // Start metadata file with info available before compression
  ArrowMetaData meta{};

  meta.numColumns = table->num_columns();

  // Get single columns from table
  ArrowTableWrapper wrapper(table);
  auto columns = wrapper.generateColumns();
  vector< vector< ColumnPart > > compressedColumns(table->num_columns());

  // Update file meta 
  meta.numChunks = ceil(table->num_rows() * 1.00 / BtrBlocksConfig::get().block_size);
  meta.columnMeta.resize(meta.numColumns);

  tbb::parallel_for(static_cast<SIZE>(0),columns.size(), [&](const auto& column_i) {
    auto& column = columns[column_i];

    die_if(meta.numChunks == column.chunks.size());

    auto parts = ArrowColumnCompressor::compressColumn(column);

    auto& columnMeta = meta.columnMeta[column_i];

    columnMeta.numParts = parts.size();
    columnMeta.type = column.type;
    columnMeta.chunksPerPart.resize(columnMeta.numParts);
    columnMeta.name = table->field(column_i)->name();

    for (SIZE part_i = 0; part_i < parts.size(); part_i++) {
      columnMeta.chunksPerPart[part_i] = parts[part_i].chunks.size();
    }

    compressedColumns[column_i] = parts;
  });

  return make_tuple(std::move(meta), compressedColumns);
};

shared_ptr<arrow::Table> ArrowColumnwiseTableCompressor::decompress(ArrowMetaData& meta, vector< vector< vector<u8> > >& compressedTable, vector<SIZE>& columnIndices){  
  SIZE numColumns = columnIndices.size();
  
  vector<std::shared_ptr<arrow::Field>> fields(numColumns);
  arrow::ChunkedArrayVector columns(numColumns);
  
  tbb::parallel_for(static_cast<SIZE>(0), numColumns, [&](const auto& column_i) {
      auto column = ArrowColumnCompressor::decompressColumn(compressedTable[columnIndices[column_i]], meta.columnMeta[columnIndices[column_i]]);

      fields[column_i] = column.field;
      columns[column_i] = column.data;
  });

  return arrow::Table::Make(arrow::schema(fields), columns);
};

} // namespace btrblocks