#include <arrow/table.h>
#include <arrow/type.h>
#include <arrow/type_fwd.h>
#include <algorithm>
#include <memory>
#include <tuple>
#include <vector>
#include "arrow/ArrowTableWrapper.hpp"
#include "arrow/columnwise/ArrowColumn.hpp"
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


tuple< unique_ptr < FileMetadata >, vector< vector< ColumnPart > > > ArrowColumnwiseTableCompressor::compress(shared_ptr<arrow::Table>& table) {
  // Start metadata file with info available before compression
  auto fileMeta = makeBytesArray(sizeof(FileMetadata) + table->num_columns() * sizeof(ColumnPartInfo));
  auto meta = unique_ptr<FileMetadata>(reinterpret_cast<FileMetadata*>(fileMeta.release()));
  meta->num_columns = table->num_columns();

  // Get single columns from table
  ArrowTableWrapper wrapper(table);
  auto columns = wrapper.generateColumns();
  vector< vector< ColumnPart > > compressedColumns{};

  for (SIZE column_i = 0; column_i < columns.size(); column_i++) {
    auto& column = columns[column_i];

    auto parts = ArrowColumnCompressor::compressColumn(column);

    // Update file meta 
    meta->num_chunks += parts.size();

    auto& partInfo = meta->parts[column_i];
    partInfo.num_parts = parts.size();
    partInfo.type = column.type;

    compressedColumns.push_back(parts);
  }


  return make_tuple(std::move(meta), compressedColumns);
};

shared_ptr<arrow::Table> ArrowColumnwiseTableCompressor::decompress(FileMetadata* meta, vector< vector< vector<u8> > >& compressedTable, vector<SIZE>& columnIndices){  
  SIZE numColumns = columnIndices.size();
  
  vector<std::shared_ptr<arrow::Field>> fields(numColumns);
  arrow::ChunkedArrayVector columns(numColumns);
  
  tbb::parallel_for(SIZE(0), numColumns, [&](const auto& column_i) {
      auto column = ArrowColumnCompressor::decompressColumn(compressedTable[columnIndices[column_i]], meta->parts[columnIndices[column_i]]);

      fields[column_i] = column.field;
      columns[column_i] = column.data;
  });

  return arrow::Table::Make(arrow::schema(fields), columns);
};

} // namespace btrblocks