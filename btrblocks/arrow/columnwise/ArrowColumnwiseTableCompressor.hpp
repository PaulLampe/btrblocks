#pragma once
#include <algorithm>
#include <memory>
#include <vector>
#include "compression/Datablock.hpp"
#include "storage/Chunk.hpp"
#include "storage/MMapVector.hpp"
#include "arrow/api.h"

namespace btrblocks {

using namespace std;

// Orchestrates all compression related tasks
struct ArrowColumnwiseTableCompressor {
 public:
  ArrowColumnwiseTableCompressor() = default;

  static tuple< unique_ptr<FileMetadata>, vector< vector< ColumnPart > > > compress(shared_ptr<arrow::Table>& table);

  static shared_ptr<arrow::Table> decompress(FileMetadata* meta, vector< vector< vector<u8> > >& compressedTable, vector<SIZE>& columnIndices);
};

}  // namespace btrblocks