#pragma once
#include <algorithm>
#include <memory>
#include <vector>
#include "compression/Datablock.hpp"
#include "storage/Chunk.hpp"
#include "storage/MMapVector.hpp"
#include "arrow/api.h"
#include "../ArrowMetaData.hpp"

namespace btrblocks {

using namespace std;

// Orchestrates all compression related tasks
struct ArrowColumnwiseTableCompressor {
 public:
  ArrowColumnwiseTableCompressor() = default;

  static tuple< ArrowMetaData, vector< vector< ColumnPart > > > compress(shared_ptr<arrow::Table>& table);

  static shared_ptr<arrow::Table> decompress(ArrowMetaData& meta, vector< vector< vector<u8> > >& compressedTable, vector<SIZE>& columnIndices);
};

}  // namespace btrblocks