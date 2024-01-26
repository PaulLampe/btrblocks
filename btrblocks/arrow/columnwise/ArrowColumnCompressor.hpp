#pragma once
#include <arrow/array/array_base.h>
#include <algorithm>
#include <memory>
#include <vector>
#include "common/Units.hpp"
#include "compression/Datablock.hpp"
#include "storage/Chunk.hpp"
#include "ArrowColumn.hpp"

namespace btrblocks {

using namespace std;

class ArrowColumnCompressor {
public: 
    static vector<ColumnPart> compressColumn(ArrowColumn& column);

    static ArrowColumn decompressColumn(vector<vector<u8>>& parts, ColumnPartInfo& columnInfo);
};

} // namespace btrblocks