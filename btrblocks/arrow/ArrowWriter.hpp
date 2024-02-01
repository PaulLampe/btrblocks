#include <memory>
#include "arrow/ArrowMetaData.hpp"
#include "common/Units.hpp"
#include "compression/Datablock.hpp"

namespace btrblocks {

class ArrowWriter {
public:

static void writeBtrBlocks(const string& folder, tuple< ArrowMetaData, vector< vector< ColumnPart > > >& compressedData);

private:

static void writeMetadata(const string& folder, ArrowMetaData& meta);

static void writeColumnPart(const string& folder, SIZE& column_i, SIZE& part_i, ColumnPart& part);

};
} // namespace btrblocks