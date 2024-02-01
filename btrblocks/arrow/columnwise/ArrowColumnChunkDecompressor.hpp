#include <arrow/api.h>
#include <vector>
#include "common/Units.hpp"
#include "compression/Datablock.hpp"

namespace btrblocks {

using namespace std;

class ArrowColumnChunkDecompressor {
public:
  static std::shared_ptr<arrow::Array> decompress(ColumnChunkMeta* columnChunk);

};

} // namespace btrblocks