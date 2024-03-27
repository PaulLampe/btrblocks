#include <arrow/api.h>
#include "arrow/ArrowMetaData.hpp"
#include "common/Units.hpp"

namespace btrblocks {

using namespace btrblocks;
using namespace std;

class MetaDataUtils {
 public:
  static vector<SIZE> resolveColumnIndices(ArrowMetaData& metadata, const vector<string>& columns);

  static shared_ptr<arrow::Schema> resolveSchema(ArrowMetaData& meta, vector<SIZE>& columnIndices);
};

}  // namespace btrblocks