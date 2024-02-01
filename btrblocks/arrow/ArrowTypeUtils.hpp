#include <arrow/type.h>
#include <arrow/type_fwd.h>
#include <memory>
#include <stdexcept>
#include "common/Units.hpp"
namespace btrblocks {

using namespace std;

struct ArrowTypeUtils {
  static shared_ptr<arrow::DataType> arrowTypeFromColumnType(ColumnType type) {
    switch (type) {
      case ColumnType::INTEGER:
        return arrow::int32();
      case ColumnType::DOUBLE:
        return arrow::float64();
      case ColumnType::STRING:
        return arrow::utf8();
      default:
        throw runtime_error("Unsupported ColumnType for arrow:" + ConvertTypeToString(type));
    }
  }
};

}  // namespace btrblocks