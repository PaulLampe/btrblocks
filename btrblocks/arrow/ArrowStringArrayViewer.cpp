#include <arrow/array/array_base.h>
#include <arrow/array/array_binary.h>
#include <stdexcept>
#include "common/Units.hpp"
#include "storage/StringArrayViewer.hpp"

namespace btrblocks {

    using namespace std;

class ArrowStringArrayViewer : public StringArrayViewer {

public:
  
  explicit ArrowStringArrayViewer(shared_ptr<arrow::StringArray>& array): StringArrayViewer(nullptr), array(array) {}
  
  inline static const str get(const u8* slots_ptr, u32 i) {
    throw runtime_error("get(2) operation is not allowed on arrow string array viewer");
  }

  [[nodiscard]] inline u32 size(u32 i) const override {
    return array->value_length(i);
  }

  inline const str operator()(u32 i) const override {
    return array->Value(i);
  }

  [[nodiscard]] inline const char* get_pointer(u32 i) const override {
    return array->Value(i).data();
  }

private:
  shared_ptr<arrow::StringArray> array;
};

}  // namespace btrblocks
