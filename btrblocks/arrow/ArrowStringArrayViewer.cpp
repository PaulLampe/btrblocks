#include <arrow/array/array_base.h>
#include <arrow/array/array_binary.h>
#include <arrow/buffer.h>
#include <cstring>
#include <memory>
#include <stdexcept>
#include "common/Units.hpp"
#include "storage/StringArrayViewer.hpp"

namespace btrblocks {

    using namespace std;

class ArrowStringArrayViewer : public StringArrayViewer {

public:
  // Giving raw_value_offsets for fsst compression (accounts for slice offset)
  explicit ArrowStringArrayViewer(shared_ptr<arrow::StringArray>& array): StringArrayViewer(reinterpret_cast<const u8*>(array->raw_value_offsets())), array(array) {}
  
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

  void transformToRegularStringArrayViewer() {
    auto offsetLength = sizeof(u32) * (array->length() + 1);

    auto buffer = arrow::AllocateBuffer(static_cast<int64_t>(array->total_values_length() + offsetLength)).ValueOrDie();

    memcpy(buffer->mutable_data_as<u8>(), reinterpret_cast<const u8*>(array->raw_value_offsets()), offsetLength);

    memcpy(buffer->mutable_data_as<u8>() + offsetLength, array->raw_data(), array->total_values_length());

    slots_ptr = buffer->data();
    _buffer = std::move(buffer); // Just ensure the buffer memory survives the end of this scope
  }

private:
  shared_ptr<arrow::StringArray> array;
  unique_ptr<arrow::Buffer> _buffer;
};

}  // namespace btrblocks
