#include "StringStats.hpp"
#include <arrow/array/array_binary.h>
#include <memory>
// -------------------------------------------------------------------------------------
namespace btrblocks {
// -------------------------------------------------------------------------------------
StringStats StringStats::generateStats(const btrblocks::StringArrayViewer& src,
                                       const BITMAP* nullmap,
                                       u32 tuple_count,
                                       SIZE column_data_size) {
  // -------------------------------------------------------------------------------------
  // Collect stats
  StringStats stats;
  // -------------------------------------------------------------------------------------
  stats.tuple_count = tuple_count;
  stats.total_size = column_data_size;
  stats.total_length = 0;
  stats.total_unique_length = 0;
  stats.null_count = 0;
  // -------------------------------------------------------------------------------------
  for (u64 row_i = 0; row_i < tuple_count; row_i++) {
    if (nullmap == nullptr || nullmap[row_i]) {
      auto current_value = src(row_i);
      if (stats.distinct_values.find(current_value) == stats.distinct_values.end()) {
        stats.distinct_values.insert(current_value);
        stats.total_unique_length += current_value.length();
      }
      stats.total_length += current_value.size();
    } else {
      stats.distinct_values.insert("");
      stats.null_count++;
      continue;
    }
  }
  // -------------------------------------------------------------------------------------
  stats.unique_count = stats.distinct_values.size();
  stats.set_count = stats.tuple_count - stats.null_count;
  return stats;
}

StringStats StringStats::generateArrowStats(const std::shared_ptr<arrow::StringArray>& array,
                                       SIZE column_data_size) {
  // -------------------------------------------------------------------------------------
  // Collect stats
  StringStats stats;
  // -------------------------------------------------------------------------------------
  stats.tuple_count = array->length();
  stats.total_size = column_data_size;
  stats.total_length = 0;
  stats.total_unique_length = 0;
  stats.null_count = 0;
  // -------------------------------------------------------------------------------------
  for (int64_t row_i = 0; row_i < array->length(); row_i++) {
    if (!array->IsNull(row_i)) {
      auto current_value = array->GetView(row_i);
      if (stats.distinct_values.find(current_value) == stats.distinct_values.end()) {
        stats.distinct_values.insert(current_value);
        stats.total_unique_length += current_value.length();
      }
      stats.total_length += current_value.size();
    } else {
      stats.distinct_values.insert("");
      stats.null_count++;
      continue;
    }
  }
  // -------------------------------------------------------------------------------------
  stats.unique_count = stats.distinct_values.size();
  stats.set_count = stats.tuple_count - stats.null_count;
  return stats;
}
// -------------------------------------------------------------------------------------
}  // namespace btrblocks