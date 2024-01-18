#include <arrow/array/array_binary.h>
#include <arrow/array/builder_binary.h>
#include <arrow/type_fwd.h>
#include <cstdint>
#include <initializer_list>
#include <memory>
#include <regex>
#include <string>
#include <string_view>
#include <vector>
#include "arrow/api.h"
#include "btrblocks.hpp"
#include "common/Units.hpp"
#include "storage/MMapVector.hpp"

using namespace btrblocks;

std::shared_ptr<arrow::Array> loadIntegerFile(std::string& filePath) {
  Vector<INTEGER> data(filePath.c_str());
  std::vector<INTEGER> vec(data.begin(), data.end());

  arrow::Int32Builder builder;

  builder.AppendValues(vec).ok();

  auto array = builder.Finish().ValueOrDie();

  return array;
}

std::shared_ptr<arrow::Array> loadDoubleFile(std::string& filePath) {
  Vector<DOUBLE> data(filePath.c_str());
  std::vector<DOUBLE> vec(data.begin(), data.end());

  arrow::DoubleBuilder builder;
  builder.AppendValues(vec).ok();
  auto array = builder.Finish().ValueOrDie();
  return array;
}

std::shared_ptr<arrow::Array> loadStringFile(std::string& filePath) {
  Vector<str> data(filePath.c_str());

  arrow::StringBuilder builder;

  builder.Resize(static_cast<int64_t>(data.size())).ok();
  
  for (auto it : data) {
    builder.Append(it).ok();
  }

  auto array = builder.Finish().ValueOrDie();
  return array;
}

std::pair<std::shared_ptr<arrow::DataType>, std::shared_ptr<arrow::Array>> loadColumnFromFile(
    std::string filePath) {
  std::string column_name, column_type_str;
  std::regex re(R"(.*\/(.*)\.(\w*))");
  std::smatch match;

  if (std::regex_search(filePath, match, re) && match.size() > 1) {
    column_name = match.str(1);
    column_type_str = match.str(2);
    ColumnType column_type = ConvertStringToType(column_type_str);

    switch (column_type) {
      case ColumnType::INTEGER:
        return {arrow::int32(), loadIntegerFile(filePath)};
      case ColumnType::DOUBLE:
        return {arrow::float64(), loadDoubleFile(filePath)};
      case ColumnType::STRING:
        return {arrow::utf8(),loadStringFile(filePath)};
      default:
        throw std::runtime_error("Wrong column type specified in file path: " + filePath);
        break;
    }
  } else {
    UNREACHABLE();
  }
}

std::shared_ptr<arrow::Table> loadTableFromFiles(std::initializer_list<std::string> paths) {
  vector<std::shared_ptr<arrow::Field>> fields{};
  vector<std::shared_ptr<arrow::Array>> arrays{};

  SIZE colIndex = 0;

  for (auto& path : paths) {
    auto [type, data] = loadColumnFromFile(path);
    fields.push_back(arrow::field(type->ToString() + std::to_string(colIndex), type));
    arrays.push_back(data);
    colIndex++;
  }

  return arrow::Table::Make(arrow::schema(fields), arrays);
}