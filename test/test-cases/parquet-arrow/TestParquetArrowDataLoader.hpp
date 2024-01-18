#include <arrow/table.h>
#include <memory>
#include <unordered_set>

std::shared_ptr<arrow::Table> loadTableFromParquet(const std::string& path, std::unordered_set<std::string> columns);