#include <initializer_list>
#include <string>
#include "arrow/api.h"

std::shared_ptr<arrow::Table> loadTableFromFiles(std::initializer_list<std::string> paths);