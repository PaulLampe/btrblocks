#include <arrow/api.h>

void checkCompressTableChunkwise(std::shared_ptr<arrow::Table>& table);

void checkCompressTableColumnwise(std::shared_ptr<arrow::Table>& table);