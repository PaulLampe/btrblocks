#include <arrow/array/array_base.h>
#include <arrow/result.h>
#include <arrow/api.h>
#include <arrow/io/api.h>
#include <arrow/type.h>
#include <arrow/type_fwd.h>
#include <parquet/arrow/reader.h>
#include <algorithm>
#include <cstddef>
#include <memory>
#include <string>
#include <vector>

using namespace std;

std::shared_ptr<arrow::Table> loadTableFromParquet(const std::string& path, unordered_set<string> columns){
    arrow::MemoryPool* pool = arrow::default_memory_pool();
    std::shared_ptr<arrow::io::RandomAccessFile> input = arrow::io::ReadableFile::Open(path).ValueOrDie();

    std::unique_ptr<parquet::arrow::FileReader> arrow_reader;
    parquet::arrow::OpenFile(input, pool, &arrow_reader).ok();

    std::shared_ptr<arrow::Table> table;
    arrow_reader->ReadTable(&table).ok();

    vector<int> columnIndices{};

    for (int i = 0; i < table->num_columns(); ++i) {
        auto name = table->field(i)->name();
        if (columns.size() == 0 || columns.find(name) != columns.end()) {
            columnIndices.push_back(i);
        }
    }

    return table->SelectColumns(columnIndices).ValueOrDie();
}