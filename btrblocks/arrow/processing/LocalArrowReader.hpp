#include <arrow/record_batch.h>
#include <arrow/type.h>
#include <arrow/type_fwd.h>
#include <memory>
#include <string>
#include <tuple>
#include <vector>
#include "arrow/ArrowMetaData.hpp"
#include "common/Units.hpp"
#include "common/Utils.hpp"

namespace btrblocks {

using namespace std;

class LocalArrowReader {

public:

  explicit LocalArrowReader(const string& folder): folder(folder) {
    vector<u8> rawMetaDataData;
    Utils::readFileToMemory(folder + "/metadata.btr", rawMetaDataData);

    metadata = ArrowMetaData(rawMetaDataData);
  };

  void scan(const vector<string>& columns, const function<void(shared_ptr<arrow::RecordBatch>)>& callback);

  shared_ptr<arrow::Schema> getSchema();

private:

  ArrowMetaData metadata;
  string folder;

  vector<vector<vector<u8>>> mmapColumns(vector<SIZE>& columnIndices);
  vector<SIZE> resolveColumnIndices(const vector<string>& columns);

  // Resolves which part and at which position each column chunk of a row group is
  // row_group_i -> column_i -> part_i + chunk_i
  vector<vector<tuple<SIZE, SIZE>>> resolveOffsets(vector<SIZE>& columnIndices);

  shared_ptr<arrow::Schema> resolveSchema(vector<SIZE>& columnIndices);
};


} // namespace btrblocks