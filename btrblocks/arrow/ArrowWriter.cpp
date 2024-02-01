#include "ArrowWriter.hpp"
#include <fstream>
#include <iterator>
#include <string>
#include "arrow/ArrowMetaData.hpp"
#include "compression/Datablock.hpp"

namespace btrblocks {
  
using namespace std;

void ArrowWriter::writeBtrBlocks(const string& folder, tuple< ArrowMetaData, vector< vector< ColumnPart > > >& compressedData) {
  auto& [meta, data] = compressedData;
  
  writeMetadata(folder, meta);

  for (SIZE column_i = 0; column_i < data.size(); column_i++) {
    for (SIZE part_i = 0; part_i < data[column_i].size(); part_i++) {
      writeColumnPart(folder, column_i, part_i, data[column_i][part_i]);
    }
  }
};

void ArrowWriter::writeMetadata(const string& folder, ArrowMetaData& meta) {
  std::ofstream metadataFile(folder + "/metadata.btr", std::ios::out | std::ios::binary);
  if (!metadataFile.good()) {
    throw Generic_Exception("Opening metadata output file failed");
  }

  auto serializedMetaData = meta.serialize();

  metadataFile.write(reinterpret_cast<char*>(serializedMetaData.data()), serializedMetaData.size());

    std::cout << "Wrote metadata \n";
}

void ArrowWriter::writeColumnPart(const string& folder, SIZE& column_i, SIZE& part_i, ColumnPart& part) {

  auto path = folder + "/column" + to_string(column_i) + "_part" + to_string(part_i) + ".btr";

  std::ofstream partFile(path, std::ios::out | std::ios::binary);

  part.write(partFile);

  std::cout << "Wrote " << path << "\n";
}


}  // namespace btrblocks