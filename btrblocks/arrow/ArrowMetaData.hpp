#pragma once
#include <vector>
#include "common/Units.hpp"
namespace btrblocks {

using namespace std;

struct ArrowMetaData {

  struct ArrowColumnMetaData {
    u32 numParts;
    ColumnType type;
    string name;
    vector<u32> chunksPerPart;

    SIZE serializedSize();

    SIZE writeSerialized(u8* destination);

    explicit ArrowColumnMetaData(u8* source);

    ArrowColumnMetaData(u32 numParts, ColumnType type, string&& name, vector<u32>&& chunksPerPart): numParts(numParts), type(type), name(name), chunksPerPart(chunksPerPart) {};
    ArrowColumnMetaData() = default;
  };

  u32 numColumns;
  u32 numChunks;
  
  vector<ArrowColumnMetaData> columnMeta;

  vector<u8> serialize();

  explicit ArrowMetaData(vector<u8> serializedData);
  ArrowMetaData() = default;
  ArrowMetaData(u32 numColumns, u32 numChunks, vector<ArrowColumnMetaData>&& columnMeta): numColumns(numColumns), numChunks(numChunks), columnMeta(columnMeta) {};

private:

  SIZE serializedSize();
};

} // namespace btrblocks