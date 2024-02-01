#include "ArrowMetaData.hpp"
#include <cstring>
#include <vector>
#include "common/Units.hpp"
#include "common/Utils.hpp"

namespace btrblocks {

SIZE ArrowMetaData::ArrowColumnMetaData::serializedSize() {
  SIZE size = sizeof(u32); // numParts

  size += name.length() + sizeof(u32); // name

  size += sizeof(ColumnType); // type

  SIZE d;
  Utils::alignBy(size, sizeof(u32), d);

  size += chunksPerPart.size() * sizeof(u32); // chunksPerPart

  return size;
}

SIZE ArrowMetaData::serializedSize() {
  SIZE size = sizeof(u32) * 2; // numChunks + numParts;

  for (auto& column : columnMeta) {
    size += column.serializedSize();
  }

  return size;
}

SIZE ArrowMetaData::ArrowColumnMetaData::writeSerialized(u8* destination) {
  auto int_dest = reinterpret_cast<u32*>(destination);

  SIZE writeOffset = 0;

  *int_dest = numParts;
  *(int_dest + 1) = name.length();

  writeOffset += sizeof(u32) * 2;
  
  memcpy(destination + writeOffset, name.data(), name.length());

  writeOffset += name.length();

  *(destination + writeOffset) = static_cast<u8>(type);

  writeOffset += sizeof(u8);

  SIZE d;
  Utils::alignBy(writeOffset, sizeof(u32), d);

  memcpy(destination + writeOffset, chunksPerPart.data(), chunksPerPart.size() * sizeof(u32));

  writeOffset += chunksPerPart.size() * sizeof(u32);

  return writeOffset;
}

vector<u8> ArrowMetaData::serialize() {
  vector<u8> serializedData(serializedSize());

  auto data = serializedData.data();

  auto int_data = reinterpret_cast<u32*>(data);

  *int_data = numColumns;
  *(int_data + 1) = numChunks;

  SIZE offset = sizeof(u32) * 2;

  for (auto& column: columnMeta) {
    offset += column.writeSerialized(data + offset);
  }

  return serializedData;
}

ArrowMetaData::ArrowColumnMetaData::ArrowColumnMetaData(u8* data): chunksPerPart() {
  auto int_data = reinterpret_cast<u32*>(data);

  numParts = *int_data;

  u32 nameLength = *(int_data + 1);

  SIZE readOffset = sizeof(u32) * 2;

  name = string(data + readOffset, data + readOffset + nameLength);

  readOffset += nameLength;

  type = static_cast<ColumnType>(*(data + readOffset));

  readOffset += sizeof(ColumnType);

  SIZE d;
  Utils::alignBy(readOffset, sizeof(u32), d);

  chunksPerPart.resize(numParts);

  memcpy(chunksPerPart.data(), reinterpret_cast<u32*>(data + readOffset), numParts * sizeof(u32));
}

ArrowMetaData::ArrowMetaData(vector<u8> serializedData): columnMeta() {
  auto data = serializedData.data();

  auto int_data = reinterpret_cast<u32*>(data);

  numColumns = *int_data;
  numChunks = *(int_data + 1);

  SIZE readOffset = sizeof(u32) * 2;

  for (SIZE column_i = 0; column_i < numColumns; column_i++) {
    auto column = ArrowColumnMetaData(data + readOffset);
    columnMeta.push_back(column);
    readOffset += column.serializedSize();
  }
}

} // namespace btrblocks