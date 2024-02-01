#include <cassert>
#include "common/Units.hpp"
#include "gtest/gtest.h"
#include "btrblocks.hpp"

TEST(ArrowMetaData, SerializeAndDeserialize)
{
  btrblocks::ArrowMetaData meta{
    static_cast<btrblocks::u32>(2),
    static_cast<btrblocks::u32>(3),
    {
      btrblocks::ArrowMetaData::ArrowColumnMetaData {
        static_cast<btrblocks::u32>(1),
        btrblocks::ColumnType::INTEGER,
        "col_1",
        std::vector<btrblocks::u32>{3}
      },
      btrblocks::ArrowMetaData::ArrowColumnMetaData {
        static_cast<btrblocks::u32>(2),
        btrblocks::ColumnType::INTEGER,
        "col_2",
        std::vector<btrblocks::u32>{1,2}
      }
    }
  };

  auto serializedData = meta.serialize();

  auto meta2 = btrblocks::ArrowMetaData(serializedData);

  assert(meta2.numColumns == meta.numColumns);
  assert(meta2.numChunks == meta.numChunks);

  for (btrblocks::SIZE column_i = 0; column_i < meta2.numColumns; column_i++) {
    auto columnMeta = meta.columnMeta[column_i];
    auto columnMeta2 = meta.columnMeta[column_i];

    assert(columnMeta2.numParts == columnMeta.numParts);

    assert(columnMeta2.name == columnMeta.name);

    assert(columnMeta2.chunksPerPart == columnMeta.chunksPerPart);
  }
}