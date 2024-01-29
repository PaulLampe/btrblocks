#include <arrow/array/array_base.h>
#include <arrow/type_fwd.h>
#include <arrow/api.h>
#include <memory>
#include <vector>
#include "common/Units.hpp"
#include "ArrowColumnChunkCompressor.hpp"
#include "compression/Datablock.hpp"
#include "btrblocks.hpp"
#include "compression/SchemePicker.hpp"

namespace btrblocks {

using namespace std;

template<> SIZE ArrowColumnChunkCompressor<ColumnType::DOUBLE>::getChunkByteSize(const shared_ptr<arrow::Array>& chunk) {
    return sizeof(DOUBLE) * chunk->length();
}

template<> SIZE ArrowColumnChunkCompressor<ColumnType::DOUBLE>::compress(const shared_ptr<arrow::Array>& chunk, ColumnChunkMeta* meta) {
    auto& config = BtrBlocksConfig::get();

    auto arr = static_pointer_cast<arrow::DoubleArray>(chunk);

    u32 afterSize = 0;

    DoubleSchemePicker::compress(reinterpret_cast<const DOUBLE*>(arr->raw_values()),
                                    chunk->null_bitmap_data(), meta->data, meta->tuple_count,
                                    config.integers.max_cascade_depth, afterSize,
                                    meta->compression_type);

    return afterSize;
}

template<> shared_ptr<arrow::Array> ArrowColumnChunkCompressor<ColumnType::DOUBLE>::decompress(ColumnChunkMeta* columnChunk) {
        auto tupleCount = columnChunk->tuple_count;
        
        auto& compressionScheme = DoubleSchemePicker::MyTypeWrapper::getScheme(columnChunk->compression_type);

        auto newBuffer = arrow::AllocateBuffer(tupleCount * sizeof(DOUBLE) + SIMD_EXTRA_BYTES);
        shared_ptr<arrow::Buffer> dataBuffer{newBuffer.ValueOrDie().release()};
        auto dest = dataBuffer->mutable_data();

        compressionScheme.decompress(
          reinterpret_cast<DOUBLE *>(dest), 
          nullptr,
          columnChunk->data,
          tupleCount, 
          0
        );

        auto nullmapBuffer = decompressBitmap(columnChunk);

        std::vector<std::shared_ptr<arrow::Buffer>> buffers = {nullmapBuffer, dataBuffer};
        auto arrayData = arrow::ArrayData::Make(arrow::float64(), tupleCount, buffers);
        
        return arrow::MakeArray(arrayData);
}

} // namespace btrblocks