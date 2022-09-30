// Copyright 2022 PingCAP, Ltd.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#include <Common/Logger.h>
#include <Core/Types.h>
#include <DataTypes/IDataType.h>
#include <Flash/Coprocessor/ChunkCodec.h>
#include <Flash/Coprocessor/DAGQuerySource.h>
#include <Flash/Coprocessor/DAGResponseWriter.h>
#include <common/logger_useful.h>
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-parameter"
#include <Flash/Mpp/TrackedMppDataPacket.h>
#include <common/ThreadPool.h>
#include <tipb/select.pb.h>

#pragma GCC diagnostic pop

namespace DB
{
/// Serializes the stream of blocks and sends them to TiDB or TiFlash with different serialization paths.
/// When sending data to TiDB, blocks with extra info are written into tipb::SelectResponse, then the whole tipb::SelectResponse is further serialized into mpp::MPPDataPacket.data.
/// Differently when sending data to TiFlash, blocks with only tuples are directly serialized into mpp::MPPDataPacket.chunks, but for the last block, its extra info (like execution summaries) is written into tipb::SelectResponse, then further serialized into mpp::MPPDataPacket.data.
template <class StreamWriterPtr, bool enable_fine_grained_shuffle>
class StreamingDAGResponseWriter : public DAGResponseWriter
{
public:
    StreamingDAGResponseWriter(
        StreamWriterPtr writer_,
        std::vector<Int64> partition_col_ids_,
        TiDB::TiDBCollators collators_,
        tipb::ExchangeType exchange_type_,
        Int64 records_per_chunk_,
        Int64 batch_send_min_limit_,
        bool should_send_exec_summary_at_last_,
        DAGContext & dag_context_,
        UInt64 fine_grained_shuffle_stream_count_,
        UInt64 fine_grained_shuffle_batch_size_,
	bool reuse_scattered_columns_flag_,
	const String & req_id = "",
	int stream_id_ = 0);
    void write(const Block & block, bool finish) override;
    void finishWrite() override;

private:
    template <bool send_exec_summary_at_last>
    void batchWrite();
    template <bool send_exec_summary_at_last>
    void batchWriteFineGrainedShuffle();

    template <bool send_exec_summary_at_last>
    void encodeThenWriteBlocks(const std::vector<Block> & input_blocks, TrackedSelectResp & response) const;
    template <bool send_exec_summary_at_last>
    void partitionAndEncodeThenWriteBlocks(std::vector<Block> & input_blocks, TrackedSelectResp & response) const;

    template <bool send_exec_summary_at_last>
    void handleExecSummary(const std::vector<Block> & input_blocks,
                           std::vector<TrackedMppDataPacket> & packet,
                           tipb::SelectResponse & response) const;
    template <bool send_exec_summary_at_last>
    void writePackets(const std::vector<size_t> & responses_row_count, std::vector<TrackedMppDataPacket> & packets) const;

    void resetScatterColumns();

    Int64 batch_send_min_limit;
    bool should_send_exec_summary_at_last; /// only one stream needs to sending execution summaries at last.
    tipb::ExchangeType exchange_type;
    StreamWriterPtr writer;
    std::vector<Block> blocks;
    std::vector<Int64> partition_col_ids;
    TiDB::TiDBCollators collators;
    size_t rows_in_blocks;
    uint16_t partition_num;
    std::unique_ptr<ChunkCodecStream> chunk_codec_stream;
    UInt64 fine_grained_shuffle_stream_count;
    UInt64 fine_grained_shuffle_batch_size;
    bool reuse_scattered_columns_flag = false;
    bool inited = false;
    Block header;
    size_t num_columns, num_bucket;
    std::vector<String> partition_key_containers_for_reuse;
    WeakHash32 hash;
    IColumn::Selector selector;
    std::vector<IColumn::ScatterColumns> scattered; // size = num_columns
    const LoggerPtr log;
    size_t cached_block_count = 0;
    bool first_block = true;
    bool first_packet = true;
    size_t total_blocks = 0;
    int stream_id = 0;
};

} // namespace DB
