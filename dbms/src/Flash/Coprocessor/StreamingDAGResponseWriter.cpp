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

#include <Common/Logger.h>
#include <Common/TiFlashException.h>
#include <DataStreams/IProfilingBlockInputStream.h>
#include <Flash/Coprocessor/ArrowChunkCodec.h>
#include <Flash/Coprocessor/CHBlockChunkCodec.h>
#include <Flash/Coprocessor/DefaultChunkCodec.h>
#include <Flash/Coprocessor/StreamWriter.h>
#include <Flash/Coprocessor/StreamingDAGResponseWriter.h>
#include <Flash/Mpp/MPPTunnelSet.h>
#include <Flash/Mpp/TrackedMppDataPacket.h>
#include <Interpreters/AggregationCommon.h>

#include <iostream>

namespace DB
{
namespace ErrorCodes
{
extern const int UNSUPPORTED_PARAMETER;
extern const int LOGICAL_ERROR;
} // namespace ErrorCodes

inline void serializeToPacket(mpp::MPPDataPacket & packet, const tipb::SelectResponse & response)
{
    if (!response.SerializeToString(packet.mutable_data()))
        throw Exception(fmt::format("Fail to serialize response, response size: {}", response.ByteSizeLong()));
}

template <class StreamWriterPtr, bool enable_fine_grained_shuffle>
StreamingDAGResponseWriter<StreamWriterPtr, enable_fine_grained_shuffle>::StreamingDAGResponseWriter(
    StreamWriterPtr writer_,
    std::vector<Int64> partition_col_ids_,
    TiDB::TiDBCollators collators_,
    tipb::ExchangeType exchange_type_,
    Int64 records_per_chunk_,
    Int64 batch_send_min_limit_,
    bool should_send_exec_summary_at_last_,
    DAGContext & dag_context_,
    uint64_t fine_grained_shuffle_stream_count_,
    UInt64 fine_grained_shuffle_batch_size_,
    bool reuse_scattered_columns_flag_,
    const String & req_id,
    int stream_id_)
    : DAGResponseWriter(records_per_chunk_, dag_context_)
    , batch_send_min_limit(batch_send_min_limit_)
    , should_send_exec_summary_at_last(should_send_exec_summary_at_last_)
    , exchange_type(exchange_type_)
    , writer(writer_)
    , partition_col_ids(std::move(partition_col_ids_))
    , collators(std::move(collators_))
    , fine_grained_shuffle_stream_count(fine_grained_shuffle_stream_count_)
    , fine_grained_shuffle_batch_size(fine_grained_shuffle_batch_size_)
    , reuse_scattered_columns_flag(reuse_scattered_columns_flag_)
    , hash(0)
    , log(Logger::get("StreamingDagResponseWriter", req_id))
    , stream_id(stream_id_)
{
    rows_in_blocks = 0;
    partition_num = writer_->getPartitionNum();
    switch (dag_context.encode_type)
    {
    case tipb::EncodeType::TypeDefault:
        chunk_codec_stream = std::make_unique<DefaultChunkCodec>()->newCodecStream(dag_context.result_field_types);
        break;
    case tipb::EncodeType::TypeChunk:
        chunk_codec_stream = std::make_unique<ArrowChunkCodec>()->newCodecStream(dag_context.result_field_types);
        break;
    case tipb::EncodeType::TypeCHBlock:
        chunk_codec_stream = std::make_unique<CHBlockChunkCodec>()->newCodecStream(dag_context.result_field_types);
        break;
    }
}

template <class StreamWriterPtr, bool enable_fine_grained_shuffle>
void StreamingDAGResponseWriter<StreamWriterPtr, enable_fine_grained_shuffle>::finishWrite()
{
    if (should_send_exec_summary_at_last)
    {
        if constexpr (enable_fine_grained_shuffle)
        {
	    //LOG_FMT_INFO(log, "FinishWrite block count {} {}", cached_block_count, blocks.size());
            assert(exchange_type == tipb::ExchangeType::Hash);
            batchWriteFineGrainedShuffle<true>();
        }
        else
        {
            batchWrite<true>();
        }
    }
    else
    {
        if constexpr (enable_fine_grained_shuffle)
        {
	    //LOG_FMT_INFO(log, "FinishWrite block count {} {}", cached_block_count, blocks.size());
            assert(exchange_type == tipb::ExchangeType::Hash);
            batchWriteFineGrainedShuffle<false>();
        }
        else
        {
            batchWrite<false>();
        }
    }
}

template <class StreamWriterPtr, bool enable_fine_grained_shuffle>
void StreamingDAGResponseWriter<StreamWriterPtr, enable_fine_grained_shuffle>::write(const Block & block, bool finish)
{
    if (unlikely(finish))
    {
	if constexpr (enable_fine_grained_shuffle)
    	{
    	    assert(exchange_type == tipb::ExchangeType::Hash);
	    LOG_FMT_INFO(log, "Finish Flag True {} {}", cached_block_count, total_blocks);
    	    if (cached_block_count > 0) {
    	    //if (static_cast<UInt64>(rows_in_blocks) >= fine_grained_shuffle_batch_size)
    	    //if (static_cast<UInt64>(rows_in_blocks) >= 60000 * fine_grained_shuffle_stream_count)
	    	//LOG_FMT_INFO(log, "LastTime SendPacket {} {}", cached_block_count, blocks.size());
    	        batchWriteFineGrainedShuffle<false>();
    	        cached_block_count = 0;
    	    }
    	}
	return;
    }

    if (block.columns() != dag_context.result_field_types.size())
        throw TiFlashException("Output column size mismatch with field type size", Errors::Coprocessor::Internal);
    size_t rows = block.rows();
    rows_in_blocks += rows;
    if (rows > 0)
    {
        blocks.push_back(block);
	cached_block_count++;
	total_blocks++;
	if (first_block)
	{
	    first_block = false;
	    LOG_FMT_INFO(log, "FirstTime WriteBlock");
	}
    }

    if constexpr (enable_fine_grained_shuffle)
    {
        assert(exchange_type == tipb::ExchangeType::Hash);
	//if (cached_block_count == fine_grained_shuffle_stream_count || rows_in_blocks >= 4096 * fine_grained_shuffle_stream_count) {
	if (cached_block_count == fine_grained_shuffle_stream_count) {
        //if (static_cast<UInt64>(rows_in_blocks) >= fine_grained_shuffle_batch_size)
        //if (static_cast<UInt64>(rows_in_blocks) >= 60000 * fine_grained_shuffle_stream_count)
            batchWriteFineGrainedShuffle<false>();
	    cached_block_count = 0;
	}
    }
    else
    {
        if (static_cast<Int64>(rows_in_blocks) > (dag_context.encode_type == tipb::EncodeType::TypeCHBlock ? batch_send_min_limit : records_per_chunk - 1))
            batchWrite<false>();
    }
}

template <class StreamWriterPtr, bool enable_fine_grained_shuffle>
template <bool send_exec_summary_at_last>
void StreamingDAGResponseWriter<StreamWriterPtr, enable_fine_grained_shuffle>::encodeThenWriteBlocks(
    const std::vector<Block> & input_blocks,
    TrackedSelectResp & response) const
{
    if (dag_context.encode_type == tipb::EncodeType::TypeCHBlock)
    {
        if (dag_context.isMPPTask()) /// broadcast data among TiFlash nodes in MPP
        {
            TrackedMppDataPacket tracked_packet(current_memory_tracker);
            if constexpr (send_exec_summary_at_last)
            {
                tracked_packet.serializeByResponse(response.getResponse());
            }
            if (input_blocks.empty())
            {
                if constexpr (send_exec_summary_at_last)
                {
                    writer->write(tracked_packet.getPacket());
                }
                return;
            }
            for (const auto & block : input_blocks)
            {
                chunk_codec_stream->encode(block, 0, block.rows());
                tracked_packet.addChunk(chunk_codec_stream->getString());
                chunk_codec_stream->clear();
            }
            writer->write(tracked_packet.getPacket());
        }
        else /// passthrough data to a non-TiFlash node, like sending data to TiSpark
        {
            response.setEncodeType(dag_context.encode_type);
            if (input_blocks.empty())
            {
                if constexpr (send_exec_summary_at_last)
                {
                    writer->write(response.getResponse());
                }
                return;
            }
            for (const auto & block : input_blocks)
            {
                chunk_codec_stream->encode(block, 0, block.rows());
                response.addChunk(chunk_codec_stream->getString());
                chunk_codec_stream->clear();
            }
            writer->write(response.getResponse());
        }
    }
    else /// passthrough data to a TiDB node
    {
        response.setEncodeType(dag_context.encode_type);
        if (input_blocks.empty())
        {
            if constexpr (send_exec_summary_at_last)
            {
                writer->write(response.getResponse());
            }
            return;
        }

        Int64 current_records_num = 0;
        for (const auto & block : input_blocks)
        {
            size_t rows = block.rows();
            for (size_t row_index = 0; row_index < rows;)
            {
                if (current_records_num >= records_per_chunk)
                {
                    response.addChunk(chunk_codec_stream->getString());
                    chunk_codec_stream->clear();
                    current_records_num = 0;
                }
                const size_t upper = std::min(row_index + (records_per_chunk - current_records_num), rows);
                chunk_codec_stream->encode(block, row_index, upper);
                current_records_num += (upper - row_index);
                row_index = upper;
            }
        }

        if (current_records_num > 0)
        {
            response.addChunk(chunk_codec_stream->getString());
            chunk_codec_stream->clear();
        }
        writer->write(response.getResponse());
    }
}


template <class StreamWriterPtr, bool enable_fine_grained_shuffle>
template <bool send_exec_summary_at_last>
void StreamingDAGResponseWriter<StreamWriterPtr, enable_fine_grained_shuffle>::batchWrite()
{
    TrackedSelectResp response;
    if constexpr (send_exec_summary_at_last)
        addExecuteSummaries(response.getResponse(), !dag_context.isMPPTask() || dag_context.isRootMPPTask());
    if (exchange_type == tipb::ExchangeType::Hash)
    {
        partitionAndEncodeThenWriteBlocks<send_exec_summary_at_last>(blocks, response);
    }
    else
    {
        encodeThenWriteBlocks<send_exec_summary_at_last>(blocks, response);
    }
    blocks.clear();
    rows_in_blocks = 0;
}

template <class StreamWriterPtr, bool enable_fine_grained_shuffle>
template <bool send_exec_summary_at_last>
void StreamingDAGResponseWriter<StreamWriterPtr, enable_fine_grained_shuffle>::handleExecSummary(
    const std::vector<Block> & input_blocks,
    std::vector<TrackedMppDataPacket> & packets,
    tipb::SelectResponse & response) const
{
    if constexpr (send_exec_summary_at_last)
    {
        /// Sending the response to only one node, default the first one.
        packets[0].serializeByResponse(response);

        // No need to send data when blocks are not empty,
        // because exec_summary will be sent together with blocks.
        if (input_blocks.empty())
        {
            for (auto part_id = 0; part_id < partition_num; ++part_id)
            {
                writer->write(packets[part_id].getPacket(), part_id);
            }
        }
    }
}

template <class StreamWriterPtr, bool enable_fine_grained_shuffle>
template <bool send_exec_summary_at_last>
void StreamingDAGResponseWriter<StreamWriterPtr, enable_fine_grained_shuffle>::writePackets(
    const std::vector<size_t> & responses_row_count,
    std::vector<TrackedMppDataPacket> & packets) const
{
    for (size_t part_id = 0; part_id < packets.size(); ++part_id)
    {
        if constexpr (send_exec_summary_at_last)
        {
            writer->write(packets[part_id].getPacket(), part_id);
        }
        else
        {
            if (responses_row_count[part_id] > 0)
                writer->write(packets[part_id].getPacket(), part_id);
        }
    }
}

inline void initInputBlocks(std::vector<Block> & input_blocks)
{
    for (auto & input_block : input_blocks)
    {
        for (size_t i = 0; i < input_block.columns(); ++i)
        {
            if (ColumnPtr converted = input_block.getByPosition(i).column->convertToFullColumnIfConst())
                input_block.getByPosition(i).column = converted;
        }
    }
}

inline void initDestColumns(const Block & input_block, std::vector<MutableColumns> & dest_tbl_cols)
{
    for (auto & cols : dest_tbl_cols)
    {
        cols = input_block.cloneEmptyColumns();
    }
}

void computeHash(const Block & input_block,
                 uint32_t bucket_num,
                 const TiDB::TiDBCollators & collators,
                 std::vector<String> & partition_key_containers,
                 const std::vector<Int64> & partition_col_ids,
                 std::vector<std::vector<MutableColumnPtr>> & result_columns)
{
    size_t rows = input_block.rows();
    WeakHash32 hash(rows);

    // get hash values by all partition key columns
    for (size_t i = 0; i < partition_col_ids.size(); ++i)
    {
        input_block.getByPosition(partition_col_ids[i]).column->updateWeakHash32(hash, collators[i], partition_key_containers[i]);
    }

    const auto & hash_data = hash.getData();

    // partition each row
    IColumn::Selector selector(rows);
    for (size_t row = 0; row < rows; ++row)
    {
        /// Row from interval [(2^32 / bucket_num) * i, (2^32 / bucket_num) * (i + 1)) goes to bucket with number i.
        selector[row] = hash_data[row]; /// [0, 2^32)
        selector[row] *= bucket_num; /// [0, bucket_num * 2^32), selector stores 64 bit values.
        selector[row] >>= 32u; /// [0, bucket_num)
    }

    for (size_t col_id = 0; col_id < input_block.columns(); ++col_id)
    {
        // Scatter columns to different partitions
        std::vector<MutableColumnPtr> part_columns = input_block.getByPosition(col_id).column->scatter(bucket_num, selector);
        assert(part_columns.size() == bucket_num);
        for (size_t bucket_idx = 0; bucket_idx < bucket_num; ++bucket_idx)
        {
            result_columns[bucket_idx][col_id] = std::move(part_columns[bucket_idx]);
        }
    }
}

static inline size_t nextPowOfTwo(size_t n)
{
    size_t t = 1;
    while (t < n)
        t <<= 1;
    return t;
}

void computeHashForReuse(
    const Block & block,
    uint32_t num_bucket,
    uint32_t num_columns,
    const TiDB::TiDBCollators & collators,
    std::vector<String> & partition_key_containers,
    const std::vector<Int64> & partition_col_ids,
    WeakHash32 & hash,
    IColumn::Selector & selector,
    std::vector<IColumn::ScatterColumns> & scattered)
{
    size_t num_rows = block.rows();
    // compute hash values
    hash.getData().reserve(nextPowOfTwo(num_rows));
    hash.reset(num_rows);
    for (size_t i = 0; i < partition_col_ids.size(); ++i)
    {
        const auto & column = block.getByPosition(partition_col_ids[i]).column;
        column->updateWeakHash32(hash, collators[i], partition_key_containers[i]);
    }

    // fill selector array with most significant bits of hash values
    selector.reserve(nextPowOfTwo(num_rows));
    selector.resize(num_rows);
    const auto & hash_data = hash.getData();
    for (size_t i = 0; i < num_rows; ++i)
    {
        /// Row from interval [(2^32 / num_bucket) * i, (2^32 / num_bucket) * (i + 1)) goes to bucket with number i.
        selector[i] = hash_data[i]; /// [0, 2^32)
        selector[i] *= num_bucket; /// [0, num_bucket * 2^32), selector stores 64 bit values.
        selector[i] >>= 32u; /// [0, num_bucket)
    }

    // partition
    for (size_t i = 0; i < num_columns; ++i)
    {
        const auto & column = block.getByPosition(i).column;
        column->scatterTo(scattered[i], selector);
    }
}

template <class StreamWriterPtr, bool enable_fine_grained_shuffle>
void StreamingDAGResponseWriter<StreamWriterPtr, enable_fine_grained_shuffle>::resetScatterColumns()
{
    scattered.resize(num_columns);
    for (size_t col_id = 0; col_id < num_columns; ++col_id)
    {
        auto & column = header.getByPosition(col_id).column;

        scattered[col_id].reserve(num_bucket);
        for (size_t chunk_id = 0; chunk_id < num_bucket; ++chunk_id)
        {
            scattered[col_id].emplace_back(column->cloneEmpty());
            scattered[col_id][chunk_id]->reserve(1024);
        }
    }
}

/// Hash exchanging data among only TiFlash nodes. Only be called when enable_fine_grained_shuffle is false.
template <class StreamWriterPtr, bool enable_fine_grained_shuffle>
template <bool send_exec_summary_at_last>
void StreamingDAGResponseWriter<StreamWriterPtr, enable_fine_grained_shuffle>::partitionAndEncodeThenWriteBlocks(
    std::vector<Block> & input_blocks,
    TrackedSelectResp & response) const
{
    static_assert(!enable_fine_grained_shuffle);
    std::vector<TrackedMppDataPacket> tracked_packets(partition_num);
    std::vector<size_t> responses_row_count(partition_num);
    handleExecSummary<send_exec_summary_at_last>(input_blocks, tracked_packets, response.getResponse());
    if (input_blocks.empty())
        return;

    initInputBlocks(input_blocks);
    Block dest_block = input_blocks[0].cloneEmpty();
    std::vector<String> partition_key_containers(collators.size());
    for (const auto & block : input_blocks)
    {
        std::vector<MutableColumns> dest_tbl_cols(partition_num);
        initDestColumns(block, dest_tbl_cols);

        computeHash(block, partition_num, collators, partition_key_containers, partition_col_ids, dest_tbl_cols);

        for (size_t part_id = 0; part_id < partition_num; ++part_id)
        {
            dest_block.setColumns(std::move(dest_tbl_cols[part_id]));
            responses_row_count[part_id] += dest_block.rows();
            chunk_codec_stream->encode(dest_block, 0, dest_block.rows());
            tracked_packets[part_id].addChunk(chunk_codec_stream->getString());
            chunk_codec_stream->clear();
        }
    }

    writePackets<send_exec_summary_at_last>(responses_row_count, tracked_packets);
}

/// Hash exchanging data among only TiFlash nodes. Only be called when enable_fine_grained_shuffle is true.
template <class StreamWriterPtr, bool enable_fine_grained_shuffle>
template <bool send_exec_summary_at_last>
void StreamingDAGResponseWriter<StreamWriterPtr, enable_fine_grained_shuffle>::batchWriteFineGrainedShuffle()
{
    static_assert(enable_fine_grained_shuffle);
    assert(exchange_type == tipb::ExchangeType::Hash);
    assert(fine_grained_shuffle_stream_count <= 1024);

    tipb::SelectResponse response;
    if constexpr (send_exec_summary_at_last)
        addExecuteSummaries(response, !dag_context.isMPPTask() || dag_context.isRootMPPTask());

    std::vector<TrackedMppDataPacket> tracked_packets(partition_num);
    std::vector<size_t> responses_row_count(partition_num, 0);
    std::vector<size_t> packet_size(partition_num, 0);

    // fine_grained_shuffle_stream_count is in [0, 1024], and partition_num is uint16_t, so will not overflow.
    uint32_t bucket_num = partition_num * fine_grained_shuffle_stream_count;
    handleExecSummary<send_exec_summary_at_last>(blocks, tracked_packets, response);
    if (!blocks.empty())
    {
        std::vector<MutableColumns> final_dest_tbl_columns(bucket_num);
        initInputBlocks(blocks);
        initDestColumns(blocks[0], final_dest_tbl_columns);

        if (reuse_scattered_columns_flag)
        {
            if (!inited)
            {
                header = blocks[0].cloneEmpty();
                num_columns = header.columns();
                num_bucket = bucket_num;
                partition_key_containers_for_reuse.resize(collators.size());
                resetScatterColumns();
                inited = true;
            }

            for (const auto & block : blocks)
            {
                computeHashForReuse(block, num_bucket, num_columns, collators, partition_key_containers_for_reuse, partition_col_ids, hash, selector, scattered);
            }

            // serialize each partitioned block and write it to its destination
            // For i-th stream_count buckets, send to i-th tiflash node.
            size_t part_id = 0;
            for (size_t bucket_idx = 0; bucket_idx < bucket_num; bucket_idx += fine_grained_shuffle_stream_count, ++part_id)
            {
                for (uint64_t stream_idx = 0; stream_idx < fine_grained_shuffle_stream_count; ++stream_idx)
                {
                    // assemble scatter columns into a block
                    MutableColumns columns;
                    columns.reserve(num_columns);
                    for (size_t col_id = 0; col_id < num_columns; ++col_id)
                        columns.emplace_back(std::move(scattered[col_id][bucket_idx + stream_idx]));
                    auto block = header.cloneWithColumns(std::move(columns));
                    // encode into packet
                    responses_row_count[part_id] += block.rows();
                    chunk_codec_stream->encode(block, 0, block.rows());
		    size_t cur_packet_size = 0;
                    tracked_packets[part_id].addChunk(chunk_codec_stream->getString(), cur_packet_size);
                    tracked_packets[part_id].packet.add_stream_ids(stream_idx);
                    chunk_codec_stream->clear();
		    packet_size[part_id] += cur_packet_size;
                    // disassemble the block back to scatter columns
                    columns = block.mutateColumns();
                    for (size_t col_id = 0; col_id < num_columns; ++col_id)
                    {
                        columns[col_id]->popBack(columns[col_id]->size()); // clear column
                        scattered[col_id][bucket_idx + stream_idx] = std::move(columns[col_id]);
                    }
                }
            }
        }
	else
        {
            std::vector<MutableColumns> final_dest_tbl_columns(bucket_num);
            initDestColumns(blocks[0], final_dest_tbl_columns);
            // Hash partition input_blocks into bucket_num.
            for (const auto & block : blocks)
            {
                std::vector<String> partition_key_containers(collators.size());
                std::vector<MutableColumns> dest_tbl_columns(bucket_num);
                initDestColumns(block, dest_tbl_columns);
                computeHash(block, bucket_num, collators, partition_key_containers, partition_col_ids, dest_tbl_columns);
                for (size_t bucket_idx = 0; bucket_idx < bucket_num; ++bucket_idx)
                {
                    for (size_t col_id = 0; col_id < block.columns(); ++col_id)
                    {
                        const MutableColumnPtr & src_col = dest_tbl_columns[bucket_idx][col_id];
                        final_dest_tbl_columns[bucket_idx][col_id]->insertRangeFrom(*src_col, 0, src_col->size());
                    }
                }
            }

            // For i-th stream_count buckets, send to i-th tiflash node.
            for (size_t bucket_idx = 0; bucket_idx < bucket_num; bucket_idx += fine_grained_shuffle_stream_count)
            {
                size_t part_id = bucket_idx / fine_grained_shuffle_stream_count; // NOLINT(clang-analyzer-core.DivideZero)
                size_t row_count_per_part = 0;
                for (uint64_t stream_idx = 0; stream_idx < fine_grained_shuffle_stream_count; ++stream_idx)
                {
                    Block dest_block = blocks[0].cloneEmpty();
                    // For now we put all rows into one Block, may cause this Block too large.
                    dest_block.setColumns(std::move(final_dest_tbl_columns[bucket_idx + stream_idx]));
                    row_count_per_part += dest_block.rows();

                    chunk_codec_stream->encode(dest_block, 0, dest_block.rows());
                    tracked_packets[part_id].addChunk(chunk_codec_stream->getString());
                    tracked_packets[part_id].packet.add_stream_ids(stream_idx);
                    chunk_codec_stream->clear();
                }
                responses_row_count[part_id] = row_count_per_part;
            }
        }
    }
    if (first_packet && stream_id == 1)
    {
        //first_packet = false;
        LOG_FMT_INFO(log, "FirstTime SendPacket PacketSize {}", packet_size[0]);
    }
    writePackets<send_exec_summary_at_last>(responses_row_count, tracked_packets);

    blocks.clear();
    rows_in_blocks = 0;
}

template class StreamingDAGResponseWriter<StreamWriterPtr, /*enable_fine_grained_shuffle=*/true>;
template class StreamingDAGResponseWriter<MPPTunnelSetPtr, /*enable_fine_grained_shuffle=*/true>;
template class StreamingDAGResponseWriter<StreamWriterPtr, /*enable_fine_grained_shuffle=*/false>;
template class StreamingDAGResponseWriter<MPPTunnelSetPtr, /*enable_fine_grained_shuffle=*/false>;

} // namespace DB
