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

#include <Common/PODArray.h>
#include <DataStreams/MarkInCompressedFile.h>
#include <IO/CompressedReadBufferFromFile.h>
#include <DataStreams/IProfilingBlockInputStream.h>

namespace DB
{
/** Deserializes the stream of blocks from the native binary format (with names and column types).
  * Designed for communication between servers.
  *
  * Can also be used to store data on disk.
  * In this case, can use the index.
  */
class SquashNativeBlockInputStream
{
public:
    /// When not ready, you need to pass more blocks to add function.
    struct SNResult
    {
        bool ready = false;
        Block block;

        explicit SNResult(bool ready_)
            : ready(ready_)
        {}
        explicit SNResult(Block && block_)
            : ready(true)
            , block(std::move(block_))
        {}
    };

    /// For cases when data structure (header) is known in advance.
    /// NOTE We may use header for data validation and/or type conversions. It is not implemented.
    SquashNativeBlockInputStream(
        const Block & header_,
	size_t row_limit_,
        bool align_column_name_with_header_ = false);

    static void readData(
        const IDataType & type,
        IColumn & column,
        ReadBuffer & istr,
        size_t rows,
        double avg_value_size_hint);

    SNResult read(ReadBuffer & istr, bool flush);

private:
    Block header;
    Block accumulated_block;
    size_t row_limit;
    bool align_column_name_with_header = false;

    struct DataTypeWithTypeName
    {
        DataTypeWithTypeName(const DataTypePtr & t, const String & n)
            : type(t)
            , name(n)
        {
        }

        DataTypePtr type;
        String name;
    };
    std::vector<DataTypeWithTypeName> header_datatypes;

    PODArray<double> avg_value_size_hints;
};

using SquashNativeDecoderPtr = std::unique_ptr<SquashNativeBlockInputStream>;
} // namespace DB
