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

#include <Common/typeid_cast.h>
#include <Core/Defines.h>
#include <DataStreams/SquashNativeBlockInputStream.h>
#include <DataTypes/DataTypeFactory.h>
#include <IO/CompressedReadBufferFromFile.h>
#include <IO/ReadHelpers.h>
#include <IO/VarInt.h>
#include <fmt/core.h>

#include <ext/range.h>


namespace DB
{
namespace ErrorCodes
{
extern const int INCORRECT_INDEX;
extern const int LOGICAL_ERROR;
extern const int CANNOT_READ_ALL_DATA;
extern const int NOT_IMPLEMENTED;
} // namespace ErrorCodes

namespace
{
void checkColumnSize(size_t expected, size_t actual)
{
    if (expected != actual)
        throw Exception(
            fmt::format("SquashNativeBlockInputStream schema mismatch, expected {}, actual {}.", expected, actual),
            ErrorCodes::LOGICAL_ERROR);
}

void checkDataTypeName(size_t column_index, const String & expected, const String & actual)
{
    if (expected != actual)
        throw Exception(
            fmt::format(
                "SquashNativeBlockInputStream schema mismatch at column {}, expected {}, actual {}",
                column_index,
                expected,
                actual),
            ErrorCodes::LOGICAL_ERROR);
}
} // namespace

SquashNativeBlockInputStream::SquashNativeBlockInputStream(
    const Block & header_,
    size_t row_limit_,
    bool align_column_name_with_header_)
    : header(header_)
    , row_limit(row_limit_)
    , align_column_name_with_header(align_column_name_with_header_)
{
    for (const auto & column : header)
        header_datatypes.emplace_back(column.type, column.type->getName());
}

void SquashNativeBlockInputStream::readData(
    const IDataType & type,
    IColumn & column,
    ReadBuffer & istr,
    size_t rows,
    double avg_value_size_hint)
{
    IDataType::InputStreamGetter input_stream_getter = [&](const IDataType::SubstreamPath &) {
        return &istr;
    };
    type.deserializeBinaryBulkWithMultipleStreams(column, input_stream_getter, rows, avg_value_size_hint, false, {});
}

SquashNativeBlockInputStream::SNResult SquashNativeBlockInputStream::read(ReadBuffer & istr, bool flush)
{
    if (flush || istr.eof())
    {
        return SNResult(std::move(accumulated_block));
    }

    /// Dimensions
    size_t columns = 0;
    size_t rows = 0;

    readVarUInt(columns, istr);
    readVarUInt(rows, istr);

    if (header)
        checkColumnSize(header.columns(), columns);

    if (!accumulated_block) {
        for (size_t i = 0; i < columns; ++i)
        {
            ColumnWithTypeAndName column;

            /// Name
            readBinary(column.name, istr);
            /// TODO: may need to throw if header && header[i].name != type_name && !align_column_name_with_header
            if (align_column_name_with_header)
                column.name = header.getByPosition(i).name;

            /// Type
            String type_name;
            readBinary(type_name, istr);
            if (header)
            {
                checkDataTypeName(i, header_datatypes[i].name, type_name);
                column.type = header_datatypes[i].type;
            }

            /// Data
            MutableColumnPtr read_column = column.type->createColumn();
            read_column->reserve(static_cast<size_t>(row_limit * 1.5));

            double avg_value_size_hint = avg_value_size_hints.empty() ? 0 : avg_value_size_hints[i];
            if (rows) /// If no rows, nothing to read.
                readData(*column.type, *read_column, istr, rows, avg_value_size_hint);

            column.column = std::move(read_column);

            accumulated_block.insert(std::move(column));
        }
    } else {
        auto mutable_columns = accumulated_block.mutateColumns();
        for (size_t i = 0; i < columns; ++i)
        {
            ColumnWithTypeAndName column;

            /// Name
            readBinary(column.name, istr);
            /// TODO: may need to throw if header && header[i].name != type_name && !align_column_name_with_header
            if (align_column_name_with_header)
                column.name = header.getByPosition(i).name;

            /// Type
            String type_name;
            readBinary(type_name, istr);
            if (header)
            {
                checkDataTypeName(i, header_datatypes[i].name, type_name);
                column.type = header_datatypes[i].type;
            }

            /// Data
            double avg_value_size_hint = avg_value_size_hints.empty() ? 0 : avg_value_size_hints[i];
            if (rows) /// If no rows, nothing to read.
                readData(*column.type, *(mutable_columns[i]), istr, rows, avg_value_size_hint);
        }
        accumulated_block.setColumns(std::move(mutable_columns));
    }

    auto accumulated_block_rows = accumulated_block.rows();
    if (accumulated_block && accumulated_block_rows >= row_limit)
    {
        /// Return accumulated data and place new block to accumulated data.
        Block res;
        accumulated_block.swap(res);
        return SNResult(std::move(res));
    }
    return SNResult(false);
}

} // namespace DB
