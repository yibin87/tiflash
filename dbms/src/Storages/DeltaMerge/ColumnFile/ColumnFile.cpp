// Copyright 2023 PingCAP, Inc.
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

#include <IO/Buffer/MemoryReadWriteBuffer.h>
#include <Storages/DeltaMerge/ColumnFile/ColumnFile.h>
#include <Storages/DeltaMerge/ColumnFile/ColumnFileBig.h>
#include <Storages/DeltaMerge/ColumnFile/ColumnFileDeleteRange.h>
#include <Storages/DeltaMerge/ColumnFile/ColumnFileInMemory.h>
#include <Storages/DeltaMerge/ColumnFile/ColumnFilePersisted.h>
#include <Storages/DeltaMerge/ColumnFile/ColumnFileTiny.h>
#include <Storages/DeltaMerge/RowKeyFilter.h>


namespace DB::DM
{

std::pair<size_t, size_t> ColumnFileReader::copyColumnsData(
    const Columns & from,
    const ColumnPtr & pk_col,
    MutableColumns & to,
    size_t rows_offset,
    size_t rows_limit,
    const RowKeyRange * range)
{
    if (range)
    {
        RowKeyColumnContainer rkcc(pk_col, range->is_common_handle);
        if (rows_limit == 1)
        {
            if (range->check(rkcc.getRowKeyValue(rows_offset)))
            {
                for (size_t col_index = 0; col_index < to.size(); ++col_index)
                    to[col_index]->insertFrom(*from[col_index], rows_offset);
                return {rows_offset, 1};
            }
            else
            {
                return {rows_offset, 0};
            }
        }
        else
        {
            auto [actual_offset, actual_limit]
                = RowKeyFilter::getPosRangeOfSorted(*range, pk_col, rows_offset, rows_limit);
            for (size_t col_index = 0; col_index < to.size(); ++col_index)
                to[col_index]->insertRangeFrom(*from[col_index], actual_offset, actual_limit);
            return {actual_offset, actual_limit};
        }
    }
    else
    {
        if (rows_limit == 1)
        {
            for (size_t col_index = 0; col_index < to.size(); ++col_index)
                to[col_index]->insertFrom(*from[col_index], rows_offset);
        }
        else
        {
            for (size_t col_index = 0; col_index < to.size(); ++col_index)
                to[col_index]->insertRangeFrom(*from[col_index], rows_offset, rows_limit);
        }
        return {rows_offset, rows_limit};
    }
}

ColumnFileInMemory * ColumnFile::tryToInMemoryFile()
{
    return !isInMemoryFile() ? nullptr : static_cast<ColumnFileInMemory *>(this);
}

ColumnFileTiny * ColumnFile::tryToTinyFile()
{
    return !isTinyFile() ? nullptr : static_cast<ColumnFileTiny *>(this);
}

ColumnFileDeleteRange * ColumnFile::tryToDeleteRange()
{
    return !isDeleteRange() ? nullptr : static_cast<ColumnFileDeleteRange *>(this);
}

ColumnFileBig * ColumnFile::tryToBigFile()
{
    return !isBigFile() ? nullptr : static_cast<ColumnFileBig *>(this);
}

ColumnFilePersisted * ColumnFile::tryToColumnFilePersisted()
{
    return !isPersisted() ? nullptr : static_cast<ColumnFilePersisted *>(this);
}

template <class T>
String ColumnFile::filesToString(const T & column_files)
{
    FmtBuffer buffer;
    buffer.append("[");
    buffer.joinStr(
        column_files.cbegin(),
        column_files.cend(),
        [](const auto & f, FmtBuffer & fb) {
            if (f->isInMemoryFile())
                fb.fmtAppend("M_{}", f->getRows());
            else if (f->isTinyFile())
                fb.fmtAppend("T_{}", f->getRows());
            else if (f->isBigFile())
                fb.fmtAppend("F_{}", f->getRows());
            else if (auto * f_delete = f->tryToDeleteRange(); f_delete)
                fb.fmtAppend("D_{}", f_delete->getDeleteRange().toString());
        },
        ",");
    buffer.append("]");
    return buffer.toString();
}

template String ColumnFile::filesToString<ColumnFiles>(const ColumnFiles & column_files);
template String ColumnFile::filesToString<ColumnFilePersisteds>(const ColumnFilePersisteds & column_files);

} // namespace DB::DM
