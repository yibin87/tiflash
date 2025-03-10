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

#pragma once

#include <Client/TimeoutSetter.h>
#include <Common/CurrentMetrics.h>
#include <Common/Stopwatch.h>
#include <Common/getFQDNOrHostName.h>
#include <Core/Protocol.h>
#include <Core/QueryProcessingStage.h>
#include <DataStreams/BlockIO.h>
#include <IO/Progress.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/Context.h>
#include <Poco/Net/TCPServerConnection.h>
#include <Server/IServer.h>
#include <Storages/KVStore/Read/RegionLockInfo.h>


namespace DB
{
/// State of query processing.
struct QueryState
{
    /// Identifier of the query.
    String query_id;

    QueryProcessingStage::Enum stage = QueryProcessingStage::Complete;
    Protocol::Compression compression = Protocol::Compression::Disable;

    /// From where to read data for INSERT.
    std::shared_ptr<ReadBuffer> maybe_compressed_in;
    BlockInputStreamPtr block_in;

    /// Where to write result data.
    std::shared_ptr<WriteBuffer> maybe_compressed_out;
    BlockOutputStreamPtr block_out;

    /// Query text.
    String query;
    /// Streams of blocks, that are processing the query.
    BlockIO io;

    /// Is request cancelled
    bool is_cancelled = false;
    /// empty or not
    bool is_empty = true;
    /// Data was sent.
    bool sent_all_data = false;
    /// Request requires data from the client (INSERT, but not INSERT SELECT).
    bool need_receive_data_for_insert = false;

    /// To output progress, the difference after the previous sending of progress.
    Progress progress;

    /// Timeouts setter for current query
    std::unique_ptr<TimeoutSetter> timeout_setter;


    void reset() { *this = QueryState(); }

    bool empty() const { return is_empty; }
};


class TCPHandler : public Poco::Net::TCPServerConnection
{
public:
    TCPHandler(IServer & server_, const Poco::Net::StreamSocket & socket_)
        : Poco::Net::TCPServerConnection(socket_)
        , server(server_)
        , log(Logger::get("TCPHandler"))
        , connection_context(server.context())
        , query_context(server.context())
    {
        server_display_name = "TiFlash";
    }

    void run() override;

private:
    IServer & server;
    LoggerPtr log;

    String client_name;
    UInt64 client_version_major = 0;
    UInt64 client_version_minor = 0;
    UInt64 client_version_patch = 0;

    Context connection_context;
    Context query_context;

    /// Streams for reading/writing from/to client connection socket.
    std::shared_ptr<ReadBuffer> in;
    std::shared_ptr<WriteBuffer> out;

    /// Time after the last check to stop the request and send the progress.
    Stopwatch after_check_cancelled;
    Stopwatch after_send_progress;

    String default_database;

    /// At the moment, only one ongoing query in the connection is supported at a time.
    QueryState state;

    /// It is the name of the server that will be sent to the client.
    String server_display_name;

    void runImpl();

    void receiveHello();
    bool receivePacket();
    void receiveQuery();
    bool receiveData();
    void readData(const Settings & global_settings);

    /// Process INSERT query
    void processInsertQuery(const Settings & global_settings);

    /// Process a request that does not require the receiving of data blocks from the client
    void processOrdinaryQuery();
    void processSharedQuery();
    void processTablesStatusRequest();

    void sendHello();
    void sendData(const Block & block); /// Write a block to the network.
    void sendException(const Exception & e);
    void sendRegionException(const std::vector<UInt64> & region_ids);
    void sendLockInfos(const LockInfoPtr & lock_info);
    void sendProgress();
    void sendEndOfStream();
    void sendProfileInfo();
    void sendExtremes();

    /// Creates state.block_in/block_out for blocks read/write, depending on whether compression is enabled.
    void initBlockInput();
    void initBlockOutput(const Block & block);

    bool isQueryCancelled();

    /// This function is called from different threads.
    void updateProgress(const Progress & value);
};

} // namespace DB
