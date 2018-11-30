/*
 * Copyright 2016-2018 shardingsphere.io.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * </p>
 */

package io.shardingsphere.shardingproxy.backend.netty.result.executor;

import com.google.common.base.Supplier;
import io.netty.channel.Channel;
import io.shardingsphere.core.constant.SQLType;
import io.shardingsphere.core.merger.QueryResult;
import io.shardingsphere.core.parsing.parser.sql.SQLStatement;
import io.shardingsphere.shardingproxy.backend.netty.NettyBackendHandler;
import io.shardingsphere.shardingproxy.backend.netty.client.response.mysql.MySQLQueryResult;
import io.shardingsphere.shardingproxy.backend.netty.result.collector.QueryResultCollector;
import io.shardingsphere.shardingproxy.frontend.mysql.CommandExecutor;
import io.shardingsphere.shardingproxy.runtime.ChannelRegistry;
import io.shardingsphere.shardingproxy.transport.mysql.packet.command.CommandPacket;
import io.shardingsphere.shardingproxy.transport.mysql.packet.command.CommandResponsePackets;
import io.shardingsphere.shardingproxy.util.ChannelUtils;
import lombok.RequiredArgsConstructor;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

/**
 * Query result executor for processing query results.
 *
 * @author wuxu
 */
@RequiredArgsConstructor
public class QueryResultExecutor implements Runnable {
    
    private final QueryResultCollector queryResultCollector;
    
    @Override
    public final void run() {
        processResults();
    }
    
    private void processResults() {
        List<QueryResult> queryResults = queryResultCollector.getResponses();
        NettyBackendHandler nettyBackendHandler = queryResultCollector.getNettyBackendHandler();
        SQLStatement sqlStatement = queryResultCollector.getSqlStatement();
        Channel frontendChannel = ChannelRegistry.FRONTEND_CHANNEL.get(queryResultCollector.getLongChannelId());
        CommandExecutor commandExecutor = ChannelRegistry.FRONTEND_CHANNEL_COMMAND_EXECUTOR.get(ChannelUtils.getLongTextId(frontendChannel));
        CommandResponsePackets responsePackets;
        if (queryResultCollector.isMasterSlaveSchema()) {
            List<CommandResponsePackets> packets = new LinkedList<>();
            for (QueryResult each : queryResults) {
                packets.add(((MySQLQueryResult) each).getCommandResponsePackets());
            }
            responsePackets = nettyBackendHandler.merge(sqlStatement, packets, queryResults);
        } else {
            List<CommandResponsePackets> packets = new ArrayList<>(queryResults.size());
            for (QueryResult each : queryResults) {
                MySQLQueryResult queryResult0 = (MySQLQueryResult) each;
                if (0 == nettyBackendHandler.getCurrentSequenceId()) {
                    nettyBackendHandler.setCurrentSequenceId(queryResult0.getCurrentSequenceId());
                }
                if (0 == nettyBackendHandler.getColumnCount()) {
                    nettyBackendHandler.setColumnCount(queryResult0.getColumnCount());
                }
                packets.add(queryResult0.getCommandResponsePackets());
            }
            responsePackets = nettyBackendHandler.merge(sqlStatement, packets, queryResults);
            if (SQLType.DDL == sqlStatement.getType() && !sqlStatement.getTables().isEmpty()) {
                try {
                    nettyBackendHandler.refreshTableMetaData(sqlStatement.getTables().getSingleTableName());
                } catch (SQLException ex) {
                    commandExecutor.writeErrPacket(ex);
                }
            }
        }
        commandExecutor.writeResult(responsePackets, new Supplier<CommandPacket>() {
            @Override
            public CommandPacket get() {
                return ChannelRegistry.FRONTEND_CHANNEL_COMMAND_PACKET.get(queryResultCollector.getCommandPacketId());
            }
        });
    }
}
