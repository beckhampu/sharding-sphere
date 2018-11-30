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

package io.shardingsphere.shardingproxy.backend.netty;

import io.netty.channel.Channel;
import io.shardingsphere.core.constant.DatabaseType;
import io.shardingsphere.core.constant.SQLType;
import io.shardingsphere.core.constant.properties.ShardingPropertiesConstant;
import io.shardingsphere.core.merger.MergeEngineFactory;
import io.shardingsphere.core.merger.MergedResult;
import io.shardingsphere.core.merger.QueryResult;
import io.shardingsphere.core.metadata.table.executor.TableMetaDataLoader;
import io.shardingsphere.core.parsing.SQLJudgeEngine;
import io.shardingsphere.core.parsing.parser.sql.SQLStatement;
import io.shardingsphere.core.routing.RouteUnit;
import io.shardingsphere.core.routing.SQLRouteResult;
import io.shardingsphere.core.routing.StatementRoutingEngine;
import io.shardingsphere.core.routing.router.masterslave.MasterSlaveRouter;
import io.shardingsphere.shardingproxy.backend.AbstractBackendHandler;
import io.shardingsphere.shardingproxy.backend.BackendExecutorContext;
import io.shardingsphere.shardingproxy.backend.ResultPacket;
import io.shardingsphere.shardingproxy.backend.netty.client.BackendNettyClientManager;
import io.shardingsphere.shardingproxy.backend.netty.result.collector.QueryResultCollector;
import io.shardingsphere.shardingproxy.runtime.ChannelRegistry;
import io.shardingsphere.shardingproxy.runtime.GlobalRegistry;
import io.shardingsphere.shardingproxy.runtime.metadata.ProxyTableMetaDataConnectionManager;
import io.shardingsphere.shardingproxy.runtime.schema.LogicSchema;
import io.shardingsphere.shardingproxy.runtime.schema.MasterSlaveSchema;
import io.shardingsphere.shardingproxy.runtime.schema.ShardingSchema;
import io.shardingsphere.shardingproxy.transport.common.packet.DatabasePacket;
import io.shardingsphere.shardingproxy.transport.mysql.constant.ColumnType;
import io.shardingsphere.shardingproxy.transport.mysql.constant.ServerErrorCode;
import io.shardingsphere.shardingproxy.transport.mysql.packet.command.CommandResponsePackets;
import io.shardingsphere.shardingproxy.transport.mysql.packet.command.query.text.query.ComQueryPacket;
import io.shardingsphere.shardingproxy.transport.mysql.packet.generic.ErrPacket;
import io.shardingsphere.shardingproxy.transport.mysql.packet.generic.OKPacket;
import io.shardingsphere.shardingproxy.util.ChannelUtils;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Setter;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

/**
 * Netty backend handler.
 *
 * @author wangkai
 * @author linjiaqi
 * @author panjuan
 */
@RequiredArgsConstructor
@Getter
public final class NettyBackendHandler extends AbstractBackendHandler {
    
    private static final GlobalRegistry GLOBAL_REGISTRY = GlobalRegistry.getInstance();
    
    private static final BackendNettyClientManager CLIENT_MANAGER = BackendNettyClientManager.getInstance();
    
    private final LogicSchema logicSchema;
    
    private final int sequenceId;
    
    private final String sql;
    
    private final DatabaseType databaseType;
    
    @Getter
    @Setter
    private int currentSequenceId;
    
    @Getter
    @Setter
    private int columnCount;
    
    private MergedResult mergedResult;
    
    @Override
    protected CommandResponsePackets execute0() throws InterruptedException, ExecutionException, TimeoutException, SQLException {
        return logicSchema instanceof MasterSlaveSchema ? executeForMasterSlave() : executeForSharding();
    }
    
    private CommandResponsePackets executeForMasterSlave() throws InterruptedException, ExecutionException, TimeoutException {
        String dataSourceName = new MasterSlaveRouter(((MasterSlaveSchema) logicSchema).getMasterSlaveRule(),
                GLOBAL_REGISTRY.getShardingProperties().<Boolean>getValue(ShardingPropertiesConstant.SQL_SHOW)).route(sql).iterator().next();
        SQLStatement sqlStatement = new SQLJudgeEngine(sql).judge();
        QueryResultCollector queryResultCollector = new QueryResultCollector(ChannelRegistry.LOCAL_FRONTEND_CHANNEL.get().id().asLongText(),
                sqlStatement, 1, true, this, ChannelRegistry.COMMAND_PACKET_ID.get());
        executeSQL(dataSourceName, sql, queryResultCollector);
        return null;
    }
    
    private CommandResponsePackets executeForSharding() throws InterruptedException, ExecutionException, TimeoutException, SQLException {
        StatementRoutingEngine routingEngine = new StatementRoutingEngine(
                ((ShardingSchema) logicSchema).getShardingRule(), logicSchema.getMetaData().getTable(), databaseType,
                GLOBAL_REGISTRY.getShardingProperties().<Boolean>getValue(ShardingPropertiesConstant.SQL_SHOW), logicSchema.getMetaData().getDataSource());
        SQLRouteResult routeResult = routingEngine.route(sql);
        if (routeResult.getRouteUnits().isEmpty()) {
            return new CommandResponsePackets(new OKPacket(1));
        }
        QueryResultCollector queryResultCollector = new QueryResultCollector(ChannelRegistry.LOCAL_FRONTEND_CHANNEL.get().id().asLongText(),
                routeResult.getSqlStatement(), routeResult.getRouteUnits().size(),
                false, this, ChannelRegistry.COMMAND_PACKET_ID.get());
        for (RouteUnit each : routeResult.getRouteUnits()) {
            executeSQL(each.getDataSourceName(), each.getSqlUnit().getSql(), queryResultCollector);
        }
        return null;
    }
    
    private void executeSQL(final String dataSourceName, final String sql, final QueryResultCollector queryResultCollector) {
        if (queryResultCollector.isBackendChannelExhausted()) {
            return;
        }
        Channel frontendChannel = ChannelRegistry.LOCAL_FRONTEND_CHANNEL.get();
        Channel backendChannel = CLIENT_MANAGER.getBackendNettyClient(logicSchema.getName()).getEventLoopChannelPoolMap().get(frontendChannel.eventLoop())
                .get(dataSourceName).acquire().getNow();
        if (backendChannel == null) {
            queryResultCollector.setBackendChannelExhausted(true);
            frontendChannel.writeAndFlush(new ErrPacket(++currentSequenceId, ServerErrorCode.ER_EXHAUSTION_BACKEND_CHANNEL_ERROR));
            return;
        }
        String backendChannelLongTextId = ChannelUtils.getLongTextId(backendChannel);
        ChannelRegistry.BACKEND_CHANNEL_QUERY_RESULT_COLLECTOR.put(backendChannelLongTextId, queryResultCollector);
        ChannelRegistry.BACKEND_CHANNEL_DATABASE_NAME.put(backendChannelLongTextId, dataSourceName);
        backendChannel.writeAndFlush(new ComQueryPacket(sequenceId, sql));
    }
    
    /**
     * Merge result set.
     *
     * @param sqlStatement SQL statement
     * @param packets the command response packets
     * @param queryResults the query result list from all databases
     * @return the merged command response packet
     */
    public CommandResponsePackets merge(final SQLStatement sqlStatement, final List<CommandResponsePackets> packets, final List<QueryResult> queryResults) {
        CommandResponsePackets headPackets = new CommandResponsePackets();
        for (CommandResponsePackets each : packets) {
            headPackets.getPackets().add(each.getHeadPacket());
        }
        for (DatabasePacket each : headPackets.getPackets()) {
            if (each instanceof ErrPacket) {
                return new CommandResponsePackets(each);
            }
        }
        if (SQLType.TCL == sqlStatement.getType()) {
            channelRelease();
        }
        if (SQLType.DML == sqlStatement.getType()) {
            return mergeDML(headPackets);
        }
        if (SQLType.DQL == sqlStatement.getType() || SQLType.DAL == sqlStatement.getType()) {
            return mergeDQLorDAL(sqlStatement, packets, queryResults);
        }
        return packets.get(0);
    }
    
    private CommandResponsePackets mergeDML(final CommandResponsePackets firstPackets) {
        int affectedRows = 0;
        long lastInsertId = 0;
        for (DatabasePacket each : firstPackets.getPackets()) {
            if (each instanceof OKPacket) {
                OKPacket okPacket = (OKPacket) each;
                affectedRows += okPacket.getAffectedRows();
                lastInsertId = okPacket.getLastInsertId();
            }
        }
        return new CommandResponsePackets(new OKPacket(1, affectedRows, lastInsertId));
    }
    
    private CommandResponsePackets mergeDQLorDAL(final SQLStatement sqlStatement, final List<CommandResponsePackets> packets, final List<QueryResult> queryResults) {
        try {
            mergedResult = MergeEngineFactory.newInstance(((ShardingSchema) logicSchema).getShardingRule(), queryResults, sqlStatement, logicSchema.getMetaData().getTable()).merge();
        } catch (final SQLException ex) {
            return new CommandResponsePackets(new ErrPacket(1, ex));
        }
        return packets.get(0);
    }
    
    // TODO :jiaqi use sql packet to refresh meta data
    // TODO refresh table meta data by SQL parse result
    
    /**
     * Refresh table metadata.
     *
     * @param logicTableName logic table name
     * @throws SQLException sql exception be threw by the method
     */
    public void refreshTableMetaData(final String logicTableName) throws SQLException {
        TableMetaDataLoader tableMetaDataLoader = new TableMetaDataLoader(
                logicSchema.getMetaData().getDataSource(), BackendExecutorContext.getInstance().getExecuteEngine(), new ProxyTableMetaDataConnectionManager(
                logicSchema.getBackendDataSource()), GLOBAL_REGISTRY.getShardingProperties().<Integer>getValue(ShardingPropertiesConstant.MAX_CONNECTIONS_SIZE_PER_QUERY));
        logicSchema.getMetaData().getTable().put(logicTableName, tableMetaDataLoader.load(logicTableName, ((ShardingSchema) logicSchema).getShardingRule()));
    }
    
    @Override
    public boolean next() throws SQLException {
        if (null == mergedResult || !mergedResult.next()) {
            channelRelease();
            return false;
        }
        return true;
    }
    
    @Override
    public ResultPacket getResultValue() throws SQLException {
        List<Object> data = new ArrayList<>(columnCount);
        for (int columnIndex = 1; columnIndex <= columnCount; columnIndex++) {
            data.add(mergedResult.getValue(columnIndex, Object.class));
        }
        return new ResultPacket(++currentSequenceId, data, columnCount, Collections.<ColumnType>emptyList());
    }
    
    private void channelRelease() {
    
    }
}
