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

package io.shardingsphere.shardingproxy.backend;

import io.shardingsphere.core.constant.DatabaseType;
import io.shardingsphere.core.constant.properties.ShardingPropertiesConstant;
import io.shardingsphere.shardingproxy.backend.jdbc.connection.BackendConnection;
import io.shardingsphere.shardingproxy.runtime.GlobalRegistry;
import io.shardingsphere.shardingproxy.transport.common.packet.DatabasePacket;
import io.shardingsphere.shardingproxy.transport.mysql.packet.command.CommandResponsePackets;
import io.shardingsphere.shardingproxy.transport.mysql.packet.generic.OKPacket;
import lombok.RequiredArgsConstructor;

import java.util.Collection;
import java.util.LinkedList;
import java.util.List;

/**
 * Backend handler for schema broadcast.
 *
 * @author chenqingyang
 */
@RequiredArgsConstructor
public final class SchemaBroadcastBackendHandler implements BackendHandler {
    
    private static final GlobalRegistry GLOBAL_REGISTRY = GlobalRegistry.getInstance();
    
    private final int sequenceId;
    
    private final String sql;
    
    private final BackendConnection backendConnection;
    
    private final DatabaseType databaseType;
    
    @Override
    public CommandResponsePackets execute() {
        List<DatabasePacket> packets = new LinkedList<>();
        for (String schema : GlobalRegistry.getInstance().getSchemaNames()) {
            BackendHandler backendHandler = BackendHandlerFactory.newTextProtocolInstance(sequenceId, sql, backendConnection, databaseType, schema);
            CommandResponsePackets commandResponsePackets = backendHandler.execute();
            if (!GLOBAL_REGISTRY.getShardingProperties().<Boolean>getValue(ShardingPropertiesConstant.PROXY_BACKEND_USE_NIO)) {
                packets.addAll(commandResponsePackets.getPackets());
            }
        }
        return merge(packets);
    }
    
    @Override
    public boolean next() {
        return false;
    }
    
    @Override
    public ResultPacket getResultValue() {
        return null;
    }
    
    private CommandResponsePackets merge(final Collection<DatabasePacket> packets) {
        int affectedRows = 0;
        long lastInsertId = 0;
        for (DatabasePacket each : packets) {
            if (each instanceof OKPacket) {
                affectedRows += ((OKPacket) each).getAffectedRows();
                if (((OKPacket) each).getLastInsertId() > lastInsertId) {
                    lastInsertId = ((OKPacket) each).getLastInsertId();
                }
            }
        }
        return new CommandResponsePackets(new OKPacket(1, affectedRows, lastInsertId));
    }
}
