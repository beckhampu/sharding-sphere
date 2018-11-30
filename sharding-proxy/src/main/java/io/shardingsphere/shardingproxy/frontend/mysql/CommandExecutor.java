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

package io.shardingsphere.shardingproxy.frontend.mysql;

import com.google.common.base.Optional;
import com.google.common.base.Supplier;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.shardingsphere.core.constant.properties.ShardingPropertiesConstant;
import io.shardingsphere.shardingproxy.backend.jdbc.connection.BackendConnection;
import io.shardingsphere.shardingproxy.backend.jdbc.connection.ConnectionStatus;
import io.shardingsphere.shardingproxy.frontend.common.FrontendHandler;
import io.shardingsphere.shardingproxy.runtime.ChannelRegistry;
import io.shardingsphere.shardingproxy.runtime.GlobalRegistry;
import io.shardingsphere.shardingproxy.transport.common.packet.DatabasePacket;
import io.shardingsphere.shardingproxy.transport.mysql.constant.ServerErrorCode;
import io.shardingsphere.shardingproxy.transport.mysql.packet.MySQLPacketPayload;
import io.shardingsphere.shardingproxy.transport.mysql.packet.command.CommandPacket;
import io.shardingsphere.shardingproxy.transport.mysql.packet.command.CommandPacketFactory;
import io.shardingsphere.shardingproxy.transport.mysql.packet.command.CommandResponsePackets;
import io.shardingsphere.shardingproxy.transport.mysql.packet.command.query.QueryCommandPacket;
import io.shardingsphere.shardingproxy.transport.mysql.packet.generic.EofPacket;
import io.shardingsphere.shardingproxy.transport.mysql.packet.generic.ErrPacket;
import io.shardingsphere.shardingproxy.transport.mysql.packet.generic.OKPacket;
import io.shardingsphere.shardingproxy.util.ChannelUtils;
import io.shardingsphere.spi.root.RootInvokeHook;
import io.shardingsphere.spi.root.SPIRootInvokeHook;
import lombok.RequiredArgsConstructor;

import java.sql.SQLException;
import java.util.UUID;

/**
 * Command executor.
 *
 * @author zhangyonglun
 */
@RequiredArgsConstructor
public final class CommandExecutor implements Runnable {
    
    private static final GlobalRegistry GLOBAL_REGISTRY = GlobalRegistry.getInstance();
    
    private static final Supplier<CommandPacket> COMMAND_PACKET_SUPPLIER = new Supplier<CommandPacket>() {
        @Override
        public CommandPacket get() {
            String uuid = ChannelRegistry.COMMAND_PACKET_ID.get();
            return ChannelRegistry.FRONTEND_CHANNEL_COMMAND_PACKET.get(uuid);
        }
    };
    
    private final ChannelHandlerContext context;
    
    private final ByteBuf message;
    
    private final FrontendHandler frontendHandler;
    
    private int currentSequenceId;
    
    private final RootInvokeHook rootInvokeHook = new SPIRootInvokeHook();
    
    @Override
    public void run() {
        rootInvokeHook.start();
        int connectionSize = 0;
        try (MySQLPacketPayload payload = new MySQLPacketPayload(message);
             BackendConnection backendConnection = frontendHandler.getBackendConnection()) {
            waitUntilConnectionReleasedIfNecessary(backendConnection);
            if (GLOBAL_REGISTRY.getShardingProperties().<Boolean>getValue(ShardingPropertiesConstant.PROXY_BACKEND_USE_NIO)) {
                ChannelRegistry.LOCAL_FRONTEND_CHANNEL.set(context.channel());
                ChannelRegistry.FRONTEND_CHANNEL_COMMAND_EXECUTOR.put(ChannelUtils.getLongTextId(context.channel()), this);
                String uuid = UUID.randomUUID().toString();
                ChannelRegistry.COMMAND_PACKET_ID.set(uuid);
                final CommandPacket commandPacket = getCommandPacket(payload, backendConnection, frontendHandler);
                ChannelRegistry.FRONTEND_CHANNEL_COMMAND_PACKET.put(uuid, commandPacket);
                commandPacket.execute();
                return;
            }
            final CommandPacket commandPacket = getCommandPacket(payload, backendConnection, frontendHandler);
            Optional<CommandResponsePackets> responsePackets = commandPacket.execute();
            if (!responsePackets.isPresent()) {
                return;
            }
            writeResult(responsePackets.get(), new Supplier<CommandPacket>() {
                @Override
                public CommandPacket get() {
                    return commandPacket;
                }
            });
            connectionSize = backendConnection.getConnectionSize();
        } catch (final SQLException ex) {
            writeErrPacket(ex);
            // CHECKSTYLE:OFF
        } catch (final Exception ex) {
            // CHECKSTYLE:ON
            context.writeAndFlush(new ErrPacket(1, ServerErrorCode.ER_STD_UNKNOWN_EXCEPTION, ex.getMessage()));
        } finally {
            rootInvokeHook.finish(connectionSize);
        }
    }
    
    /**
     * Output result to client.
     *
     * @param responsePackets the response packets of executing SQL
     */
    public void writeResult(final CommandResponsePackets responsePackets) {
        writeResult(responsePackets, COMMAND_PACKET_SUPPLIER);
    }
    
    /**
     * Output result to client.
     *
     * @param responsePackets the response packets of executing SQL
     * @param supplier the supplier of CommandPacket
     */
    public void writeResult(final CommandResponsePackets responsePackets, final Supplier<CommandPacket> supplier) {
        for (DatabasePacket each : responsePackets.getPackets()) {
            context.channel().writeAndFlush(each);
        }
        CommandPacket commandPacket = supplier.get();
        if (commandPacket instanceof QueryCommandPacket && !(responsePackets.getHeadPacket() instanceof OKPacket) && !(responsePackets.getHeadPacket() instanceof ErrPacket)) {
            try {
                writeMoreResults((QueryCommandPacket) commandPacket, responsePackets.getPackets().size());
            } catch (SQLException ex) {
                writeErrPacket(ex);
            }
        }
    }
    
    /**
     * Output error packet to client.
     *
     * @param ex sql exception
     */
    public void writeErrPacket(final SQLException ex) {
        context.writeAndFlush(new ErrPacket(++currentSequenceId, ex));
    }
    
    private void waitUntilConnectionReleasedIfNecessary(final BackendConnection backendConnection) throws InterruptedException {
        if (ConnectionStatus.TRANSACTION != backendConnection.getStatus() && ConnectionStatus.INIT != backendConnection.getStatus()
                && ConnectionStatus.TERMINATED != backendConnection.getStatus()) {
            while (!backendConnection.compareAndSetStatus(ConnectionStatus.RELEASE, ConnectionStatus.RUNNING)) {
                synchronized (backendConnection.getLock()) {
                    backendConnection.getLock().wait(1000);
                }
            }
        }
    }
    
    private void writeMoreResults(final QueryCommandPacket queryCommandPacket, final int headPacketsCount) throws SQLException {
        if (!context.channel().isActive()) {
            return;
        }
        currentSequenceId = headPacketsCount;
        while (queryCommandPacket.next()) {
            if (!GLOBAL_REGISTRY.getShardingProperties().<Boolean>getValue(ShardingPropertiesConstant.PROXY_BACKEND_USE_NIO)) {
                while (!context.channel().isWritable() && context.channel().isActive()) {
                    synchronized (frontendHandler) {
                        try {
                            frontendHandler.wait();
                        } catch (final InterruptedException ignored) {
                        }
                    }
                }
            }
            DatabasePacket resultValue = queryCommandPacket.getResultValue();
            currentSequenceId = resultValue.getSequenceId();
            context.writeAndFlush(resultValue);
        }
        context.writeAndFlush(new EofPacket(++currentSequenceId));
    }
    
    private CommandPacket getCommandPacket(final MySQLPacketPayload payload, final BackendConnection backendConnection, final FrontendHandler frontendHandler) throws SQLException {
        int sequenceId = payload.readInt1();
        return CommandPacketFactory.newInstance(sequenceId, payload, backendConnection, frontendHandler);
    }
}

