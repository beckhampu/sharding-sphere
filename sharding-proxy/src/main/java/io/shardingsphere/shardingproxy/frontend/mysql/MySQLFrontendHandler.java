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
import com.google.common.base.Strings;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.EventLoopGroup;
import io.shardingsphere.shardingproxy.backend.jdbc.connection.BackendConnection;
import io.shardingsphere.shardingproxy.frontend.common.FrontendHandler;
import io.shardingsphere.shardingproxy.frontend.common.executor.ExecutorGroup;
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
import io.shardingsphere.shardingproxy.transport.mysql.packet.handshake.AuthenticationHandler;
import io.shardingsphere.shardingproxy.transport.mysql.packet.handshake.ConnectionIdGenerator;
import io.shardingsphere.shardingproxy.transport.mysql.packet.handshake.HandshakePacket;
import io.shardingsphere.shardingproxy.transport.mysql.packet.handshake.HandshakeResponse41Packet;
import io.shardingsphere.spi.root.RootInvokeHook;
import io.shardingsphere.spi.root.SPIRootInvokeHook;
import lombok.RequiredArgsConstructor;

import java.sql.SQLException;

/**
 * MySQL frontend handler.
 *
 * @author zhangliang
 * @author panjuan
 * @author wangkai
 */
@RequiredArgsConstructor
public final class MySQLFrontendHandler extends FrontendHandler {
    
    private final EventLoopGroup eventLoopGroup;
    
    private final AuthenticationHandler authenticationHandler = new AuthenticationHandler();
    
    private final RootInvokeHook rootInvokeHook = new SPIRootInvokeHook();
    
    @Override
    protected void handshake(final ChannelHandlerContext context) {
        int connectionId = ConnectionIdGenerator.getInstance().nextId();
        ChannelRegistry.getInstance().putConnectionId(context.channel().id().asShortText(), connectionId);
        context.writeAndFlush(new HandshakePacket(connectionId, authenticationHandler.getAuthPluginData()));
    }
    
    @Override
    protected void auth(final ChannelHandlerContext context, final ByteBuf message) {
        try (MySQLPacketPayload payload = new MySQLPacketPayload(message)) {
            HandshakeResponse41Packet response41 = new HandshakeResponse41Packet(payload);
            if (authenticationHandler.login(response41.getUsername(), response41.getAuthResponse())) {
                if (!Strings.isNullOrEmpty(response41.getDatabase()) && !GlobalRegistry.getInstance().schemaExists(response41.getDatabase())) {
                    context.writeAndFlush(new ErrPacket(response41.getSequenceId() + 1, ServerErrorCode.ER_BAD_DB_ERROR, response41.getDatabase()));
                    return;
                }
                setCurrentSchema(response41.getDatabase());
                context.writeAndFlush(new OKPacket(response41.getSequenceId() + 1));
            } else {
                // TODO localhost should replace to real ip address
                context.writeAndFlush(new ErrPacket(response41.getSequenceId() + 1,
                        ServerErrorCode.ER_ACCESS_DENIED_ERROR, response41.getUsername(), "localhost", 0 == response41.getAuthResponse().length ? "NO" : "YES"));
            }
        }
    }
    
    @Override
    protected void executeCommand(final ChannelHandlerContext context, final ByteBuf message) {
        new ExecutorGroup(eventLoopGroup, context.channel().id()).getExecutorService().execute(new CommandExecutor(context, message));
    }
    
    @Override
    public void channelWritabilityChanged(final ChannelHandlerContext context) {
        if (context.channel().isWritable()) {
            synchronized (this) {
                this.notifyAll();
            }
        }
    }
    
    @RequiredArgsConstructor
    class CommandExecutor implements Runnable {
        
        private final ChannelHandlerContext context;
        
        private final ByteBuf message;
        
        private int currentSequenceId;
        
        @Override
        public void run() {
            rootInvokeHook.start();
            int connectionSize = 0;
            try (MySQLPacketPayload payload = new MySQLPacketPayload(message);
                 BackendConnection backendConnection = new BackendConnection()) {
                setBackendConnection(backendConnection);
                CommandPacket commandPacket = getCommandPacket(payload, backendConnection, MySQLFrontendHandler.this);
                Optional<CommandResponsePackets> responsePackets = commandPacket.execute();
                if (!responsePackets.isPresent()) {
                    return;
                }
                for (DatabasePacket each : responsePackets.get().getPackets()) {
                    context.writeAndFlush(each);
                }
                if (commandPacket instanceof QueryCommandPacket && !(responsePackets.get().getHeadPacket() instanceof OKPacket) && !(responsePackets.get().getHeadPacket() instanceof ErrPacket)) {
                    writeMoreResults((QueryCommandPacket) commandPacket, responsePackets.get().getPackets().size());
                }
                connectionSize = backendConnection.getConnectionSize();
            } catch (final SQLException ex) {
                context.writeAndFlush(new ErrPacket(++currentSequenceId, ex));
                // CHECKSTYLE:OFF
            } catch (final Exception ex) {
                // CHECKSTYLE:ON
                context.writeAndFlush(new ErrPacket(1, ServerErrorCode.ER_STD_UNKNOWN_EXCEPTION, ex.getMessage()));
            } finally {
                rootInvokeHook.finish(connectionSize);
            }
        }
        
        private CommandPacket getCommandPacket(final MySQLPacketPayload payload, final BackendConnection backendConnection, final FrontendHandler frontendHandler) throws SQLException {
            int sequenceId = payload.readInt1();
            int connectionId = ChannelRegistry.getInstance().getConnectionId(context.channel().id().asShortText());
            return CommandPacketFactory.newInstance(sequenceId, connectionId, payload, backendConnection, frontendHandler);
        }
        
        private void writeMoreResults(final QueryCommandPacket queryCommandPacket, final int headPacketsCount) throws SQLException {
            if (!context.channel().isActive()) {
                return;
            }
            currentSequenceId = headPacketsCount;
            while (queryCommandPacket.next()) {
                while (!context.channel().isWritable() && context.channel().isActive()) {
                    synchronized (MySQLFrontendHandler.this) {
                        try {
                            MySQLFrontendHandler.this.wait();
                        } catch (final InterruptedException ignored) {
                        }
                    }
                }
                DatabasePacket resultValue = queryCommandPacket.getResultValue();
                currentSequenceId = resultValue.getSequenceId();
                context.writeAndFlush(resultValue);
            }
            context.writeAndFlush(new EofPacket(++currentSequenceId));
        }
    }
}
