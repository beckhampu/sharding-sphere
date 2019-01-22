/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.shardingsphere.shardingproxy.transport.mysql.packet.command.query.text.query;

import com.google.common.base.Optional;
import lombok.SneakyThrows;
import org.apache.shardingsphere.core.constant.ShardingConstant;
import org.apache.shardingsphere.shardingproxy.backend.BackendHandler;
import org.apache.shardingsphere.shardingproxy.backend.ResultPacket;
import org.apache.shardingsphere.shardingproxy.backend.jdbc.connection.BackendConnection;
import org.apache.shardingsphere.shardingproxy.backend.jdbc.connection.ConnectionStatus;
import org.apache.shardingsphere.shardingproxy.runtime.GlobalRegistry;
import org.apache.shardingsphere.shardingproxy.runtime.schema.ShardingSchema;
import org.apache.shardingsphere.shardingproxy.transport.common.packet.DatabasePacket;
import org.apache.shardingsphere.shardingproxy.transport.mysql.constant.ColumnType;
import org.apache.shardingsphere.shardingproxy.transport.mysql.packet.MySQLPacketPayload;
import org.apache.shardingsphere.shardingproxy.transport.mysql.packet.command.CommandPacketType;
import org.apache.shardingsphere.shardingproxy.transport.mysql.packet.command.CommandResponsePackets;
import org.apache.shardingsphere.shardingproxy.transport.mysql.packet.command.query.FieldCountPacket;
import org.apache.shardingsphere.shardingproxy.transport.mysql.packet.command.query.text.TextResultSetRowPacket;
import org.apache.shardingsphere.shardingproxy.transport.mysql.packet.command.query.text.query.fixture.ShardingTransactionManagerFixture;
import org.apache.shardingsphere.shardingproxy.transport.mysql.packet.generic.OKPacket;
import org.apache.shardingsphere.transaction.core.TransactionOperationType;
import org.apache.shardingsphere.transaction.core.TransactionType;
import org.hamcrest.CoreMatchers;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.lang.reflect.Field;
import java.sql.SQLException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public final class ComQueryPacketTest {
    
    @Mock
    private MySQLPacketPayload payload;
    
    private BackendConnection backendConnection = new BackendConnection(TransactionType.LOCAL);
    
    @Before
    public void setUp() {
        setShardingSchemas();
        backendConnection.setCurrentSchema(ShardingConstant.LOGIC_SCHEMA_NAME);
    }
    
    @After
    public void tearDown() {
        ShardingTransactionManagerFixture.getInvocations().clear();
    }
    
    @SneakyThrows
    private void setShardingSchemas() {
        ShardingSchema shardingSchema = mock(ShardingSchema.class);
        Map<String, ShardingSchema> shardingSchemas = new HashMap<>();
        shardingSchemas.put(ShardingConstant.LOGIC_SCHEMA_NAME, shardingSchema);
        Field field = GlobalRegistry.class.getDeclaredField("logicSchemas");
        field.setAccessible(true);
        field.set(GlobalRegistry.getInstance(), shardingSchemas);
    }
    
    @Test
    public void assertWrite() {
        ComQueryPacket actual = new ComQueryPacket(1, "SELECT id FROM tbl");
        assertThat(actual.getSequenceId(), is(1));
        actual.write(payload);
        verify(payload).writeInt1(CommandPacketType.COM_QUERY.getValue());
        verify(payload).writeStringEOF("SELECT id FROM tbl");
    }
    
    @Test
    public void assertExecuteWithoutTransaction() throws SQLException {
        when(payload.readStringEOF()).thenReturn("SELECT id FROM tbl");
        ComQueryPacket packet = new ComQueryPacket(1, payload, backendConnection);
        FieldCountPacket expectedFieldCountPacket = new FieldCountPacket(1, 1);
        setBackendHandler(packet, expectedFieldCountPacket);
        Optional<CommandResponsePackets> actual = packet.execute();
        assertTrue(actual.isPresent());
        assertThat(actual.get().getPackets().size(), is(1));
        assertThat(actual.get().getPackets().iterator().next(), is((DatabasePacket) expectedFieldCountPacket));
        assertTrue(packet.next());
        assertThat(packet.getResultValue().getSequenceId(), is(2));
        assertThat(((TextResultSetRowPacket) packet.getResultValue()).getData(), is(Collections.<Object>singletonList(99999L)));
        assertFalse(packet.next());
    }
    
    @SneakyThrows
    private void setBackendHandler(final ComQueryPacket packet, final FieldCountPacket expectedFieldCountPacket) {
        BackendHandler backendHandler = mock(BackendHandler.class);
        when(backendHandler.next()).thenReturn(true, false);
        when(backendHandler.getResultValue()).thenReturn(new ResultPacket(1, Collections.<Object>singletonList("id"), 1, Collections.singletonList(ColumnType.MYSQL_TYPE_VARCHAR)));
        when(backendHandler.execute()).thenReturn(new CommandResponsePackets(expectedFieldCountPacket));
        when(backendHandler.next()).thenReturn(true, false);
        when(backendHandler.getResultValue()).thenReturn(new ResultPacket(2, Collections.<Object>singletonList(99999L), 1, Collections.singletonList(ColumnType.MYSQL_TYPE_LONG)));
        Field field = ComQueryPacket.class.getDeclaredField("backendHandler");
        field.setAccessible(true);
        field.set(packet, backendHandler);
    }
    
    @Test
    public void assertExecuteTCLWithLocalTransaction() {
        when(payload.readStringEOF()).thenReturn("COMMIT");
        ComQueryPacket packet = new ComQueryPacket(1, payload, backendConnection);
        backendConnection.getStateHandler().getAndSetStatus(ConnectionStatus.TRANSACTION);
        Optional<CommandResponsePackets> actual = packet.execute();
        assertTrue(actual.isPresent());
        assertOKPacket(actual.get());
    }
    
    @Test
    public void assertExecuteTCLWithXATransaction() {
        backendConnection.setTransactionType(TransactionType.XA);
        when(payload.readStringEOF()).thenReturn("ROLLBACK");
        ComQueryPacket packet = new ComQueryPacket(1, payload, backendConnection);
        backendConnection.getStateHandler().getAndSetStatus(ConnectionStatus.TRANSACTION);
        Optional<CommandResponsePackets> actual = packet.execute();
        assertTrue(actual.isPresent());
        assertOKPacket(actual.get());
        assertTrue(ShardingTransactionManagerFixture.getInvocations().contains(TransactionOperationType.ROLLBACK));
    }
    
    @Test
    public void assertExecuteRollbackWithXATransaction() {
        backendConnection.setTransactionType(TransactionType.XA);
        when(payload.readStringEOF()).thenReturn("COMMIT");
        ComQueryPacket packet = new ComQueryPacket(1, payload, backendConnection);
        backendConnection.getStateHandler().getAndSetStatus(ConnectionStatus.TRANSACTION);
        Optional<CommandResponsePackets> actual = packet.execute();
        assertTrue(actual.isPresent());
        assertOKPacket(actual.get());
        assertTrue(ShardingTransactionManagerFixture.getInvocations().contains(TransactionOperationType.COMMIT));
    }
    
    private void assertOKPacket(final CommandResponsePackets actual) {
        assertThat(actual.getPackets().size(), is(1));
        assertThat((actual.getPackets().iterator().next()).getSequenceId(), is(1));
        assertThat(actual.getPackets().iterator().next(), CoreMatchers.<DatabasePacket>instanceOf(OKPacket.class));
    }
}
