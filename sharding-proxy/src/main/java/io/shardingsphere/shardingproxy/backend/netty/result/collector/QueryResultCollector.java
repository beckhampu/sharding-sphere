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

package io.shardingsphere.shardingproxy.backend.netty.result.collector;

import io.shardingsphere.core.constant.SQLType;
import io.shardingsphere.core.merger.QueryResult;
import io.shardingsphere.core.parsing.parser.sql.SQLStatement;
import io.shardingsphere.shardingproxy.backend.netty.NettyBackendHandler;
import io.shardingsphere.shardingproxy.backend.netty.client.response.mysql.MySQLQueryResult;
import lombok.Getter;
import lombok.Setter;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * The collector of collecting query results.
 *
 * @author wuxu
 */
public final class QueryResultCollector {
    
    @Getter
    private final String longChannelId;
    
    @Getter
    private final SQLStatement sqlStatement;
    
    private final int mergeCount;
    
    @Getter
    private List<QueryResult> responses;
    
    @Getter
    private final boolean isMasterSlaveSchema;
    
    @Getter
    private final NettyBackendHandler nettyBackendHandler;
    
    @Getter
    private final String commandPacketId;
    
    @Setter
    @Getter
    private int currentSequenceId;
    
    @Setter
    @Getter
    private boolean isBackendChannelExhausted;
    
    public QueryResultCollector(final String longChannelId, final SQLStatement sqlStatement, final int resultSize, final boolean isMasterSlaveSchema,
                                final NettyBackendHandler nettyBackendHandler, final String commandPacketId) {
        this.longChannelId = longChannelId;
        this.sqlStatement = sqlStatement;
        mergeCount = resultSize;
        responses = new ArrayList<>(resultSize);
        this.isMasterSlaveSchema = isMasterSlaveSchema;
        this.nettyBackendHandler = nettyBackendHandler;
        this.commandPacketId = commandPacketId;
        this.currentSequenceId = 0;
        this.isBackendChannelExhausted = false;
    }
    
    /**
     * Check if all query results are collected from all databases.
     *
     * @return boolean
     */
    public boolean isDone() {
        if (mergeCount != responses.size()) {
            return false;
        }
        if (sqlStatement.getType() != SQLType.DQL) {
            return true;
        }
        for (Iterator<QueryResult> it = responses.iterator(); it.hasNext();) {
            MySQLQueryResult mySQLQueryResult = (MySQLQueryResult) it.next();
            if (!mySQLQueryResult.isRowFinished() && !mySQLQueryResult.isGenericFinished()) {
                return false;
            }
        }
        return true;
    }
    
    /**
     * Add query result to the list of current collector.
     *
     * @param response query result from a DB
     */
    public void setResponse(final QueryResult response) {
        responses.add(response);
    }
}
