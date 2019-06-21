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

package org.apache.shardingsphere.core.execute.sql.prepare;

import com.google.common.collect.Lists;
import lombok.RequiredArgsConstructor;
import org.apache.shardingsphere.core.constant.ConnectionMode;
import org.apache.shardingsphere.core.execute.ShardingExecuteGroup;
import org.apache.shardingsphere.core.execute.StatementExecuteUnit;
import org.apache.shardingsphere.core.route.RouteUnit;
import org.apache.shardingsphere.core.route.SQLUnit;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

/**
 * SQL execute prepare template.
 *
 * @author zhaojun
 * @author zhangliang
 * @author panjuan
 * @author maxiaoguang
 */
@RequiredArgsConstructor
public final class SQLExecutePrepareTemplate {
    
    private final int maxConnectionsSizePerQuery;
    
    /**
     * Get execute unit groups.
     * 获取执行分片单元组
     *
     * @param routeUnits route units
     * @param callback SQL execute prepare callback
     * @return statement execute unit groups
     * @throws SQLException SQL exception
     */
    public Collection<ShardingExecuteGroup<StatementExecuteUnit>> getExecuteUnitGroups(final Collection<RouteUnit> routeUnits, final SQLExecutePrepareCallback callback) throws SQLException {
        return getSynchronizedExecuteUnitGroups(routeUnits, callback);
    }
    
    /**
     * 获取同步执行分片单元组
     *
     * @param routeUnits
     * @param callback
     * @return
     * @throws SQLException
     */
    private Collection<ShardingExecuteGroup<StatementExecuteUnit>> getSynchronizedExecuteUnitGroups(
            final Collection<RouteUnit> routeUnits, final SQLExecutePrepareCallback callback) throws SQLException {
        //将所有的执行单元根据数据源进行分组
        Map<String, List<SQLUnit>> sqlUnitGroups = getSQLUnitGroups(routeUnits);
        
        Collection<ShardingExecuteGroup<StatementExecuteUnit>> result = new LinkedList<>();
        
        //计算每个数据源的执行单元分组构造执行组对象，然后汇总到结果集中
        for (Entry<String, List<SQLUnit>> entry : sqlUnitGroups.entrySet()) {
            result.addAll(getSQLExecuteGroups(entry.getKey(), entry.getValue(), callback));
        }
        return result;
    }
    
    /**
     * 将所有的SQL执行语句根据数据源进行分组
     *  key： 数据源
     *  value： SQL执行单元集合
     *
     * @param routeUnits
     * @return
     */
    private Map<String, List<SQLUnit>> getSQLUnitGroups(final Collection<RouteUnit> routeUnits) {
        Map<String, List<SQLUnit>> result = new LinkedHashMap<>(routeUnits.size(), 1);
        for (RouteUnit each : routeUnits) {
            if (!result.containsKey(each.getDataSourceName())) {
                result.put(each.getDataSourceName(), new LinkedList<SQLUnit>());
            }
            result.get(each.getDataSourceName()).add(each.getSqlUnit());
        }
        return result;
    }
    
    /**
     * 获取所有的执行单元组，确定每个执行单元组的连接模式
     *
     * @param dataSourceName
     * @param sqlUnits
     * @param callback
     * @return
     * @throws SQLException
     */
    private List<ShardingExecuteGroup<StatementExecuteUnit>> getSQLExecuteGroups(
            final String dataSourceName, final List<SQLUnit> sqlUnits, final SQLExecutePrepareCallback callback) throws SQLException {
        List<ShardingExecuteGroup<StatementExecuteUnit>> result = new LinkedList<>();
        
        //计算分组的大小，并对sqlUnits执行单元进行分组
        //maxConnectionsSizePerQuery=1，sqlUnits.size()=4，desiredPartitionSize=4.比如一次请求最大连接数为1，要执行的sql条数为4，分组大小为4，则需要进行1个分组(容量为4，分片大小为4)。连接限制
        //maxConnectionsSizePerQuery=3，sqlUnits.size()=4，desiredPartitionSize=2.比如一次请求最大连接数为3，要执行的sql条数为4，分组大小为2，则需要进行2个分组(容量为4，分片大小为2)。连接限制
        //maxConnectionsSizePerQuery=3，sqlUnits.size()=2，desiredPartitionSize=1.比如一次请求最大连接数为3，要执行的sql条数为2，分组大小为1。则需要进行2个分组(容量为2，分片大小为1)。内存限制
        int desiredPartitionSize = Math.max(0 == sqlUnits.size() % maxConnectionsSizePerQuery ? sqlUnits.size() / maxConnectionsSizePerQuery : sqlUnits.size() / maxConnectionsSizePerQuery + 1, 1);
        List<List<SQLUnit>> sqlUnitPartitions = Lists.partition(sqlUnits, desiredPartitionSize);
        
        //若maxConnectionsSizePerQuery小于需要执行的SQL数量，则为连接限制模式，否则为内存限制模式
        ConnectionMode connectionMode = maxConnectionsSizePerQuery < sqlUnits.size() ? ConnectionMode.CONNECTION_STRICTLY : ConnectionMode.MEMORY_STRICTLY;
        
        //获取需要的连接
        List<Connection> connections = callback.getConnections(connectionMode, dataSourceName, sqlUnitPartitions.size());
        int count = 0;
        
        //根据分组设置执行单元组，为每个单元组指定connection
        for (List<SQLUnit> each : sqlUnitPartitions) {
            result.add(getSQLExecuteGroup(connectionMode, connections.get(count++), dataSourceName, each, callback));
        }
        return result;
    }
    
    private ShardingExecuteGroup<StatementExecuteUnit> getSQLExecuteGroup(final ConnectionMode connectionMode, final Connection connection, 
                                                                          final String dataSourceName, final List<SQLUnit> sqlUnitGroup, final SQLExecutePrepareCallback callback) throws SQLException {
        List<StatementExecuteUnit> result = new LinkedList<>();
        for (SQLUnit each : sqlUnitGroup) {
            result.add(callback.createStatementExecuteUnit(connection, new RouteUnit(dataSourceName, each), connectionMode));
        }
        return new ShardingExecuteGroup<>(result);
    }
}


