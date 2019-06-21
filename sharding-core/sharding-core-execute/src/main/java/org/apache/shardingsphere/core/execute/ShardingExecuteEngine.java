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

package org.apache.shardingsphere.core.execute;

import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import org.apache.shardingsphere.core.exception.ShardingException;

import java.sql.SQLException;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;

/**
 * Sharding execute engine.
 *
 * @author zhangliang
 */
public final class ShardingExecuteEngine implements AutoCloseable {
    
    /**
     * 并发执行服务ExecutorService
     */
    private final ShardingExecutorService shardingExecutorService;
    
    private ListeningExecutorService executorService;
    
    public ShardingExecuteEngine(final int executorSize) {
        shardingExecutorService = new ShardingExecutorService(executorSize);
        executorService = shardingExecutorService.getExecutorService();
    }
    
    /**
     * Execute for group.
     *
     * @param inputGroups input groups
     * @param callback sharding execute callback
     * @param <I> type of input value
     * @param <O> type of return value
     * @return execute result
     * @throws SQLException throw if execute failure
     */
    public <I, O> List<O> groupExecute(final Collection<ShardingExecuteGroup<I>> inputGroups, final ShardingGroupExecuteCallback<I, O> callback) throws SQLException {
        return groupExecute(inputGroups, null, callback, false);
    }
    
    /**
     * Execute for group.
     *
     * @param inputGroups input groups
     * @param firstCallback first sharding execute callback
     * @param callback sharding execute callback
     * @param serial whether using multi thread execute or not
     * @param <I> type of input value
     * @param <O> type of return value
     * @return execute result
     * @throws SQLException throw if execute failure
     */
    public <I, O> List<O> groupExecute(
        final Collection<ShardingExecuteGroup<I>> inputGroups, final ShardingGroupExecuteCallback<I, O> firstCallback, final ShardingGroupExecuteCallback<I, O> callback, final boolean serial)
        throws SQLException {
        // 若执行执行分片组为空，这直接返回
        if (inputGroups.isEmpty()) {
            return Collections.emptyList();
        }
        // 根据是否可串行执行标志，调用相对应的方法
        return serial ? serialExecute(inputGroups, firstCallback, callback) : parallelExecute(inputGroups, firstCallback, callback);
    }
    
    /**
     * 串行执行方法
     *
     * @param inputGroups 执行分片组
     * @param firstCallback 第一个执行后的回调
     * @param callback 全部执行后的回调
     * @param <I>
     * @param <O>
     * @return
     * @throws SQLException
     */
    private <I, O> List<O> serialExecute(final Collection<ShardingExecuteGroup<I>> inputGroups, final ShardingGroupExecuteCallback<I, O> firstCallback,
                                         final ShardingGroupExecuteCallback<I, O> callback) throws SQLException {
        List<O> result = new LinkedList<>();
        //首先获取第一个执行分片组，进行同步执行
        Iterator<ShardingExecuteGroup<I>> inputGroupsIterator = inputGroups.iterator();
        ShardingExecuteGroup<I> firstInputs = inputGroupsIterator.next();
        result.addAll(syncGroupExecute(firstInputs, null == firstCallback ? callback : firstCallback));
        
        //然后剩下分片组串行执行
        for (ShardingExecuteGroup<I> each : Lists.newArrayList(inputGroupsIterator)) {
            result.addAll(syncGroupExecute(each, callback));
        }
        return result;
    }
    
    /**
     * 并行执行方法
     *
     * @param inputGroups
     * @param firstCallback
     * @param callback
     * @param <I>
     * @param <O>
     * @return
     * @throws SQLException
     */
    private <I, O> List<O> parallelExecute(final Collection<ShardingExecuteGroup<I>> inputGroups, final ShardingGroupExecuteCallback<I, O> firstCallback,
                                           final ShardingGroupExecuteCallback<I, O> callback) throws SQLException {
        //首先获取第一个执行分片组，进行同步执行
        Iterator<ShardingExecuteGroup<I>> inputGroupsIterator = inputGroups.iterator();
        ShardingExecuteGroup<I> firstInputs = inputGroupsIterator.next();
        
        //剩下分片组并行执行
        Collection<ListenableFuture<Collection<O>>> restResultFutures = asyncGroupExecute(Lists.newArrayList(inputGroupsIterator), callback);
       
        //合并所有执行结果
        return getGroupResults(syncGroupExecute(firstInputs, null == firstCallback ? callback : firstCallback), restResultFutures);
    }
    
    private <I, O> Collection<ListenableFuture<Collection<O>>> asyncGroupExecute(final List<ShardingExecuteGroup<I>> inputGroups, final ShardingGroupExecuteCallback<I, O> callback) {
        Collection<ListenableFuture<Collection<O>>> result = new LinkedList<>();
        for (ShardingExecuteGroup<I> each : inputGroups) {
            result.add(asyncGroupExecute(each, callback));
        }
        return result;
    }
    
    private <I, O> ListenableFuture<Collection<O>> asyncGroupExecute(final ShardingExecuteGroup<I> inputGroup, final ShardingGroupExecuteCallback<I, O> callback) {
        final Map<String, Object> dataMap = ShardingExecuteDataMap.getDataMap();
        return executorService.submit(new Callable<Collection<O>>() {
            
            @Override
            public Collection<O> call() throws SQLException {
                return callback.execute(inputGroup.getInputs(), false, dataMap);
            }
        });
    }
    
    private <I, O> Collection<O> syncGroupExecute(final ShardingExecuteGroup<I> executeGroup, final ShardingGroupExecuteCallback<I, O> callback) throws SQLException {
        return callback.execute(executeGroup.getInputs(), true, ShardingExecuteDataMap.getDataMap());
    }
    
    private <O> List<O> getGroupResults(final Collection<O> firstResults, final Collection<ListenableFuture<Collection<O>>> restFutures) throws SQLException {
        List<O> result = new LinkedList<>();
        result.addAll(firstResults);
        for (ListenableFuture<Collection<O>> each : restFutures) {
            try {
                result.addAll(each.get());
            } catch (final InterruptedException | ExecutionException ex) {
                return throwException(ex);
            }
        }
        return result;
    }
    
    private <O> List<O> throwException(final Exception exception) throws SQLException {
        if (exception.getCause() instanceof SQLException) {
            throw (SQLException) exception.getCause();
        }
        throw new ShardingException(exception);
    }
    
    @Override
    public void close() {
        shardingExecutorService.close();
    }
}
