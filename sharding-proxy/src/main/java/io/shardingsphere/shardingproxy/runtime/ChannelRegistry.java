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

package io.shardingsphere.shardingproxy.runtime;

import io.netty.channel.Channel;
import io.shardingsphere.shardingproxy.backend.netty.result.collector.QueryResultCollector;
import io.shardingsphere.shardingproxy.frontend.mysql.CommandExecutor;
import io.shardingsphere.shardingproxy.transport.mysql.packet.command.CommandPacket;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Channel registry.
 *
 * @author wangkai
 * @author zhangliang
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class ChannelRegistry {
    
    public static final ThreadLocal<Channel> LOCAL_FRONTEND_CHANNEL = new ThreadLocal<>();
    
    public static final ThreadLocal<String> COMMAND_PACKET_ID = new ThreadLocal<>();
    
    public static final Map<String, CommandExecutor> FRONTEND_CHANNEL_COMMAND_EXECUTOR = new ConcurrentHashMap<>();
    
    public static final Map<String, Channel> FRONTEND_CHANNEL = new ConcurrentHashMap<>();
    
    public static final Map<String, CommandPacket> FRONTEND_CHANNEL_COMMAND_PACKET = new ConcurrentHashMap<>();
    
    public static final Map<String, QueryResultCollector> BACKEND_CHANNEL_QUERY_RESULT_COLLECTOR = new ConcurrentHashMap<>();
    
    public static final Map<String, String> BACKEND_CHANNEL_DATABASE_NAME = new ConcurrentHashMap<>();
}
