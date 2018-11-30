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

package io.shardingsphere.shardingproxy.util;

import io.netty.channel.Channel;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;

/**
 * Util for netty channel.
 *
 * @author wuxu
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class ChannelUtils {
    
    /**
     * Returns the long yet globally unique string representation of the channel.
     *
     * @param channel a instance of channel in netty
     * @return long text globally unique id of channel
     */
    public static String getLongTextId(final Channel channel) {
        return channel.id().asLongText();
    }
}
