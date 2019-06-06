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

package org.apache.rocketmq.broker.plugin;

import java.io.IOException;
import java.lang.reflect.Constructor;
import org.apache.rocketmq.store.MessageStore;

/**
 * uitl，用于构建对应的消息存储
 */
public final class MessageStoreFactory {
    /**
     * 提供对消息存储服务的wrapper操作，实现拦截处理，
     * 插件必须存在有两个参数的构造函数，其中之一是MessageStorePluginContext，
     * 另外一个则是MessageStore
     * @param context
     * @param messageStore
     * @return
     * @throws IOException
     */
    public final static MessageStore build(MessageStorePluginContext context, MessageStore messageStore) throws IOException {
        //存储插件，获得插件配置，进行构建插件，来wrapper存储，，号切割插件
        String plugin = context.getBrokerConfig().getMessageStorePlugIn();
        if (plugin != null && plugin.trim().length() != 0) {
            String[] pluginClasses = plugin.split(",");
            for (int i = pluginClasses.length - 1; i >= 0; --i) {
                String pluginClass = pluginClasses[i];
                try {
                    @SuppressWarnings("unchecked")
                    Class<AbstractPluginMessageStore> clazz = (Class<AbstractPluginMessageStore>) Class.forName(pluginClass);
                    Constructor<AbstractPluginMessageStore> construct = clazz.getConstructor(MessageStorePluginContext.class, MessageStore.class);
                    messageStore = construct.newInstance(context, messageStore);
                } catch (Throwable e) {
                    throw new RuntimeException(String.format(
                        "Initialize plugin's class %s not found!", pluginClass), e);
                }
            }
        }
        return messageStore;
    }
}
