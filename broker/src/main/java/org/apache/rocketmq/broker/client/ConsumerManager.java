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
package org.apache.rocketmq.broker.client;

import io.netty.channel.Channel;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.common.protocol.heartbeat.ConsumeType;
import org.apache.rocketmq.common.protocol.heartbeat.MessageModel;
import org.apache.rocketmq.common.protocol.heartbeat.SubscriptionData;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.common.RemotingUtil;

/**
 * 位于broker中关于consumer的管理注册表
 */
public class ConsumerManager {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.BROKER_LOGGER_NAME);
    private static final long CHANNEL_EXPIRED_TIMEOUT = 1000 * 120;
    private final ConcurrentMap<String/* Group */, ConsumerGroupInfo> consumerTable = new ConcurrentHashMap<String, ConsumerGroupInfo>(1024);
    //监听器，一旦发生变更，需要通知相关事件
    private final ConsumerIdsChangeListener consumerIdsChangeListener;

    public ConsumerManager(final ConsumerIdsChangeListener consumerIdsChangeListener) {
        this.consumerIdsChangeListener = consumerIdsChangeListener;
    }

    public ClientChannelInfo findChannel(final String group, final String clientId) {
        ConsumerGroupInfo consumerGroupInfo = this.consumerTable.get(group);
        if (consumerGroupInfo != null) {
            return consumerGroupInfo.findChannel(clientId);
        }
        return null;
    }

    public SubscriptionData findSubscriptionData(final String group, final String topic) {
        ConsumerGroupInfo consumerGroupInfo = this.getConsumerGroupInfo(group);
        if (consumerGroupInfo != null) {
            return consumerGroupInfo.findSubscriptionData(topic);
        }

        return null;
    }

    public ConsumerGroupInfo getConsumerGroupInfo(final String group) {
        return this.consumerTable.get(group);
    }

    public int findSubscriptionDataCount(final String group) {
        ConsumerGroupInfo consumerGroupInfo = this.getConsumerGroupInfo(group);
        if (consumerGroupInfo != null) {
            return consumerGroupInfo.getSubscriptionTable().size();
        }

        return 0;
    }

    /**
     * 消费端发生关闭，我们需要进行通知客户端，发生了变更，先尝试发生注销变更，再尝试发生变更变更
     */
    public void doChannelCloseEvent(final String remoteAddr, final Channel channel) {
        Iterator<Entry<String, ConsumerGroupInfo>> it = this.consumerTable.entrySet().iterator();
        while (it.hasNext()) {
            Entry<String, ConsumerGroupInfo> next = it.next();
            ConsumerGroupInfo info = next.getValue();
            boolean removed = info.doChannelCloseEvent(remoteAddr, channel);
            if (removed) {
                if (info.getChannelInfoTable().isEmpty()) {
                    ConsumerGroupInfo remove = this.consumerTable.remove(next.getKey());
                    if (remove != null) {
                        log.info("unregister consumer ok, no any connection, and remove consumer group, {}", next.getKey());
                        this.consumerIdsChangeListener.handle(ConsumerGroupEvent.UNREGISTER, next.getKey());
                    }
                }

                this.consumerIdsChangeListener.handle(ConsumerGroupEvent.CHANGE, next.getKey(), info.getAllChannel());
            }
        }
    }

    /**
     * 注册consumer，我们需要通知相关的客户端感知到发生了变化，尝试通知变更，进行注册事件的处理
     * @param group 来自客户端
     * @param clientChannelInfo 来自计算
     * @param consumeType 来自客户端
     * @param messageModel 来自客户端
     * @param consumeFromWhere 来自客户端
     * @param subList 来自客户端
     * @param isNotifyConsumerIdsChangedEnable 来自计算
     * @return 返回是否变更
     */
    public boolean registerConsumer(final String group, final ClientChannelInfo clientChannelInfo, ConsumeType consumeType, MessageModel messageModel, ConsumeFromWhere consumeFromWhere,
        final Set<SubscriptionData> subList, boolean isNotifyConsumerIdsChangedEnable) {

        //多个消费端，如果消费组相同，则broker侧是认为是一份关于group数据，而不是客户端的数据
        // 获得映射关系
        ConsumerGroupInfo consumerGroupInfo = this.consumerTable.get(group);
        if (null == consumerGroupInfo) {
            //不存在我们构建一个映射关系
            ConsumerGroupInfo tmp = new ConsumerGroupInfo(group, consumeType, messageModel, consumeFromWhere);
            //获得一个结果
            ConsumerGroupInfo prev = this.consumerTable.putIfAbsent(group, tmp);
            consumerGroupInfo = prev != null ? prev : tmp;
        }

        //更新channel，也就是尝试注册，这里多个节点不会有任何的异议
        boolean r1 = consumerGroupInfo.updateChannel(clientChannelInfo, consumeType, messageModel, consumeFromWhere);
        //更新订阅信息，也就是尝试注册，这里更新户根据订阅的数据不一致进行处理，这里的维度是topic的维度，因此group一致，当topic一致时
        //需要保证订阅的的信息一致
        boolean r2 = consumerGroupInfo.updateSubscription(subList);

        if (r1 || r2) {
            if (isNotifyConsumerIdsChangedEnable) {
                //处理变更结果
                this.consumerIdsChangeListener.handle(ConsumerGroupEvent.CHANGE, group, consumerGroupInfo.getAllChannel());
            }
        }


        //处理注册结果
        this.consumerIdsChangeListener.handle(ConsumerGroupEvent.REGISTER, group, subList);

        return r1 || r2;
    }

    /**
     * 从内存中注销consumer，我们需要通知相应的客户端来感知这个结果，因为发生了相关的变更
     * @param group
     * @param clientChannelInfo
     * @param isNotifyConsumerIdsChangedEnable
     */
    public void unregisterConsumer(final String group, final ClientChannelInfo clientChannelInfo, boolean isNotifyConsumerIdsChangedEnable) {
        ConsumerGroupInfo consumerGroupInfo = this.consumerTable.get(group);
        if (consumerGroupInfo == null){
            return;
        }
        consumerGroupInfo.unregisterChannel(clientChannelInfo);
        if (consumerGroupInfo.getChannelInfoTable().isEmpty()) {
            ConsumerGroupInfo remove = this.consumerTable.remove(group);
            if (remove != null) {
                log.info("unregister consumer ok, no any connection, and remove consumer group, {}", group);
                this.consumerIdsChangeListener.handle(ConsumerGroupEvent.UNREGISTER, group);
            }
        }
        if (isNotifyConsumerIdsChangedEnable) {
            this.consumerIdsChangeListener.handle(ConsumerGroupEvent.CHANGE, group, consumerGroupInfo.getAllChannel());
        }
    }

    public void scanNotActiveChannel() {
        Iterator<Entry<String, ConsumerGroupInfo>> it = this.consumerTable.entrySet().iterator();
        while (it.hasNext()) {
            Entry<String, ConsumerGroupInfo> next = it.next();
            String group = next.getKey();
            ConsumerGroupInfo consumerGroupInfo = next.getValue();
            ConcurrentMap<Channel, ClientChannelInfo> channelInfoTable =
                consumerGroupInfo.getChannelInfoTable();

            Iterator<Entry<Channel, ClientChannelInfo>> itChannel = channelInfoTable.entrySet().iterator();
            while (itChannel.hasNext()) {
                Entry<Channel, ClientChannelInfo> nextChannel = itChannel.next();
                ClientChannelInfo clientChannelInfo = nextChannel.getValue();
                long diff = System.currentTimeMillis() - clientChannelInfo.getLastUpdateTimestamp();
                if (diff > CHANNEL_EXPIRED_TIMEOUT) {
                    log.warn(
                        "SCAN: remove expired channel from ConsumerManager consumerTable. channel={}, consumerGroup={}",
                        RemotingHelper.parseChannelRemoteAddr(clientChannelInfo.getChannel()), group);
                    RemotingUtil.closeChannel(clientChannelInfo.getChannel());
                    itChannel.remove();
                }
            }

            if (channelInfoTable.isEmpty()) {
                log.warn(
                    "SCAN: remove expired channel from ConsumerManager consumerTable, all clear, consumerGroup={}",
                    group);
                it.remove();
            }
        }
    }

    public HashSet<String> queryTopicConsumeByWho(final String topic) {
        HashSet<String> groups = new HashSet<>();
        Iterator<Entry<String, ConsumerGroupInfo>> it = this.consumerTable.entrySet().iterator();
        while (it.hasNext()) {
            Entry<String, ConsumerGroupInfo> entry = it.next();
            ConcurrentMap<String, SubscriptionData> subscriptionTable =
                entry.getValue().getSubscriptionTable();
            if (subscriptionTable.containsKey(topic)) {
                groups.add(entry.getKey());
            }
        }
        return groups;
    }
}
