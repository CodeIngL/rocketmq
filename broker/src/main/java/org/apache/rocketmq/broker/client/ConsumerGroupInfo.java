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
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
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

/**
 * 消费组信息
 * 位于内存中
 * 包括在该消费组下，消费端对不同topic的订阅关系。
 *
 * 一个消费组，维护了其下的相关信息，
 * 可以有多个topic和对应订阅信息
 * 多个客户端channel和客户端的信息，
 * 该消费组支持的消费方式和类型
 *
 */
public class ConsumerGroupInfo {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.BROKER_LOGGER_NAME);
    //消费组名
    private final String groupName;
    //topic和topic对应的相关订阅信息
    private final ConcurrentMap<String/* Topic */, SubscriptionData> subscriptionTable = new ConcurrentHashMap<String, SubscriptionData>();
    //通道和对应的网络客户端信息，维护了Channel和Channel对应的客户端信息的映射关系
    private final ConcurrentMap<Channel, ClientChannelInfo> channelInfoTable = new ConcurrentHashMap<Channel, ClientChannelInfo>(16);
    //消息消费方式类型
    private volatile ConsumeType consumeType;
    //消息消费类型
    private volatile MessageModel messageModel;
    //消息
    private volatile ConsumeFromWhere consumeFromWhere;
    //最后更新时间
    private volatile long lastUpdateTimestamp = System.currentTimeMillis();

    public ConsumerGroupInfo(String groupName, ConsumeType consumeType, MessageModel messageModel, ConsumeFromWhere consumeFromWhere) {
        this.groupName = groupName;
        this.consumeType = consumeType;
        this.messageModel = messageModel;
        this.consumeFromWhere = consumeFromWhere;
    }

    /**
     * 通过客户端id找到对应的client的ChannelInfo的相关的信息
     * @param clientId
     * @return
     */
    public ClientChannelInfo findChannel(final String clientId) {
        Iterator<Entry<Channel, ClientChannelInfo>> it = this.channelInfoTable.entrySet().iterator();
        while (it.hasNext()) {
            Entry<Channel, ClientChannelInfo> next = it.next();
            if (next.getValue().getClientId().equals(clientId)) {
                return next.getValue();
            }
        }
        return null;
    }

    public ConcurrentMap<String, SubscriptionData> getSubscriptionTable() {
        return subscriptionTable;
    }

    public ConcurrentMap<Channel, ClientChannelInfo> getChannelInfoTable() {
        return channelInfoTable;
    }

    public List<Channel> getAllChannel() {
        List<Channel> result = new ArrayList<>();

        result.addAll(this.channelInfoTable.keySet());

        return result;
    }

    /**
     * 获得所有的客户端的id，从channelInfo中获取，遍历所有信息，
     * 我们将这些同一个Group下的所有的clientId进行返回
     * @return
     */
    public List<String> getAllClientId() {
        List<String> result = new ArrayList<>();

        Iterator<Entry<Channel, ClientChannelInfo>> it = this.channelInfoTable.entrySet().iterator();

        while (it.hasNext()) {
            Entry<Channel, ClientChannelInfo> entry = it.next();
            ClientChannelInfo clientChannelInfo = entry.getValue();
            result.add(clientChannelInfo.getClientId());
        }

        return result;
    }

    public void unregisterChannel(final ClientChannelInfo clientChannelInfo) {
        ClientChannelInfo old = this.channelInfoTable.remove(clientChannelInfo.getChannel());
        if (old != null) {
            log.info("unregister a consumer[{}] from consumerGroupInfo {}", this.groupName, old.toString());
        }
    }

    public boolean doChannelCloseEvent(final String remoteAddr, final Channel channel) {
        final ClientChannelInfo info = this.channelInfoTable.remove(channel);
        if (info != null) {
            log.warn("NETTY EVENT: remove not active channel[{}] from ConsumerGroupInfo groupChannelTable, consumer group: {}", info.toString(), groupName);
            return true;
        }

        return false;
    }

    /**
     * 更新channel
     * @param infoNew
     * @param consumeType
     * @param messageModel
     * @param consumeFromWhere
     * @return 是否进行更新
     */
    public boolean updateChannel(final ClientChannelInfo infoNew, ConsumeType consumeType, MessageModel messageModel, ConsumeFromWhere consumeFromWhere) {
        boolean updated = false;
        this.consumeType = consumeType;
        this.messageModel = messageModel;
        this.consumeFromWhere = consumeFromWhere;

        ClientChannelInfo infoOld = this.channelInfoTable.get(infoNew.getChannel());
        if (null == infoOld) {
            ClientChannelInfo prev = this.channelInfoTable.put(infoNew.getChannel(), infoNew);
            if (null == prev) {
                log.info("new consumer connected, group: {} {} {} channel: {}", this.groupName, consumeType, messageModel, infoNew.toString());
                updated = true;
            }

            infoOld = infoNew;
        } else {
            //两个的id不同，bug，但是我们还是会将他放置进去
            if (!infoOld.getClientId().equals(infoNew.getClientId())) {
                log.error("[BUG] consumer channel exist in broker, but clientId not equal. GROUP: {} OLD: {} NEW: {} ", this.groupName, infoOld.toString(), infoNew.toString());
                this.channelInfoTable.put(infoNew.getChannel(), infoNew);
            }
        }

        this.lastUpdateTimestamp = System.currentTimeMillis();
        infoOld.setLastUpdateTimestamp(this.lastUpdateTimestamp);

        //返回是否新值
        return updated;
    }

    /**
     * 更新订阅的数据
     * @param subList
     * @return 是否进行了更新
     */
    public boolean updateSubscription(final Set<SubscriptionData> subList) {
        boolean updated = false;

        //遍历由客户端传递上来的订阅信息
        for (SubscriptionData sub : subList) {
            //获得topic订阅对应的订阅信息
            SubscriptionData old = this.subscriptionTable.get(sub.getTopic());
            if (old == null) {
                SubscriptionData prev = this.subscriptionTable.putIfAbsent(sub.getTopic(), sub);
                if (null == prev) {
                    updated = true;
                    log.info("subscription changed, add new topic, group: {} {}", this.groupName, sub.toString());
                }
            } else if (sub.getSubVersion() > old.getSubVersion()) {
                //版本发生了变化
                if (this.consumeType == ConsumeType.CONSUME_PASSIVELY) {
                    log.info("subscription changed, group: {} OLD: {} NEW: {}", this.groupName, old.toString(), sub.toString());
                }
                //更新相关的信息
                this.subscriptionTable.put(sub.getTopic(), sub);
            }
        }

        //处理每一个topic相关的信息
        Iterator<Entry<String, SubscriptionData>> it = this.subscriptionTable.entrySet().iterator();
        while (it.hasNext()) {
            Entry<String, SubscriptionData> next = it.next();
            String oldTopic = next.getKey();

            boolean exist = false;
            for (SubscriptionData sub : subList) {
                if (sub.getTopic().equals(oldTopic)) {
                    //有相同
                    exist = true;
                    break;
                }
            }

            if (!exist) {
                //不存在，我们发现是变更

                log.warn("subscription changed, group: {} remove topic {} {}", this.groupName, oldTopic, next.getValue().toString());
                //删除这个不存在的topic，这是由客户端发生的
                //同一消费组下，不同的topic订阅就会发生相关的问题，会删除两者相关的差集部分
                it.remove();
                updated = true;
            }
        }

        this.lastUpdateTimestamp = System.currentTimeMillis();

        //是否发生了变更
        return updated;
    }

    //-----get and set -----//

    public Set<String> getSubscribeTopics() {
        return subscriptionTable.keySet();
    }

    public SubscriptionData findSubscriptionData(final String topic) {
        return this.subscriptionTable.get(topic);
    }

    public ConsumeType getConsumeType() {
        return consumeType;
    }

    public void setConsumeType(ConsumeType consumeType) {
        this.consumeType = consumeType;
    }

    public MessageModel getMessageModel() {
        return messageModel;
    }

    public void setMessageModel(MessageModel messageModel) {
        this.messageModel = messageModel;
    }

    public String getGroupName() {
        return groupName;
    }

    public long getLastUpdateTimestamp() {
        return lastUpdateTimestamp;
    }

    public void setLastUpdateTimestamp(long lastUpdateTimestamp) {
        this.lastUpdateTimestamp = lastUpdateTimestamp;
    }

    public ConsumeFromWhere getConsumeFromWhere() {
        return consumeFromWhere;
    }

    public void setConsumeFromWhere(ConsumeFromWhere consumeFromWhere) {
        this.consumeFromWhere = consumeFromWhere;
    }
}
