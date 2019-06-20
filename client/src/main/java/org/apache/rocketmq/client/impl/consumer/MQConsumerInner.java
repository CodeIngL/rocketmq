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
package org.apache.rocketmq.client.impl.consumer;

import java.util.Set;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.protocol.body.ConsumerRunningInfo;
import org.apache.rocketmq.common.protocol.heartbeat.ConsumeType;
import org.apache.rocketmq.common.protocol.heartbeat.MessageModel;
import org.apache.rocketmq.common.protocol.heartbeat.SubscriptionData;

/**
 * Consumer inner interface
 */
public interface MQConsumerInner {

    /**
     * 获得消费组名
     * @return
     */
    String groupName();

    /**
     * 消费模式
     * @return
     */
    MessageModel messageModel();

    /**
     * 消费拉取模式
     * @return
     */
    ConsumeType consumeType();

    /**
     * 从哪里进行消费
     * @return
     */
    ConsumeFromWhere consumeFromWhere();

    /**
     * 订阅的数据
     * @return
     */
    Set<SubscriptionData> subscriptions();

    /**
     * 负载均衡
     */
    void doRebalance();

    /**
     * 持久化相关偏移量
     */
    void persistConsumerOffset();

    /**
     * 更新相关的订阅关系
     * @param topic
     * @param info
     */
    void updateTopicSubscribeInfo(final String topic, final Set<MessageQueue> info);

    /**
     * 是否要进行更新
     * @param topic
     * @return
     */
    boolean isSubscribeTopicNeedUpdate(final String topic);

    /**
     * ？是否为订阅组的单位？？
     * @return
     */
    boolean isUnitMode();

    /**
     * 运行时的信息
     * @return
     */
    ConsumerRunningInfo consumerRunningInfo();
}
