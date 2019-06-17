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

package org.apache.rocketmq.broker.longpolling;

import org.apache.rocketmq.store.MessageArrivingListener;

import java.util.Map;

/**
 * 消息从commitlog中投递的到消费队列的时候进行，到达触发
 */
public class NotifyMessageArrivingListener implements MessageArrivingListener {

    private final PullRequestHoldService holdService;

    public NotifyMessageArrivingListener(final PullRequestHoldService holdService) {
        this.holdService = holdService;
    }

    @Override
    public void arriving(String topic, int queueId, long logicOffset, long tagsCode,
        long msgStoreTime, byte[] filterBitMap, Map<String, String> properties) {
        this.holdService.notifyMessageArriving(topic, queueId, logicOffset, tagsCode, msgStoreTime, filterBitMap, properties);
    }
}
