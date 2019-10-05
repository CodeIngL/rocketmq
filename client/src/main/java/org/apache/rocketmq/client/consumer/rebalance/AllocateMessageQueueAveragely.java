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
package org.apache.rocketmq.client.consumer.rebalance;

import java.util.ArrayList;
import java.util.List;
import org.apache.rocketmq.client.consumer.AllocateMessageQueueStrategy;
import org.apache.rocketmq.client.log.ClientLogger;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.common.message.MessageQueue;

/**
 * Average Hashing queue algorithm
 * 默认的分配算法
 */
public class AllocateMessageQueueAveragely implements AllocateMessageQueueStrategy {
    private final InternalLogger log = ClientLogger.getLog();

    /**
     * 默认的分配算法
     * @param consumerGroup current consumer group
     * @param currentCID current consumer id
     * @param mqAll message queue set in current topic
     * @param cidAll consumer set in current consumer group
     * @return
     */
    @Override
    public List<MessageQueue> allocate(String consumerGroup, String currentCID, List<MessageQueue> mqAll, List<String> cidAll) {
        if (currentCID == null || currentCID.length() < 1) {
            throw new IllegalArgumentException("currentCID is empty");
        }
        if (mqAll == null || mqAll.isEmpty()) {
            throw new IllegalArgumentException("mqAll is null or mqAll empty");
        }
        if (cidAll == null || cidAll.isEmpty()) {
            throw new IllegalArgumentException("cidAll is null or cidAll empty");
        }

        List<MessageQueue> result = new ArrayList<MessageQueue>();
        if (!cidAll.contains(currentCID)) {
            log.info("[BUG] ConsumerGroup: {} The consumerId: {} not in cidAll: {}", consumerGroup, currentCID, cidAll);
            return result;
        }

        //当前id在列表中的位置
        int index = cidAll.indexOf(currentCID);
        //消费队列取模消费者数量,模几
        int mod = mqAll.size() % cidAll.size();
        //消息队列小于消费者，为1，否则消费队列数/消费者数，一般来说消费队列远远大于消费者数量

        //平均一个消费者拖多个消费队列，不足补1，足就不用补
        //当消息队列小于消费者数量，平均值就是1，当模数大于当前cid坐标，是值+1；否则是值
        int averageSize = mqAll.size() <= cidAll.size() ? 1 : (mod > 0 && index < mod ? mqAll.size() / cidAll.size() + 1 : mqAll.size() / cidAll.size());
        //开始的坐标，没有加一则使用乘法，否则加一个mod
        int startIndex = (mod > 0 && index < mod) ? index * averageSize : index * averageSize + mod;
        //计算范围
        int range = Math.min(averageSize, mqAll.size() - startIndex);

        //把这一部分算在自己头上
        for (int i = 0; i < range; i++) {
            result.add(mqAll.get((startIndex + i) % mqAll.size()));
        }
        return result;
    }

    @Override
    public String getName() {
        return "AVG";
    }
}
