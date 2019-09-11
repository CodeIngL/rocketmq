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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.rocketmq.client.consumer.AllocateMessageQueueStrategy;
import org.apache.rocketmq.client.impl.FindBrokerResult;
import org.apache.rocketmq.client.impl.factory.MQClientInstance;
import org.apache.rocketmq.client.log.ClientLogger;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.protocol.body.LockBatchRequestBody;
import org.apache.rocketmq.common.protocol.body.UnlockBatchRequestBody;
import org.apache.rocketmq.common.protocol.heartbeat.ConsumeType;
import org.apache.rocketmq.common.protocol.heartbeat.MessageModel;
import org.apache.rocketmq.common.protocol.heartbeat.SubscriptionData;

/**
 * Base class for rebalance algorithm
 * <p>
 *     客户端负载均衡
 * </p>
 */
public abstract class RebalanceImpl {
    protected static final InternalLogger log = ClientLogger.getLog();

    //消费队列和其快照对应
    protected final ConcurrentMap<MessageQueue, ProcessQueue> processQueueTable = new ConcurrentHashMap<MessageQueue, ProcessQueue>(64);
    //topic和对应的消费队列集合
    protected final ConcurrentMap<String/* topic */, Set<MessageQueue>> topicSubscribeInfoTable = new ConcurrentHashMap<String, Set<MessageQueue>>();
    //topic和订阅信息
    protected final ConcurrentMap<String /* topic */, SubscriptionData> subscriptionInner = new ConcurrentHashMap<String, SubscriptionData>();
    //消费组
    protected String consumerGroup;
    //消费方式
    protected MessageModel messageModel;
    //分配消息队列的策略
    protected AllocateMessageQueueStrategy allocateMessageQueueStrategy;
    //网络客户端
    protected MQClientInstance mQClientFactory;

    public RebalanceImpl(String consumerGroup, MessageModel messageModel,
        AllocateMessageQueueStrategy allocateMessageQueueStrategy,
        MQClientInstance mQClientFactory) {
        this.consumerGroup = consumerGroup;
        this.messageModel = messageModel;
        this.allocateMessageQueueStrategy = allocateMessageQueueStrategy;
        this.mQClientFactory = mQClientFactory;
    }

    public void unlock(final MessageQueue mq, final boolean oneway) {
        FindBrokerResult findBrokerResult = this.mQClientFactory.findBrokerAddressInSubscribe(mq.getBrokerName(), MixAll.MASTER_ID, true);
        if (findBrokerResult != null) {
            UnlockBatchRequestBody reqBody = new UnlockBatchRequestBody();
            reqBody.setConsumerGroup(this.consumerGroup);
            reqBody.setClientId(this.mQClientFactory.getClientId());
            reqBody.getMqSet().add(mq);

            try {
                this.mQClientFactory.getMQClientAPIImpl().unlockBatchMQ(findBrokerResult.getBrokerAddr(), reqBody, 1000, oneway);
                log.warn("unlock messageQueue. group:{}, clientId:{}, mq:{}", consumerGroup, mQClientFactory.getClientId(), mq);
            } catch (Exception e) {
                log.error("unlockBatchMQ exception, " + mq, e);
            }
        }
    }

    public void unlockAll(final boolean oneway) {
        HashMap<String, Set<MessageQueue>> brokerMqs = this.buildProcessQueueTableByBrokerName();

        for (final Map.Entry<String, Set<MessageQueue>> entry : brokerMqs.entrySet()) {
            final String brokerName = entry.getKey();
            final Set<MessageQueue> mqs = entry.getValue();

            if (mqs.isEmpty())
                continue;

            FindBrokerResult findBrokerResult = this.mQClientFactory.findBrokerAddressInSubscribe(brokerName, MixAll.MASTER_ID, true);
            if (findBrokerResult != null) {
                UnlockBatchRequestBody requestBody = new UnlockBatchRequestBody();
                requestBody.setConsumerGroup(this.consumerGroup);
                requestBody.setClientId(this.mQClientFactory.getClientId());
                requestBody.setMqSet(mqs);

                try {
                    this.mQClientFactory.getMQClientAPIImpl().unlockBatchMQ(findBrokerResult.getBrokerAddr(), requestBody, 1000, oneway);

                    for (MessageQueue mq : mqs) {
                        ProcessQueue processQueue = this.processQueueTable.get(mq);
                        if (processQueue != null) {
                            processQueue.setLocked(false);
                            log.info("the message queue unlock OK, Group: {} {}", this.consumerGroup, mq);
                        }
                    }
                } catch (Exception e) {
                    log.error("unlockBatchMQ exception, " + mqs, e);
                }
            }
        }
    }

    /**
     * 构建brokerName和对应的相关的消息队列集合的映射
     * @return
     */
    private HashMap<String/* brokerName */, Set<MessageQueue>> buildProcessQueueTableByBrokerName() {
        HashMap<String, Set<MessageQueue>> result = new HashMap<String, Set<MessageQueue>>();
        for (MessageQueue mq : this.processQueueTable.keySet()) {
            Set<MessageQueue> mqs = result.get(mq.getBrokerName());
            if (null == mqs) {
                mqs = new HashSet<>();
                result.put(mq.getBrokerName(), mqs);
            }

            mqs.add(mq);
        }

        return result;
    }

    /**
     * 锁定一个消息队列
     * @param mq
     * @return
     */
    public boolean lock(final MessageQueue mq) {
        //查找相关的信息
        FindBrokerResult findBrokerResult = this.mQClientFactory.findBrokerAddressInSubscribe(mq.getBrokerName(), MixAll.MASTER_ID, true);
        if (findBrokerResult == null){
            return false;
        }
        LockBatchRequestBody requestBody = new LockBatchRequestBody();
        requestBody.setConsumerGroup(this.consumerGroup);
        requestBody.setClientId(this.mQClientFactory.getClientId());
        requestBody.getMqSet().add(mq);

        try {
            Set<MessageQueue> lockedMq = this.mQClientFactory.getMQClientAPIImpl().lockBatchMQ(findBrokerResult.getBrokerAddr(), requestBody, 1000);
            for (MessageQueue mmqq : lockedMq) {
                ProcessQueue processQueue = this.processQueueTable.get(mmqq);
                if (processQueue != null) {
                    processQueue.setLocked(true);
                    processQueue.setLastLockTimestamp(System.currentTimeMillis());
                }
            }

            boolean lockOK = lockedMq.contains(mq);
            log.info("the message queue lock {}, {} {}", lockOK ? "OK" : "Failed", this.consumerGroup, mq);
            return lockOK;
        } catch (Exception e) {
            log.error("lockBatchMQ exception, " + mq, e);
        }
            return false;

    }

    public void lockAll() {
        HashMap<String, Set<MessageQueue>> brokerMqs = this.buildProcessQueueTableByBrokerName();

        for (Entry<String, Set<MessageQueue>> entry : brokerMqs.entrySet()) {
            //名字
            final String brokerName = entry.getKey();
            //mqs
            final Set<MessageQueue> mqs = entry.getValue();

            if (mqs.isEmpty())
                continue;

            //查找对应的broker
            FindBrokerResult findBrokerResult = this.mQClientFactory.findBrokerAddressInSubscribe(brokerName, MixAll.MASTER_ID, true);
            if (findBrokerResult == null){
                continue;
            }
            //批量锁定请求
            LockBatchRequestBody reqBody = new LockBatchRequestBody();
            reqBody.setConsumerGroup(this.consumerGroup);
            reqBody.setClientId(this.mQClientFactory.getClientId());
            reqBody.setMqSet(mqs);

            try {
                //让broker锁定指定相关的mq
                Set<MessageQueue> lockOKMQSet = this.mQClientFactory.getMQClientAPIImpl().lockBatchMQ(findBrokerResult.getBrokerAddr(), reqBody, 1000);

                //遍历后，我们锁定对应的processQueue
                for (MessageQueue mq : lockOKMQSet) {
                    ProcessQueue processQueue = this.processQueueTable.get(mq);
                    if (processQueue != null) {
                        if (!processQueue.isLocked()) {
                            log.info("the message queue locked OK, Group: {} {}", this.consumerGroup, mq);
                        }
                        processQueue.setLocked(true);
                        processQueue.setLastLockTimestamp(System.currentTimeMillis());
                    }
                }

                //遍历mq，如果broker返回相关，我们遍历，可能有些无法锁定，我们设置并记录
                for (MessageQueue mq : mqs) {
                    if (!lockOKMQSet.contains(mq)) {
                        ProcessQueue processQueue = this.processQueueTable.get(mq);
                        if (processQueue != null) {
                            processQueue.setLocked(false);
                            log.warn("the message queue locked Failed, Group: {} {}", this.consumerGroup, mq);
                        }
                    }
                }
            } catch (Exception e) {
                log.error("lockBatchMQ exception, " + mqs, e);
            }
        }
    }


    /**
     * rebalanceImpl进行平衡，两种方式统一负载均衡操作，pull不支持isOrder操作，push根据设置支持isOrder操作。
     * @param isOrder 是否是顺序消费
     */
    public void doRebalance(final boolean isOrder) {
        //遍历所有的订阅关系
        Map<String, SubscriptionData> subTable = this.getSubscriptionInner();
        if (subTable != null) {
            for (final Map.Entry<String, SubscriptionData> entry : subTable.entrySet()) {
                String topic = entry.getKey();
                try {
                    this.rebalanceByTopic(topic, isOrder); //对每一个topic进行负载均衡
                } catch (Throwable e) {
                    if (!topic.startsWith(MixAll.RETRY_GROUP_TOPIC_PREFIX)) {
                        log.warn("rebalanceByTopic Exception", e);
                    }
                }
            }
        }

        this.truncateMessageQueueNotMyTopic();
    }

    public ConcurrentMap<String, SubscriptionData> getSubscriptionInner() {
        return subscriptionInner;
    }

    /**
     * 根据topic进行负载均衡，同理pull方式不支持isOrder，push方式根据设置进行支持与否
     * @param topic 可能要负载均衡的topic
     * @param isOrder 是否有序
     */
    private void rebalanceByTopic(final String topic, final boolean isOrder) {
        Set<MessageQueue> mqSet = this.topicSubscribeInfoTable.get(topic);//找到topic对应的消费队列
        switch (messageModel) {
            case BROADCASTING: { //广播方式
                if (mqSet == null){
                    log.warn("doRebalance, {}, but the topic[{}] not exist.", consumerGroup, topic);
                    return;
                }
                boolean changed = this.updateProcessQueueTableInRebalance(topic, mqSet, isOrder);
                if (changed) {
                    this.messageQueueChanged(topic, mqSet, mqSet);
                    log.info("messageQueueChanged {} {} {} {}", consumerGroup, topic, mqSet, mqSet);
                }
                break;
            }
            case CLUSTERING: { //集群方式
                if (null == mqSet) {
                    if (!topic.startsWith(MixAll.RETRY_GROUP_TOPIC_PREFIX)) { //没有对应的消息队列，而且这个topic也不是重试的topic，说明没有相关的信息直接返回
                        log.warn("doRebalance, {}, but the topic[{}] not exist.", consumerGroup, topic);
                    }
                    return;
                }

                List<String> cidAll = this.mQClientFactory.findConsumerIdList(topic, consumerGroup); //找到远程的消费者的Id
                if (null == cidAll) { //该topic没有相应的消费客户端，说明没有客户端进行消费，自然我们不需要进行负载均衡，直接返回
                    log.warn("doRebalance, {} {}, get consumer id list failed", consumerGroup, topic);
                    return;
                }

                List<MessageQueue> mqAll = new ArrayList<MessageQueue>();
                mqAll.addAll(mqSet);

                Collections.sort(mqAll); //排序
                Collections.sort(cidAll); //排序

                AllocateMessageQueueStrategy strategy = this.allocateMessageQueueStrategy; //分配策略

                List<MessageQueue> allocateResult = null;
                try {
                    /**
                     * 分配策略
                     */
                    allocateResult = strategy.allocate(this.consumerGroup, this.mQClientFactory.getClientId(), mqAll, cidAll); //分配结果
                } catch (Throwable e) {
                    log.error("AllocateMessageQueueStrategy.allocate Exception. allocateMessageQueueStrategyName={}", strategy.getName(), e);
                    return;
                }

                Set<MessageQueue> allocateResultSet = new HashSet<>(); //分配结果去重
                if (allocateResult != null) {
                    allocateResultSet.addAll(allocateResult);
                }

                boolean changed = this.updateProcessQueueTableInRebalance(topic, allocateResultSet, isOrder); //是否有变更
                if (changed) {
                    log.info("rebalanced result changed. allocateMessageQueueStrategyName={}, group={}, topic={}, clientId={}, mqAllSize={}, cidAllSize={}, rebalanceResultSize={}, rebalanceResultSet={}",
                            strategy.getName(), consumerGroup, topic, this.mQClientFactory.getClientId(), mqSet.size(), cidAll.size(), allocateResultSet.size(), allocateResultSet);
                    this.messageQueueChanged(topic, mqSet, allocateResultSet); //有变更进行通知
                }
            }
            break;
            default:
                break;
        }
    }

    /**
     * 尝试清理不是我的topic的消息队列
     */
    private void truncateMessageQueueNotMyTopic() {
        Map<String, SubscriptionData> subTable = this.getSubscriptionInner(); //获得我订阅的

        for (MessageQueue mq : this.processQueueTable.keySet()) { //遍历存储
            if (!subTable.containsKey(mq.getTopic())) { //不是我的
                ProcessQueue pq = this.processQueueTable.remove(mq);
                if (pq != null) {
                    pq.setDropped(true); //设置，
                    log.info("doRebalance, {}, truncateMessageQueueNotMyTopic remove unnecessary mq, {}", consumerGroup, mq);
                }
            }
        }
    }

    /**
     * 更新重平衡结果，同样，pull模式不支持顺序消费，push模式按照设置可以支持相关的顺序消费
     * @param topic 主题
     * @param mqSet 获得的消息队列，目前topic在本机上对应的消费队列集合，也就是新的消息队列
     * @param isOrder 是否是顺序消费
     * @return
     */
    private boolean updateProcessQueueTableInRebalance(final String topic, final Set<MessageQueue> mqSet, final boolean isOrder) {
        boolean changed = false;

        //消息队列和处理队列
        Iterator<Entry<MessageQueue, ProcessQueue>> it = this.processQueueTable.entrySet().iterator(); //本地存储的副本
        //进行可能的更改
        while (it.hasNext()) {
            Entry<MessageQueue, ProcessQueue> next = it.next();
            MessageQueue mq = next.getKey();
            ProcessQueue pq = next.getValue();

            if (mq.getTopic().equals(topic)) { //topic一致
                if (!mqSet.contains(mq)) { //重新负载均衡的结果不包含副本存储的，需要标记这个删除
                    //标记这个要删除
                    pq.setDropped(true);
                    if (this.removeUnnecessaryMessageQueue(mq, pq)) { //是否移除这个已经被标记drop消息队列
                        it.remove();
                        changed = true;
                        log.info("doRebalance, {}, remove unnecessary mq, {}", consumerGroup, mq);
                    }
                } else if (pq.isPullExpired()) { //拉取超时
                    switch (this.consumeType()) { //类型
                        case CONSUME_ACTIVELY: //活跃，pull模式不关注
                            break;
                        case CONSUME_PASSIVELY: //不活跃
                            pq.setDropped(true); //丢弃
                            if (this.removeUnnecessaryMessageQueue(mq, pq)) { //是否移除这个已经被标记drop消息队列
                                it.remove();
                                changed = true;
                                log.error("[BUG]doRebalance, {}, remove unnecessary mq, {}, because pull is pause, so try to fixed it", consumerGroup, mq);
                            }
                            break;
                        default:
                            break;
                    }
                }
            }
        }

        //构建重新需要拉取的分发上的请求
        List<PullRequest> pullRequestList = new ArrayList<>();
        //遍历，构建分发的请求
        for (MessageQueue mq : mqSet) {
            if(this.processQueueTable.containsKey(mq)){ //已存在我们不用关注这个，上面已经处理过了
                continue;
            }
            if (isOrder && !this.lock(mq)) {
                log.warn("doRebalance, {}, add a new mq failed, {}, because lock failed", consumerGroup, mq);
                continue;
            }

            //新建操作

            //删除脏副本
            this.removeDirtyOffset(mq); //删除这个队列对应的offset
            ProcessQueue pq = new ProcessQueue(); //构建新的副本
            long nextOffset = this.computePullFromWhere(mq); //计算下一个offset，pull方式总是0，push则是会进行相关的计算
            if (nextOffset >= 0) {
                ProcessQueue pre = this.processQueueTable.putIfAbsent(mq, pq); //更新放置
                if (pre != null) { //先前存在
                    log.info("doRebalance, {}, mq already exists, {}", consumerGroup, mq);
                } else { //先前不存在
                    log.info("doRebalance, {}, add a new mq, {}", consumerGroup, mq);
                    PullRequest req = new PullRequest(); //构建一个消息准备进行拉取消息
                    req.setConsumerGroup(consumerGroup);
                    req.setNextOffset(nextOffset);
                    req.setMessageQueue(mq);
                    req.setProcessQueue(pq);
                    pullRequestList.add(req);
                    changed = true;
                }
            } else {
                log.warn("doRebalance, {}, add new mq failed, {}", consumerGroup, mq);
            }
        }

        /**
         * 分发处理的结果
         */
        this.dispatchPullRequest(pullRequestList); //分发拉取的请求列表，因为pull上面是0，因此构建这里也是不进行相关的处理
        return changed;
    }

    /**
     * rebalance进行切换，有变更的时候，我们进行相关的通知
     * @param topic
     * @param mqAll
     * @param mqDivided
     */
    public abstract void messageQueueChanged(final String topic, final Set<MessageQueue> mqAll,
        final Set<MessageQueue> mqDivided);

    /**
     * 是否移除不必要的消息队列，由具体子类根据自己的方式来进行标记是否进行删除
     * @param mq
     * @param pq
     * @return
     */
    public abstract boolean removeUnnecessaryMessageQueue(final MessageQueue mq, final ProcessQueue pq);

    public abstract ConsumeType consumeType();

    public abstract void removeDirtyOffset(final MessageQueue mq);

    public abstract long computePullFromWhere(final MessageQueue mq);

    /**
     * 消费端负载均衡完毕后，进行分发请求，由不同方式子类来确定分发的逻辑
     * @param pullRequestList
     */
    public abstract void dispatchPullRequest(final List<PullRequest> pullRequestList);

    public void removeProcessQueue(final MessageQueue mq) {
        ProcessQueue prev = this.processQueueTable.remove(mq);
        if (prev != null) {
            boolean droped = prev.isDropped();
            prev.setDropped(true);
            this.removeUnnecessaryMessageQueue(mq, prev);
            log.info("Fix Offset, {}, remove unnecessary mq, {} Droped: {}", consumerGroup, mq, droped);
        }
    }

    public ConcurrentMap<MessageQueue, ProcessQueue> getProcessQueueTable() {
        return processQueueTable;
    }

    public ConcurrentMap<String, Set<MessageQueue>> getTopicSubscribeInfoTable() {
        return topicSubscribeInfoTable;
    }

    public String getConsumerGroup() {
        return consumerGroup;
    }

    public void setConsumerGroup(String consumerGroup) {
        this.consumerGroup = consumerGroup;
    }

    public MessageModel getMessageModel() {
        return messageModel;
    }

    public void setMessageModel(MessageModel messageModel) {
        this.messageModel = messageModel;
    }

    public AllocateMessageQueueStrategy getAllocateMessageQueueStrategy() {
        return allocateMessageQueueStrategy;
    }

    public void setAllocateMessageQueueStrategy(AllocateMessageQueueStrategy allocateMessageQueueStrategy) {
        this.allocateMessageQueueStrategy = allocateMessageQueueStrategy;
    }

    public MQClientInstance getmQClientFactory() {
        return mQClientFactory;
    }

    public void setmQClientFactory(MQClientInstance mQClientFactory) {
        this.mQClientFactory = mQClientFactory;
    }

    public void destroy() {
        Iterator<Entry<MessageQueue, ProcessQueue>> it = this.processQueueTable.entrySet().iterator();
        while (it.hasNext()) {
            Entry<MessageQueue, ProcessQueue> next = it.next();
            next.getValue().setDropped(true);
        }

        this.processQueueTable.clear();
    }
}
