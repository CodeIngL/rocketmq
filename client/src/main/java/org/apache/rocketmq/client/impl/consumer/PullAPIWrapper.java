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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.rocketmq.client.consumer.PullCallback;
import org.apache.rocketmq.client.consumer.PullResult;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.hook.FilterMessageContext;
import org.apache.rocketmq.client.hook.FilterMessageHook;
import org.apache.rocketmq.client.impl.CommunicationMode;
import org.apache.rocketmq.client.impl.FindBrokerResult;
import org.apache.rocketmq.client.impl.factory.MQClientInstance;
import org.apache.rocketmq.client.log.ClientLogger;
import org.apache.rocketmq.common.MQVersion;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.filter.ExpressionType;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.protocol.header.PullMessageRequestHeader;
import org.apache.rocketmq.common.protocol.heartbeat.SubscriptionData;
import org.apache.rocketmq.common.protocol.route.TopicRouteData;
import org.apache.rocketmq.common.sysflag.PullSysFlag;
import org.apache.rocketmq.remoting.exception.RemotingException;

import static org.apache.rocketmq.client.consumer.PullStatus.FOUND;
import static org.apache.rocketmq.common.message.MessageAccessor.putProperty;
import static org.apache.rocketmq.common.message.MessageConst.*;
import static org.apache.rocketmq.common.sysflag.PullSysFlag.hasClassFilterFlag;

/**
 * 拉模式的核心处理逻辑,无论是push还是pull，实际上是一种拉取的模式，但是在broker中，如果支持长连接的话，我们会在何时时候进行推送会客户端。
 */
public class PullAPIWrapper {
    private final InternalLogger log = ClientLogger.getLog();
    private final MQClientInstance mQClientFactory;
    private final String consumerGroup;
    private final boolean unitMode;
    //消息应该从哪个队列拉取
    private ConcurrentMap<MessageQueue, AtomicLong/* brokerId */> pullFromWhichNodeTable = new ConcurrentHashMap<MessageQueue, AtomicLong>(32);
    private volatile boolean connectBrokerByUser = false;
    private volatile long defaultBrokerId = MixAll.MASTER_ID;
    private Random random = new Random(System.currentTimeMillis());
    private ArrayList<FilterMessageHook> filterMessageHookList = new ArrayList<FilterMessageHook>();

    public PullAPIWrapper(MQClientInstance mQClientFactory, String consumerGroup, boolean unitMode) {
        this.mQClientFactory = mQClientFactory;
        this.consumerGroup = consumerGroup;
        this.unitMode = unitMode;
    }

    /**
     * 处理消息拉取结果，需要再进行匹配，因为，broker端还是不是完全完善的
     * @param mq
     * @param pullResult
     * @param subscriptionData
     * @return
     */
    public PullResult processPullResult(final MessageQueue mq, final PullResult pullResult, final SubscriptionData subscriptionData) {
        //结果
        PullResultExt pullResultExt = (PullResultExt) pullResult;

        //更新下一次建议拉取的消息来自哪个节点，拉取结果会告诉我们下一个合适拉取broker
        this.updatePullFromWhichNode(mq, pullResultExt.getSuggestWhichBrokerId());

        if (FOUND == pullResult.getPullStatus()) {
            //解码成消息
            List<MessageExt> msgList = MessageDecoder.decodes(ByteBuffer.wrap(pullResultExt.getMessageBinary()));

            //过滤的消息
            List<MessageExt> msgListFilterAgain = msgList;
            // 存在tag，要匹配tag notice:精确匹配消息
            if (!subscriptionData.getTagsSet().isEmpty() && !subscriptionData.isClassFilterMode()) {
                msgListFilterAgain = new ArrayList<>(msgList.size());
                for (MessageExt msg : msgList) {
                    if (msg.getTags() != null) {
                        //消息的tag需要是订阅信息的一个子集，才能保证
                        if (subscriptionData.getTagsSet().contains(msg.getTags())) {
                            msgListFilterAgain.add(msg);
                        }
                    }
                }
            }

            //存在hook，要匹配hook
            if (this.hasHook()) {
                FilterMessageContext filterMessageContext = new FilterMessageContext();
                filterMessageContext.setUnitMode(unitMode);
                filterMessageContext.setMsgList(msgListFilterAgain);
                this.executeHook(filterMessageContext);
            }

            for (MessageExt msg : msgListFilterAgain) {
                //事务消息处理
                String traFlag = msg.getProperty(PROPERTY_TRANSACTION_PREPARED);
                if (traFlag != null && Boolean.parseBoolean(traFlag)) {
                    msg.setTransactionId(msg.getProperty(PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX)); //设置事物id
                }
                putProperty(msg, PROPERTY_MIN_OFFSET, Long.toString(pullResult.getMinOffset())); //最小的offset
                putProperty(msg, PROPERTY_MAX_OFFSET, Long.toString(pullResult.getMaxOffset())); //最大的offset
            }

            //拉取结果可以匹配消息
            pullResultExt.setMsgFoundList(msgListFilterAgain);
        }

        //减小压力
        pullResultExt.setMessageBinary(null);

        return pullResult;
    }

    /**
     * 更新下一次消息应该从哪个broker中拉取，这个结果有本次从broker中拉取的消息结果体现
     * @param mq
     * @param brokerId
     */
    public void updatePullFromWhichNode(final MessageQueue mq, final long brokerId) {
        AtomicLong suggest = this.pullFromWhichNodeTable.get(mq);
        if (null == suggest) {
            this.pullFromWhichNodeTable.put(mq, new AtomicLong(brokerId));
        } else {
            suggest.set(brokerId);
        }
    }

    public boolean hasHook() {
        return !this.filterMessageHookList.isEmpty();
    }

    /**
     * 执行hook
     * @param context
     */
    public void executeHook(final FilterMessageContext context) {
        if (!this.filterMessageHookList.isEmpty()) {
            for (FilterMessageHook hook : this.filterMessageHookList) {
                try {
                    hook.filterMessage(context);
                } catch (Throwable e) {
                    log.error("execute hook error. hookName={}", hook.hookName());
                }
            }
        }
    }

    /**
     * 核心的拉取模式对远程broker消息的拉取
     * @param mq 消息队列
     * @param subExpression 订阅表达式
     * @param expressionType 订阅类型
     * @param subVersion 版本
     * @param offset 拉取开始的offset
     * @param maxNums 最大消息量
     * @param sysFlag 系统标记
     * @param commitOffset 已提交的offset
     * @param brokerSuspendMaxTimeMillis 支持挂起的最大时间
     * @param timeoutMillis 超时时间
     * @param communicationMode 通信方式
     * @param pullCallback 回调参数
     * @return 拉取消息的结果
     * @throws MQClientException
     * @throws RemotingException
     * @throws MQBrokerException
     * @throws InterruptedException
     */
    public PullResult pullKernelImpl(final MessageQueue mq, final String subExpression, final String expressionType, final long subVersion,
        final long offset, final int maxNums, final int sysFlag, final long commitOffset, final long brokerSuspendMaxTimeMillis,
        final long timeoutMillis, final CommunicationMode communicationMode, final PullCallback pullCallback
    ) throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
        //查找broker
        FindBrokerResult findBrokerResult = mQClientFactory.findBrokerAddressInSubscribe(mq.getBrokerName(), this.recalculatePullFromWhichNode(mq), false);
        if (null == findBrokerResult) {
            //找不到从nameserver中跟新本地的信息
            this.mQClientFactory.updateTopicRouteInfoFromNameServer(mq.getTopic());
            //现在再从本地中找一遍
            findBrokerResult = mQClientFactory.findBrokerAddressInSubscribe(mq.getBrokerName(), this.recalculatePullFromWhichNode(mq), false);
        }

        if (findBrokerResult != null) {
            {
                // check version
                if (!ExpressionType.isTagType(expressionType) && findBrokerResult.getBrokerVersion() < MQVersion.Version.V4_1_0_SNAPSHOT.ordinal()) {
                    throw new MQClientException("The broker[" + mq.getBrokerName() + ", " + findBrokerResult.getBrokerVersion() + "] does not upgrade to support for filter message by " + expressionType, null);
                }
            }
            int sysFlagInner = sysFlag;

            if (findBrokerResult.isSlave()) {
                //是slave我们删除携带的commitOffset相关标记
                sysFlagInner = PullSysFlag.clearCommitOffsetFlag(sysFlagInner);
            }

            //构建拉取请求
            PullMessageRequestHeader reqHeader = new PullMessageRequestHeader();
            reqHeader.setConsumerGroup(this.consumerGroup);
            reqHeader.setTopic(mq.getTopic());
            reqHeader.setQueueId(mq.getQueueId());
            reqHeader.setQueueOffset(offset);
            reqHeader.setMaxMsgNums(maxNums);
            reqHeader.setSysFlag(sysFlagInner);
            reqHeader.setCommitOffset(commitOffset);
            reqHeader.setSuspendTimeoutMillis(brokerSuspendMaxTimeMillis);
            reqHeader.setSubscription(subExpression);
            reqHeader.setSubVersion(subVersion);
            reqHeader.setExpressionType(expressionType);

            //获得broker地址
            String brokerAddr = findBrokerResult.getBrokerAddr();
            if (hasClassFilterFlag(sysFlagInner)) {
                //存在filterServer，从filterServer这个代理的角色上拉取消息
                brokerAddr = computePullFromWhichFilterServer(mq.getTopic(), brokerAddr);
            }

            //正是拉取消息
            return this.mQClientFactory.getMQClientAPIImpl().pullMessage(brokerAddr, reqHeader, timeoutMillis, communicationMode, pullCallback);
        }

        throw new MQClientException("The broker[" + mq.getBrokerName() + "] not exist", null);
    }

    /**
     * 计算重新去哪个节点拉取消息，用于减轻主从压力，比如从从节点上获取
     * master角色的id总是0
     * @param mq
     * @return
     */
    public long recalculatePullFromWhichNode(final MessageQueue mq) {
        if (this.isConnectBrokerByUser()) {
            return this.defaultBrokerId;
        }

        //从缓存中获得我们建议的拉取broker节点
        AtomicLong suggest = this.pullFromWhichNodeTable.get(mq);
        if (suggest != null) {
            return suggest.get();
        }

        //默认返回主节点
        return MixAll.MASTER_ID;
    }

    /**
     * 存在filterServer计算broker对应的filterServer地址
     */
    private String computePullFromWhichFilterServer(final String topic, final String brokerAddr) throws MQClientException {
        ConcurrentMap<String, TopicRouteData> topicRouteTable = this.mQClientFactory.getTopicRouteTable();
        if (topicRouteTable != null) {
            TopicRouteData topicRouteData = topicRouteTable.get(topic);
            List<String> list = topicRouteData.getFilterServerTable().get(brokerAddr);
            if (list != null && !list.isEmpty()) {
                //随机从获得一个FilterServer的地址
                return list.get(randomNum() % list.size());
            }
        }

        throw new MQClientException("Find Filter Server Failed, Broker Addr: " + brokerAddr + " topic: " + topic, null);
    }

    public boolean isConnectBrokerByUser() {
        return connectBrokerByUser;
    }

    public void setConnectBrokerByUser(boolean connectBrokerByUser) {
        this.connectBrokerByUser = connectBrokerByUser;

    }

    public int randomNum() {
        int value = random.nextInt();
        if (value < 0) {
            value = Math.abs(value);
            if (value < 0)
                value = 0;
        }
        return value;
    }

    public void registerFilterMessageHook(ArrayList<FilterMessageHook> filterMessageHookList) {
        this.filterMessageHookList = filterMessageHookList;
    }

    public long getDefaultBrokerId() {
        return defaultBrokerId;
    }

    public void setDefaultBrokerId(long defaultBrokerId) {
        this.defaultBrokerId = defaultBrokerId;
    }
}
