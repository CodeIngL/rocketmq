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
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;

import org.apache.rocketmq.client.QueryResult;
import org.apache.rocketmq.client.Validators;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.MessageSelector;
import org.apache.rocketmq.client.consumer.PullCallback;
import org.apache.rocketmq.client.consumer.PullResult;
import org.apache.rocketmq.client.consumer.listener.MessageListener;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.consumer.listener.MessageListenerOrderly;
import org.apache.rocketmq.client.consumer.store.LocalFileOffsetStore;
import org.apache.rocketmq.client.consumer.store.OffsetStore;
import org.apache.rocketmq.client.consumer.store.ReadOffsetType;
import org.apache.rocketmq.client.consumer.store.RemoteBrokerOffsetStore;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.hook.ConsumeMessageContext;
import org.apache.rocketmq.client.hook.ConsumeMessageHook;
import org.apache.rocketmq.client.hook.FilterMessageHook;
import org.apache.rocketmq.client.impl.CommunicationMode;
import org.apache.rocketmq.client.impl.MQClientManager;
import org.apache.rocketmq.client.impl.factory.MQClientInstance;
import org.apache.rocketmq.client.log.ClientLogger;
import org.apache.rocketmq.client.stat.ConsumerStatsManager;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.ServiceState;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.filter.FilterAPI;
import org.apache.rocketmq.common.help.FAQUrl;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageAccessor;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.protocol.body.ConsumeStatus;
import org.apache.rocketmq.common.protocol.body.ConsumerRunningInfo;
import org.apache.rocketmq.common.protocol.body.ProcessQueueInfo;
import org.apache.rocketmq.common.protocol.body.QueueTimeSpan;
import org.apache.rocketmq.common.protocol.heartbeat.ConsumeType;
import org.apache.rocketmq.common.protocol.heartbeat.MessageModel;
import org.apache.rocketmq.common.protocol.heartbeat.SubscriptionData;
import org.apache.rocketmq.common.protocol.route.BrokerData;
import org.apache.rocketmq.common.protocol.route.TopicRouteData;
import org.apache.rocketmq.common.sysflag.PullSysFlag;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.exception.RemotingException;

import static org.apache.rocketmq.client.consumer.store.ReadOffsetType.READ_FROM_MEMORY;
import static org.apache.rocketmq.client.impl.CommunicationMode.ASYNC;
import static org.apache.rocketmq.common.MixAll.DEFAULT_CONSUMER_GROUP;
import static org.apache.rocketmq.common.MixAll.RETRY_GROUP_TOPIC_PREFIX;
import static org.apache.rocketmq.common.ServiceState.CREATE_JUST;
import static org.apache.rocketmq.common.ServiceState.START_FAILED;
import static org.apache.rocketmq.common.filter.FilterAPI.buildSubscriptionData;
import static org.apache.rocketmq.common.help.FAQUrl.*;
import static org.apache.rocketmq.common.protocol.heartbeat.MessageModel.CLUSTERING;
import static org.apache.rocketmq.common.sysflag.PullSysFlag.buildSysFlag;

/**
 * 内部承载push形式消费消息核心流程类
 *
 * @see DefaultMQPullConsumerImpl
 */
public class DefaultMQPushConsumerImpl implements MQConsumerInner {
    /**
     * Delay some time when exception occur
     */
    private static final long PULL_TIME_DELAY_MILLS_WHEN_EXCEPTION = 3000;
    /**
     * Flow control interval
     */
    private static final long PULL_TIME_DELAY_MILLS_WHEN_FLOW_CONTROL = 50;
    /**
     * Delay some time when suspend pull service
     */
    private static final long PULL_TIME_DELAY_MILLS_WHEN_SUSPEND = 1000;
    private static final long BROKER_SUSPEND_MAX_TIME_MILLIS = 1000 * 15;
    private static final long CONSUMER_TIMEOUT_MILLIS_WHEN_SUSPEND = 1000 * 30;
    private final InternalLogger log = ClientLogger.getLog();
    //拉取消息的上层接口
    private final DefaultMQPushConsumer defaultMQPushConsumer;
    //负载均衡实现
    private final RebalanceImpl rebalanceImpl = new RebalancePushImpl(this);
    //过滤消息hook
    private final ArrayList<FilterMessageHook> filterMessageHookList = new ArrayList<>();
    //一次消费的开始时间
    private final long consumerStartTimestamp = System.currentTimeMillis();
    //消费消息hook
    private final ArrayList<ConsumeMessageHook> consumeMessageHookList = new ArrayList<>();
    //rpcHook
    private final RPCHook rpcHook;
    //服务状态
    private volatile ServiceState serviceState = CREATE_JUST;
    //网络客户端
    private MQClientInstance mQClientFactory;
    //核心的拉起组件
    private PullAPIWrapper pullAPIWrapper;
    //暂停标记
    private volatile boolean pause = false;
    //是否顺序消费
    private boolean consumeOrderly = false;
    //监听器
    private MessageListener messageListenerInner;
    //消息offset存储
    private OffsetStore offsetStore;
    //消费服务
    private ConsumeMessageService consumeMessageService;
    //流控次数，记录发生流控的次数
    private long queueFlowControlTimes = 0;
    //跨度流控次数，记录发生流控的次数
    private long queueMaxSpanFlowControlTimes = 0;

    public DefaultMQPushConsumerImpl(DefaultMQPushConsumer defaultMQPushConsumer, RPCHook rpcHook) {
        this.defaultMQPushConsumer = defaultMQPushConsumer;
        this.rpcHook = rpcHook;
    }

    public void registerFilterMessageHook(final FilterMessageHook hook) {
        this.filterMessageHookList.add(hook);
        log.info("register FilterMessageHook Hook, {}", hook.hookName());
    }

    public boolean hasHook() {
        return !this.consumeMessageHookList.isEmpty();
    }

    public void registerConsumeMessageHook(final ConsumeMessageHook hook) {
        this.consumeMessageHookList.add(hook);
        log.info("register consumeMessageHook Hook, {}", hook.hookName());
    }

    public void executeHookBefore(final ConsumeMessageContext context) {
        if (!this.consumeMessageHookList.isEmpty()) {
            for (ConsumeMessageHook hook : this.consumeMessageHookList) {
                try {
                    hook.consumeMessageBefore(context);
                } catch (Throwable e) {
                }
            }
        }
    }

    public void executeHookAfter(final ConsumeMessageContext context) {
        if (!this.consumeMessageHookList.isEmpty()) {
            for (ConsumeMessageHook hook : this.consumeMessageHookList) {
                try {
                    hook.consumeMessageAfter(context);
                } catch (Throwable e) {
                }
            }
        }
    }

    public void createTopic(String key, String newTopic, int queueNum) throws MQClientException {
        createTopic(key, newTopic, queueNum, 0);
    }

    public void createTopic(String key, String newTopic, int queueNum, int topicSysFlag) throws MQClientException {
        this.mQClientFactory.getMQAdminImpl().createTopic(key, newTopic, queueNum, topicSysFlag);
    }

    public Set<MessageQueue> fetchSubscribeMessageQueues(String topic) throws MQClientException {
        Set<MessageQueue> result = this.rebalanceImpl.getTopicSubscribeInfoTable().get(topic);
        if (null == result) {
            this.mQClientFactory.updateTopicRouteInfoFromNameServer(topic);
            result = this.rebalanceImpl.getTopicSubscribeInfoTable().get(topic);
        }

        if (null == result) {
            throw new MQClientException("The topic[" + topic + "] not exist", null);
        }

        return result;
    }

    public DefaultMQPushConsumer getDefaultMQPushConsumer() {
        return defaultMQPushConsumer;
    }

    public long earliestMsgStoreTime(MessageQueue mq) throws MQClientException {
        return this.mQClientFactory.getMQAdminImpl().earliestMsgStoreTime(mq);
    }

    public long maxOffset(MessageQueue mq) throws MQClientException {
        return this.mQClientFactory.getMQAdminImpl().maxOffset(mq);
    }

    public long minOffset(MessageQueue mq) throws MQClientException {
        return this.mQClientFactory.getMQAdminImpl().minOffset(mq);
    }

    public OffsetStore getOffsetStore() {
        return offsetStore;
    }

    public void setOffsetStore(OffsetStore offsetStore) {
        this.offsetStore = offsetStore;
    }

    /**
     * 拉取消息，即使这个请求包含的ProcessQueue被标记删除了，我们不再去消费这个请求，因为他已经被删除了
     *
     * @param pullRequest
     */
    public void pullMessage(final PullRequest pullRequest) {

        //获得ProcessQueue
        final ProcessQueue processQueue = pullRequest.getProcessQueue();
        if (processQueue.isDropped()) {
            //已经被舍弃了，不需要再关注这个消息队列
            log.info("the pull request[{}] is dropped.", pullRequest.toString());
            return;
        }

        //设置最后一次拉取时间
        pullRequest.getProcessQueue().setLastPullTimestamp(System.currentTimeMillis());

        try {
            this.makeSureStateOK();
        } catch (MQClientException e) {
            log.warn("pullMessage exception, consumer state not ok", e);
            //延迟执行
            this.executePullRequestLater(pullRequest, PULL_TIME_DELAY_MILLS_WHEN_EXCEPTION);//3000
            return;
        }

        if (this.isPause()) {
            log.warn("consumer was paused, execute pull request later. instanceName={}, group={}", this.defaultMQPushConsumer.getInstanceName(), this.defaultMQPushConsumer.getConsumerGroup());
            //延迟执行
            this.executePullRequestLater(pullRequest, PULL_TIME_DELAY_MILLS_WHEN_SUSPEND);//1000
            return;
        }

        //processQueue当前的缓存消息数量
        long cachedMessageCount = processQueue.getMsgCount().get();
        //processQueue当前的缓存大小
        long cachedMessageSizeInMiB = processQueue.getMsgSize().get() / (1024 * 1024);

        //流控操作1，缓存的消息数量大于设定的流控阈值(1000)
        if (cachedMessageCount > this.defaultMQPushConsumer.getPullThresholdForQueue()) {
            //延迟执行拉取消息
            this.executePullRequestLater(pullRequest, PULL_TIME_DELAY_MILLS_WHEN_FLOW_CONTROL);//50
            if ((queueFlowControlTimes++ % 1000) == 0) {
                //日志输出
                log.warn("the cached message count exceeds the threshold {}, so do flow control, minOffset={}, maxOffset={}, count={}, size={} MiB, pullRequest={}, flowControlTimes={}",
                        this.defaultMQPushConsumer.getPullThresholdForQueue(), processQueue.getMsgTreeMap().firstKey(), processQueue.getMsgTreeMap().lastKey(), cachedMessageCount, cachedMessageSizeInMiB, pullRequest, queueFlowControlTimes);
            }
            return;
        }

        //流控操作2，缓存的大小大于设定的流控阈值(100M)
        if (cachedMessageSizeInMiB > this.defaultMQPushConsumer.getPullThresholdSizeForQueue()) {
            //延迟执行拉取消息
            this.executePullRequestLater(pullRequest, PULL_TIME_DELAY_MILLS_WHEN_FLOW_CONTROL); //50
            if ((queueFlowControlTimes++ % 1000) == 0) {
                //日志输出
                log.warn("the cached message size exceeds the threshold {} MiB, so do flow control, minOffset={}, maxOffset={}, count={}, size={} MiB, pullRequest={}, flowControlTimes={}",
                        this.defaultMQPushConsumer.getPullThresholdSizeForQueue(), processQueue.getMsgTreeMap().firstKey(), processQueue.getMsgTreeMap().lastKey(), cachedMessageCount, cachedMessageSizeInMiB, pullRequest, queueFlowControlTimes);
            }
            return;
        }


        if (!this.consumeOrderly) {
            //普通消费模式额外的流控支持
            //流控操作3，消息缓存的跨度超过设定的流控阈值(2000)
            if (processQueue.getMaxSpan() > this.defaultMQPushConsumer.getConsumeConcurrentlyMaxSpan()) {
                //延迟执行拉取消息
                this.executePullRequestLater(pullRequest, PULL_TIME_DELAY_MILLS_WHEN_FLOW_CONTROL); //50
                if ((queueMaxSpanFlowControlTimes++ % 1000) == 0) {
                    //日志输出
                    log.warn("the queue's messages, span too long, so do flow control, minOffset={}, maxOffset={}, maxSpan={}, pullRequest={}, flowControlTimes={}",
                            processQueue.getMsgTreeMap().firstKey(), processQueue.getMsgTreeMap().lastKey(), processQueue.getMaxSpan(), pullRequest, queueMaxSpanFlowControlTimes);
                }
                return;
            }
        } else {
            //顺序消费模式
            if (processQueue.isLocked()) { //已经上锁
                if (!pullRequest.isLockedFirst()) {
                    //不是第一次上锁，我们尝试上锁，就是第一次请求 获得要拉取消息的offset
                    final long offset = this.rebalanceImpl.computePullFromWhere(pullRequest.getMessageQueue());
                    boolean brokerBusy = offset < pullRequest.getNextOffset();
                    log.info("the first time to pull message, so fix offset from broker. pullRequest: {} NewOffset: {} brokerBusy: {}", pullRequest, offset, brokerBusy);
                    if (brokerBusy) {
                        log.info("[NOTIFYME]the first time to pull message, but pull request offset larger than broker consume offset. pullRequest: {} NewOffset: {}", pullRequest, offset);
                    }

                    //设置锁定
                    pullRequest.setLockedFirst(true);
                    //设置拉取offset
                    pullRequest.setNextOffset(offset);
                }
            } else {
                //延迟执行拉取
                this.executePullRequestLater(pullRequest, PULL_TIME_DELAY_MILLS_WHEN_EXCEPTION); //3000
                log.info("pull message later because not locked in broker, {}", pullRequest);
                return;
            }
        }


        //获得节点对topic的订阅信息
        final SubscriptionData subscriptionData = this.rebalanceImpl.getSubscriptionInner().get(pullRequest.getMessageQueue().getTopic());
        if (null == subscriptionData) {
            //无订阅信息，延迟执行拉取
            this.executePullRequestLater(pullRequest, PULL_TIME_DELAY_MILLS_WHEN_EXCEPTION); //3000
            log.warn("find the consumer's subscription failed, {}", pullRequest);
            return;
        }

        //开始时间
        final long beginTimestamp = System.currentTimeMillis();

        PullCallback callback = new PullCallback() {

            @Override
            public void onSuccess(PullResult result) {
                if (result == null) { //结果空忽略
                    return;
                }
                MessageQueue mq = pullRequest.getMessageQueue(); //本消息队列
                //处理拉取消息结果，内部消息会被简单进行处理，比如统一的转换成对象，并进行真正的匹配
                result = DefaultMQPushConsumerImpl.this.pullAPIWrapper.processPullResult(mq, result, subscriptionData);//处理消息队列

                //继续处理，还没有落入到真正的结构中
                switch (result.getPullStatus()) {
                    case FOUND: //发现存在消息，进行处理
                        long prevRequestOffset = pullRequest.getNextOffset(); //先前拉取的offset
                        pullRequest.setNextOffset(result.getNextBeginOffset()); //下一次拉取的offset
                        //拉取来回时间
                        long pullRT = System.currentTimeMillis() - beginTimestamp;
                        //统计rt
                        DefaultMQPushConsumerImpl.this.getConsumerStatsManager().incPullRT(pullRequest.getConsumerGroup(), mq.getTopic(), pullRT);

                        //第一个消息的offset
                        long firstMsgOffset = Long.MAX_VALUE;
                        if (result.getMsgFoundList() == null || result.getMsgFoundList().isEmpty()) {
                            //匹配后结果是空的，立即尝试拉取消息
                            DefaultMQPushConsumerImpl.this.executePullRequestImmediately(pullRequest);
                        } else {
                            //有消息，第一个消息的offset
                            firstMsgOffset = result.getMsgFoundList().get(0).getQueueOffset();

                            //统计tps
                            DefaultMQPushConsumerImpl.this.getConsumerStatsManager().incPullTPS(pullRequest.getConsumerGroup(), mq.getTopic(), result.getMsgFoundList().size());

                            //将拉取消息分发进消息的内存存储，也就是processQueue中，processQueue才是真正存储着我们要消费的消息
                            //匹配的消息先进行投递到内存结构中
                            boolean dispatchToConsume = processQueue.putMessage(result.getMsgFoundList());
                            //消费消息的服务，构建一个需要被消费的请求，由消费消息的线程进行处理，这里因为上面已经分发了相关的消息，也就是给用户的消费监听进行消费
                            consumeMessageService.submitConsumeRequest(result.getMsgFoundList(), processQueue, mq, dispatchToConsume);


                            //根据设定配置，是否进行立马还是延迟的拉取消息
                            if (defaultMQPushConsumer.getPullInterval() > 0) {
                                executePullRequestLater(pullRequest, DefaultMQPushConsumerImpl.this.defaultMQPushConsumer.getPullInterval());
                            } else {
                                executePullRequestImmediately(pullRequest);
                            }
                        }

                        //结果存在问题，输出日志
                        if (result.getNextBeginOffset() < prevRequestOffset || firstMsgOffset < prevRequestOffset) {
                            log.warn("[BUG] pull message result maybe data wrong, nextBeginOffset: {} firstMsgOffset: {} prevRequestOffset: {}",
                                    result.getNextBeginOffset(), firstMsgOffset, prevRequestOffset);
                        }

                        break;
                    case NO_NEW_MSG:
                    case NO_MATCHED_MSG:
                        //没有新消息，或者没有匹配的消息，设置下一次nextOffset
                        pullRequest.setNextOffset(result.getNextBeginOffset());

                        //矫正offset，现在的内存结构中的消息可能已经没有
                        DefaultMQPushConsumerImpl.this.correctTagsOffset(pullRequest);

                        //立即拉取消息
                        executePullRequestImmediately(pullRequest);
                        break;
                    case OFFSET_ILLEGAL:
                        //非法的offset，我们标记整个消息队列应该是有问题的。
                        log.warn("the pull request offset illegal, {} {}", pullRequest.toString(), result.toString());
                        pullRequest.setNextOffset(result.getNextBeginOffset()); //设置下一个nextOffset
                        pullRequest.getProcessQueue().setDropped(true); //标记该存储要被丢弃
                        DefaultMQPushConsumerImpl.this.executeTaskLater(() -> {
                            try {
                                //更新
                                offsetStore.updateOffset(pullRequest.getMessageQueue(), pullRequest.getNextOffset(), false);

                                //持久化
                                offsetStore.persist(pullRequest.getMessageQueue());

                                //删除
                                rebalanceImpl.removeProcessQueue(pullRequest.getMessageQueue());

                                log.warn("fix the pull request offset, {}", pullRequest);
                            } catch (Throwable e) {
                                log.error("executeTaskLater Exception", e);
                            }
                        }, 10000);
                        break;
                    default:
                        break;
                }
            }

            @Override
            public void onException(Throwable e) {
                if (!pullRequest.getMessageQueue().getTopic().startsWith(RETRY_GROUP_TOPIC_PREFIX)) {
                    log.warn("execute the pull request exception", e);
                }
                //重试，延迟执行拉取
                DefaultMQPushConsumerImpl.this.executePullRequestLater(pullRequest, PULL_TIME_DELAY_MILLS_WHEN_EXCEPTION); //3000
            }
        };

        boolean commitOffsetEnable = false;
        long commitOffsetValue = 0L;
        if (CLUSTERING == this.defaultMQPushConsumer.getMessageModel()) {
            //集群方式支持，我们需要拿到当下节点已经消费到哪里了，因此我们需要从offsetStore中读取相关信息
            //从内存中获取offset
            commitOffsetValue = this.offsetStore.readOffset(pullRequest.getMessageQueue(), READ_FROM_MEMORY);
            if (commitOffsetValue > 0) {
                commitOffsetEnable = true;
            }
        }

        //构建订阅表达式
        String subExpression = null;
        boolean classFilter = false;
        SubscriptionData sd = this.rebalanceImpl.getSubscriptionInner().get(pullRequest.getMessageQueue().getTopic());
        if (sd != null) {
            if (this.defaultMQPushConsumer.isPostSubscriptionWhenPull() && !sd.isClassFilterMode()) {
                //支持构建订阅的信息，我们会在标记中构建支持的订阅信息，并且不是基于类过滤的模式
                subExpression = sd.getSubString();
            }
            classFilter = sd.isClassFilterMode();
        }

        /* commitOffset*//* suspend*/ /* subscription*//* class filter*/
        /**
         * push模式支持长轮训的方式，也就是支持在broker端进行挂起
         */
        int sysFlag = buildSysFlag(commitOffsetEnable, true, subExpression != null, classFilter);
        try {
            //开始拉取消息，核心的拉取消息api
            this.pullAPIWrapper.pullKernelImpl(
                    pullRequest.getMessageQueue(), subExpression, subscriptionData.getExpressionType(), subscriptionData.getSubVersion(),
                    pullRequest.getNextOffset(), this.defaultMQPushConsumer.getPullBatchSize(), sysFlag, commitOffsetValue,
                    BROKER_SUSPEND_MAX_TIME_MILLIS, CONSUMER_TIMEOUT_MILLIS_WHEN_SUSPEND, ASYNC, callback);
        } catch (Exception e) {
            log.error("pullKernelImpl exception", e);
            //延迟进行消息的拉取，ex:broker存在问题
            this.executePullRequestLater(pullRequest, PULL_TIME_DELAY_MILLS_WHEN_EXCEPTION);//3000
        }
    }


    /**
     * 确定服务的状态
     *
     * @throws MQClientException
     */
    private void makeSureStateOK() throws MQClientException {
        if (this.serviceState != ServiceState.RUNNING) {
            throw new MQClientException("The consumer service state not OK, " + this.serviceState + suggestTodo(CLIENT_SERVICE_NOT_OK), null);
        }
    }

    /**
     * 随后执行
     *
     * @param pullRequest
     * @param timeDelay
     */
    private void executePullRequestLater(final PullRequest pullRequest, final long timeDelay) {
        this.mQClientFactory.getPullMessageService().executePullRequestLater(pullRequest, timeDelay);
    }

    public boolean isPause() {
        return pause;
    }

    public void setPause(boolean pause) {
        this.pause = pause;
    }

    public ConsumerStatsManager getConsumerStatsManager() {
        return this.mQClientFactory.getConsumerStatsManager();
    }

    /**
     * 立即拉取请求，一般发生在处理已经拉取请求之后，当然负载均衡服务也会触发
     *
     * @param pullRequest
     */
    public void executePullRequestImmediately(final PullRequest pullRequest) {
        this.mQClientFactory.getPullMessageService().executePullRequestImmediately(pullRequest);
    }

    /**
     * 更新offset
     * @param pullRequest
     */
    private void correctTagsOffset(final PullRequest pullRequest) {
        if (0L == pullRequest.getProcessQueue().getMsgCount().get()) {//消息为0，内存中的消息全部被消费完了
            //进行更新offset
            this.offsetStore.updateOffset(pullRequest.getMessageQueue(), pullRequest.getNextOffset(), true);
        }
    }

    public void executeTaskLater(final Runnable r, final long timeDelay) {
        this.mQClientFactory.getPullMessageService().executeTaskLater(r, timeDelay);
    }

    public QueryResult queryMessage(String topic, String key, int maxNum, long begin, long end)
            throws MQClientException, InterruptedException {
        return this.mQClientFactory.getMQAdminImpl().queryMessage(topic, key, maxNum, begin, end);
    }

    public MessageExt queryMessageByUniqKey(String topic, String uniqKey) throws MQClientException,
            InterruptedException {
        return this.mQClientFactory.getMQAdminImpl().queryMessageByUniqKey(topic, uniqKey);
    }

    public void registerMessageListener(MessageListener messageListener) {
        this.messageListenerInner = messageListener;
    }

    /**
     * 恢复
     */
    public void resume() {
        this.pause = false;
        doRebalance();
        log.info("resume this consumer, {}", this.defaultMQPushConsumer.getConsumerGroup());
    }

    /**
     * 消息重发
     *
     * @param msg
     * @param delayLevel
     * @param brokerName
     * @throws RemotingException
     * @throws MQBrokerException
     * @throws InterruptedException
     * @throws MQClientException
     */
    public void sendMessageBack(MessageExt msg, int delayLevel, final String brokerName)
            throws RemotingException, MQBrokerException, InterruptedException, MQClientException {
        try {
            String brokerAddr = (null != brokerName) ? this.mQClientFactory.findBrokerAddressInPublish(brokerName) : RemotingHelper.parseSocketAddressAddr(msg.getStoreHost());
            this.mQClientFactory.getMQClientAPIImpl().consumerSendMessageBack(brokerAddr, msg,
                    this.defaultMQPushConsumer.getConsumerGroup(), delayLevel, 5000, getMaxReconsumeTimes());
        } catch (Exception e) {
            log.error("sendMessageBack Exception, " + this.defaultMQPushConsumer.getConsumerGroup(), e);

            //发送失败的，我们进行重试，但是会使用延迟，

            //重试的topic
            Message newMsg = new Message(MixAll.getRetryTopic(this.defaultMQPushConsumer.getConsumerGroup()), msg.getBody());

            //原始的消息id
            String originMsgId = MessageAccessor.getOriginMessageId(msg);
            MessageAccessor.setOriginMessageId(newMsg, UtilAll.isBlank(originMsgId) ? msg.getMsgId() : originMsgId);

            //设计标记
            newMsg.setFlag(msg.getFlag());
            //属性
            MessageAccessor.setProperties(newMsg, msg.getProperties());
            //属性
            MessageAccessor.putProperty(newMsg, MessageConst.PROPERTY_RETRY_TOPIC, msg.getTopic());
            //重发次数
            MessageAccessor.setReconsumeTime(newMsg, String.valueOf(msg.getReconsumeTimes() + 1));
            //最大重试次数
            MessageAccessor.setMaxReconsumeTimes(newMsg, String.valueOf(getMaxReconsumeTimes()));
            //设置延迟级别，重新消费，我们需要根据重试的次数，进行设置新的延迟等级
            newMsg.setDelayTimeLevel(3 + msg.getReconsumeTimes());

            //发送
            this.mQClientFactory.getDefaultMQProducer().send(newMsg);
        }
    }

    private int getMaxReconsumeTimes() {
        // default reconsume times: 16
        if (this.defaultMQPushConsumer.getMaxReconsumeTimes() == -1) {
            return 16;
        } else {
            return this.defaultMQPushConsumer.getMaxReconsumeTimes();
        }
    }

    public synchronized void shutdown() {
        switch (this.serviceState) {
            case CREATE_JUST:
                break;
            case RUNNING:
                this.consumeMessageService.shutdown();
                this.persistConsumerOffset();
                this.mQClientFactory.unregisterConsumer(this.defaultMQPushConsumer.getConsumerGroup());
                this.mQClientFactory.shutdown();
                log.info("the consumer [{}] shutdown OK", this.defaultMQPushConsumer.getConsumerGroup());
                this.rebalanceImpl.destroy();
                this.serviceState = ServiceState.SHUTDOWN_ALREADY;
                break;
            case SHUTDOWN_ALREADY:
                break;
            default:
                break;
        }
    }

    /**
     * push消费
     *
     * @throws MQClientException
     */
    public synchronized void start() throws MQClientException {
        String consumerGroup = defaultMQPushConsumer.getConsumerGroup(); //消费组
        MessageModel messageModel = defaultMQPushConsumer.getMessageModel(); //消息订阅模式
        switch (serviceState) { //状态
            case CREATE_JUST: //刚刚创建
                log.info("the consumer [{}] start beginning. messageModel={}, isUnitMode={}", consumerGroup, messageModel, defaultMQPushConsumer.isUnitMode());
                serviceState = START_FAILED;

                //检查配置
                checkConfig();

                //copy订阅的相关信息
                copySubscription();

                //集群消费方式，默认方式
                if (messageModel == CLUSTERING) {
                    defaultMQPushConsumer.changeInstanceNameToPID();
                }

                //构建mq的网络客户端
                mQClientFactory = MQClientManager.getInstance().getAndCreateMQClientInstance(defaultMQPushConsumer, rpcHook);

                //构建消费端负载均衡

                //设置集群组
                rebalanceImpl.setConsumerGroup(consumerGroup);
                //设置消费方式
                rebalanceImpl.setMessageModel(messageModel);
                //设置消息队列分配策略
                rebalanceImpl.setAllocateMessageQueueStrategy(defaultMQPushConsumer.getAllocateMessageQueueStrategy());
                //设置客户端
                rebalanceImpl.setmQClientFactory(mQClientFactory);

                //构建核心拉取方式

                pullAPIWrapper = new PullAPIWrapper(mQClientFactory, consumerGroup, isUnitMode());
                pullAPIWrapper.registerFilterMessageHook(filterMessageHookList);

                //构建控制消费offset的offset存储
                if (defaultMQPushConsumer.getOffsetStore() != null) { //有自己的消息存储，使用自己的消息存储，否则根据具体的模型进行存储的使用
                    offsetStore = defaultMQPushConsumer.getOffsetStore();//使用用户自己设置的存储
                } else {
                    switch (messageModel) {
                        case BROADCASTING://广播模式下的offset存储，使用了本地的消息存储
                            offsetStore = new LocalFileOffsetStore(mQClientFactory, consumerGroup);
                            break;
                        case CLUSTERING://集群模式下的offset存储，使用了交互式的基于远程的消息存储
                            offsetStore = new RemoteBrokerOffsetStore(mQClientFactory, consumerGroup);
                            break;
                        default:
                            break;
                    }
                    defaultMQPushConsumer.setOffsetStore(offsetStore); //设置offset的存储
                }
                offsetStore.load(); //存储加载

                //消息监听的服务，构建顺序消费，或者并发消费，这是和pull模式一点区别

                MessageListener listener = getMessageListenerInner();
                if (listener instanceof MessageListenerOrderly) {
                    consumeOrderly = true; //顺序消费
                    //消费服务是顺序消费
                    consumeMessageService = new ConsumeMessageOrderlyService(this, (MessageListenerOrderly) listener);
                } else if (listener instanceof MessageListenerConcurrently) {
                    //非顺序消费
                    consumeOrderly = false;
                    //消费服务是并发的消费
                    consumeMessageService = new ConsumeMessageConcurrentlyService(this, (MessageListenerConcurrently) listener);
                }
                consumeMessageService.start();

                //注册

                boolean registerOK = mQClientFactory.registerConsumer(consumerGroup, this);
                if (!registerOK) {
                    serviceState = CREATE_JUST;
                    consumeMessageService.shutdown();
                    throw new MQClientException("The consumer group[" + consumerGroup + "] has been created before, specify another name please." + suggestTodo(GROUP_NAME_DUPLICATE_URL),
                            null);
                }

                //开启
                mQClientFactory.start();
                log.info("the consumer [{}] start OK.", consumerGroup);
                serviceState = ServiceState.RUNNING;
                break;
            case RUNNING:
            case START_FAILED:
            case SHUTDOWN_ALREADY:
                //错误的状态
                throw new MQClientException("The PushConsumer service state not OK, maybe started once, "
                        + serviceState
                        + suggestTodo(CLIENT_SERVICE_NOT_OK),
                        null);
            default:
                break;
        }

        //以下是other的区别


        //push方式根据自己订阅的数据，尝试更新路由信息后，立即开始other操作
        Map<String, SubscriptionData> subTable = this.getSubscriptionInner();
        if (subTable != null) {
            for (final Map.Entry<String, SubscriptionData> entry : subTable.entrySet()) {
                final String topic = entry.getKey();
                this.mQClientFactory.updateTopicRouteInfoFromNameServer(topic);
            }
        }
        mQClientFactory.checkClientInBroker();
        mQClientFactory.sendHeartbeatToAllBrokerWithLock();
        //立即进行一次负载均衡，促进拉取消息
        mQClientFactory.rebalanceImmediately();
    }

    /**
     * 校验初始化的相关属性，我们需要进行相关的设置，使用的时候
     *
     * @throws MQClientException
     */
    private void checkConfig() throws MQClientException {
        String consumerGroup = this.defaultMQPushConsumer.getConsumerGroup();
        Validators.checkGroup(consumerGroup);

        if (null == consumerGroup) {
            throw new MQClientException("consumerGroup is null" + suggestTodo(CLIENT_PARAMETER_CHECK_URL), null);
        }

        if (consumerGroup.equals(DEFAULT_CONSUMER_GROUP)) {
            throw new MQClientException("consumerGroup can not equal " + DEFAULT_CONSUMER_GROUP + ", please specify another one."
                    + suggestTodo(CLIENT_PARAMETER_CHECK_URL),
                    null);
        }

        if (null == this.defaultMQPushConsumer.getMessageModel()) {
            throw new MQClientException("messageModel is null" + suggestTodo(CLIENT_PARAMETER_CHECK_URL), null);
        }

        if (null == this.defaultMQPushConsumer.getConsumeFromWhere()) {
            throw new MQClientException("consumeFromWhere is null" + suggestTodo(CLIENT_PARAMETER_CHECK_URL), null);
        }

        Date dt = UtilAll.parseDate(this.defaultMQPushConsumer.getConsumeTimestamp(), UtilAll.YYYYMMDDHHMMSS);
        if (null == dt) {
            throw new MQClientException(
                    "consumeTimestamp is invalid, the valid format is yyyyMMddHHmmss,but received "
                            + this.defaultMQPushConsumer.getConsumeTimestamp()
                            + " " + suggestTodo(CLIENT_PARAMETER_CHECK_URL), null);
        }

        // allocateMessageQueueStrategy
        if (null == this.defaultMQPushConsumer.getAllocateMessageQueueStrategy()) {
            throw new MQClientException("allocateMessageQueueStrategy is null" + suggestTodo(CLIENT_PARAMETER_CHECK_URL), null);
        }

        // subscription
        if (null == this.defaultMQPushConsumer.getSubscription()) {
            throw new MQClientException("subscription is null" + suggestTodo(CLIENT_PARAMETER_CHECK_URL), null);
        }

        // messageListener
        if (null == this.defaultMQPushConsumer.getMessageListener()) {
            throw new MQClientException("messageListener is null" + suggestTodo(CLIENT_PARAMETER_CHECK_URL), null);
        }

        boolean orderly = this.defaultMQPushConsumer.getMessageListener() instanceof MessageListenerOrderly;
        boolean concurrently = this.defaultMQPushConsumer.getMessageListener() instanceof MessageListenerConcurrently;
        if (!orderly && !concurrently) {
            throw new MQClientException("messageListener must be instanceof MessageListenerOrderly or MessageListenerConcurrently"
                    + suggestTodo(CLIENT_PARAMETER_CHECK_URL), null);
        }

        // consumeThreadMin
        if (this.defaultMQPushConsumer.getConsumeThreadMin() < 1
                || this.defaultMQPushConsumer.getConsumeThreadMin() > 1000) {
            throw new MQClientException("consumeThreadMin Out of range [1, 1000]" + suggestTodo(CLIENT_PARAMETER_CHECK_URL), null);
        }

        // consumeThreadMax
        if (this.defaultMQPushConsumer.getConsumeThreadMax() < 1 || this.defaultMQPushConsumer.getConsumeThreadMax() > 1000) {
            throw new MQClientException("consumeThreadMax Out of range [1, 1000]" + suggestTodo(CLIENT_PARAMETER_CHECK_URL), null);
        }

        // consumeThreadMin can't be larger than consumeThreadMax
        if (this.defaultMQPushConsumer.getConsumeThreadMin() > this.defaultMQPushConsumer.getConsumeThreadMax()) {
            throw new MQClientException(
                    "consumeThreadMin (" + this.defaultMQPushConsumer.getConsumeThreadMin() + ") "
                            + "is larger than consumeThreadMax (" + this.defaultMQPushConsumer.getConsumeThreadMax() + ")",
                    null);
        }

        // consumeConcurrentlyMaxSpan
        if (this.defaultMQPushConsumer.getConsumeConcurrentlyMaxSpan() < 1
                || this.defaultMQPushConsumer.getConsumeConcurrentlyMaxSpan() > 65535) {
            throw new MQClientException("consumeConcurrentlyMaxSpan Out of range [1, 65535]" + suggestTodo(CLIENT_PARAMETER_CHECK_URL),
                    null);
        }

        // pullThresholdForQueue
        if (this.defaultMQPushConsumer.getPullThresholdForQueue() < 1 || this.defaultMQPushConsumer.getPullThresholdForQueue() > 65535) {
            throw new MQClientException(
                    "pullThresholdForQueue Out of range [1, 65535]"
                            + suggestTodo(CLIENT_PARAMETER_CHECK_URL),
                    null);
        }

        // pullThresholdForTopic
        if (this.defaultMQPushConsumer.getPullThresholdForTopic() != -1) {
            if (this.defaultMQPushConsumer.getPullThresholdForTopic() < 1 || this.defaultMQPushConsumer.getPullThresholdForTopic() > 6553500) {
                throw new MQClientException(
                        "pullThresholdForTopic Out of range [1, 6553500]"
                                + suggestTodo(CLIENT_PARAMETER_CHECK_URL),
                        null);
            }
        }

        // pullThresholdSizeForQueue
        if (this.defaultMQPushConsumer.getPullThresholdSizeForQueue() < 1 || this.defaultMQPushConsumer.getPullThresholdSizeForQueue() > 1024) {
            throw new MQClientException(
                    "pullThresholdSizeForQueue Out of range [1, 1024]"
                            + suggestTodo(CLIENT_PARAMETER_CHECK_URL),
                    null);
        }

        if (this.defaultMQPushConsumer.getPullThresholdSizeForTopic() != -1) {
            // pullThresholdSizeForTopic
            if (this.defaultMQPushConsumer.getPullThresholdSizeForTopic() < 1 || this.defaultMQPushConsumer.getPullThresholdSizeForTopic() > 102400) {
                throw new MQClientException(
                        "pullThresholdSizeForTopic Out of range [1, 102400]"
                                + suggestTodo(CLIENT_PARAMETER_CHECK_URL),
                        null);
            }
        }

        // pullInterval
        if (this.defaultMQPushConsumer.getPullInterval() < 0 || this.defaultMQPushConsumer.getPullInterval() > 65535) {
            throw new MQClientException(
                    "pullInterval Out of range [0, 65535]"
                            + suggestTodo(CLIENT_PARAMETER_CHECK_URL),
                    null);
        }

        // consumeMessageBatchMaxSize
        if (this.defaultMQPushConsumer.getConsumeMessageBatchMaxSize() < 1
                || this.defaultMQPushConsumer.getConsumeMessageBatchMaxSize() > 1024) {
            throw new MQClientException(
                    "consumeMessageBatchMaxSize Out of range [1, 1024]"
                            + suggestTodo(CLIENT_PARAMETER_CHECK_URL),
                    null);
        }

        // pullBatchSize
        if (this.defaultMQPushConsumer.getPullBatchSize() < 1 || this.defaultMQPushConsumer.getPullBatchSize() > 1024) {
            throw new MQClientException(
                    "pullBatchSize Out of range [1, 1024]"
                            + suggestTodo(CLIENT_PARAMETER_CHECK_URL),
                    null);
        }
    }

    /**
     * copy相关的订阅的信息
     * 包括订阅信息，监听器，消费方式下关于重试的处理
     *
     * @throws MQClientException
     */
    private void copySubscription() throws MQClientException {
        //获得消费组
        String consumerGroup = this.defaultMQPushConsumer.getConsumerGroup();
        try {
            //遍历toic和订阅组的映射
            Map<String, String> sub = this.defaultMQPushConsumer.getSubscription();
            if (sub != null) {
                for (final Map.Entry<String, String> entry : sub.entrySet()) {
                    final String topic = entry.getKey();
                    final String subString = entry.getValue();
                    SubscriptionData subscriptionData = buildSubscriptionData(consumerGroup, topic, subString);
                    this.rebalanceImpl.getSubscriptionInner().put(topic, subscriptionData);
                }
            }

            if (null == this.messageListenerInner) {
                this.messageListenerInner = this.defaultMQPushConsumer.getMessageListener();
            }

            switch (this.defaultMQPushConsumer.getMessageModel()) {
                case BROADCASTING:
                    break;
                case CLUSTERING:
                    //集群模式
                    final String retryTopic = MixAll.getRetryTopic(consumerGroup);
                    SubscriptionData subscriptionData = buildSubscriptionData(consumerGroup, retryTopic, SubscriptionData.SUB_ALL);
                    this.rebalanceImpl.getSubscriptionInner().put(retryTopic, subscriptionData);
                    break;
                default:
                    break;
            }
        } catch (Exception e) {
            throw new MQClientException("subscription exception", e);
        }
    }

    public MessageListener getMessageListenerInner() {
        return messageListenerInner;
    }

    public ConcurrentMap<String, SubscriptionData> getSubscriptionInner() {
        return this.rebalanceImpl.getSubscriptionInner();
    }

    /**
     * 使用子表达式的订阅，比如使用tag
     *
     * @param topic
     * @param subExpression
     * @throws MQClientException
     */
    public void subscribe(String topic, String subExpression) throws MQClientException {
        try {
            SubscriptionData subscriptionData = buildSubscriptionData(this.defaultMQPushConsumer.getConsumerGroup(), topic, subExpression);
            this.rebalanceImpl.getSubscriptionInner().put(topic, subscriptionData);
            if (this.mQClientFactory != null) {
                //订阅立马发送一次心跳
                this.mQClientFactory.sendHeartbeatToAllBrokerWithLock();
            }
        } catch (Exception e) {
            throw new MQClientException("subscription exception", e);
        }
    }

    /**
     * 使用自定义类的表达式的订阅
     *
     * @param topic
     * @param fullClassName
     * @param filterClassSource
     * @throws MQClientException
     */
    public void subscribe(String topic, String fullClassName, String filterClassSource) throws MQClientException {
        try {
            //构建订阅的数据
            SubscriptionData subscriptionData = buildSubscriptionData(this.defaultMQPushConsumer.getConsumerGroup(), topic, "*");
            //设置其他
            subscriptionData.setSubString(fullClassName); //更新subString是全类名
            subscriptionData.setClassFilterMode(true); //设置使用类过滤方式
            subscriptionData.setFilterClassSource(filterClassSource); //设置过滤类的源码


            //更新内部数据
            this.rebalanceImpl.getSubscriptionInner().put(topic, subscriptionData);
            if (this.mQClientFactory != null) {
                //订阅，立马通过心跳进行发送信息
                this.mQClientFactory.sendHeartbeatToAllBrokerWithLock();
            }

        } catch (Exception e) {
            throw new MQClientException("subscription exception", e);
        }
    }

    /**
     * 带有selector的订阅
     *
     * @param topic
     * @param messageSelector
     * @throws MQClientException
     */
    public void subscribe(final String topic, final MessageSelector messageSelector) throws MQClientException {
        try {
            if (messageSelector == null) { //如果不传递selector，我们委托给普通的使用all的方式进行订阅
                subscribe(topic, SubscriptionData.SUB_ALL);
                return;
            }

            //构建核心的订阅数据，每一种方式的核心都是构建这个核心的订阅数据结构
            SubscriptionData subscriptionData = FilterAPI.build(topic, messageSelector.getExpression(), messageSelector.getExpressionType());

            //放入负载均衡中等待下一次rebalance
            this.rebalanceImpl.getSubscriptionInner().put(topic, subscriptionData);
            if (this.mQClientFactory != null) {
                //订阅立马发送一次心跳
                this.mQClientFactory.sendHeartbeatToAllBrokerWithLock();
            }
        } catch (Exception e) {
            throw new MQClientException("subscription exception", e);
        }
    }

    /**
     * 尝试暂停
     */
    public void suspend() {
        this.pause = true;
        log.info("suspend this consumer, {}", this.defaultMQPushConsumer.getConsumerGroup());
    }

    public void unsubscribe(String topic) {
        this.rebalanceImpl.getSubscriptionInner().remove(topic);
    }

    public void updateConsumeOffset(MessageQueue mq, long offset) {
        this.offsetStore.updateOffset(mq, offset, false);
    }

    public void updateCorePoolSize(int corePoolSize) {
        this.consumeMessageService.updateCorePoolSize(corePoolSize);
    }

    public MessageExt viewMessage(String msgId)
            throws RemotingException, MQBrokerException, InterruptedException, MQClientException {
        return this.mQClientFactory.getMQAdminImpl().viewMessage(msgId);
    }

    public RebalanceImpl getRebalanceImpl() {
        return rebalanceImpl;
    }

    public boolean isConsumeOrderly() {
        return consumeOrderly;
    }

    public void setConsumeOrderly(boolean consumeOrderly) {
        this.consumeOrderly = consumeOrderly;
    }

    public void resetOffsetByTimeStamp(long timeStamp)
            throws RemotingException, MQBrokerException, InterruptedException, MQClientException {
        for (String topic : rebalanceImpl.getSubscriptionInner().keySet()) {
            Set<MessageQueue> mqs = rebalanceImpl.getTopicSubscribeInfoTable().get(topic);
            Map<MessageQueue, Long> offsetTable = new HashMap<MessageQueue, Long>();
            if (mqs != null) {
                for (MessageQueue mq : mqs) {
                    long offset = searchOffset(mq, timeStamp);
                    offsetTable.put(mq, offset);
                }
                this.mQClientFactory.resetOffset(topic, groupName(), offsetTable);
            }
        }
    }

    public long searchOffset(MessageQueue mq, long timestamp) throws MQClientException {
        return this.mQClientFactory.getMQAdminImpl().searchOffset(mq, timestamp);
    }

    @Override
    public String groupName() {
        return this.defaultMQPushConsumer.getConsumerGroup();
    }

    @Override
    public MessageModel messageModel() {
        return this.defaultMQPushConsumer.getMessageModel();
    }

    @Override
    public ConsumeType consumeType() {
        return ConsumeType.CONSUME_PASSIVELY;
    }

    @Override
    public ConsumeFromWhere consumeFromWhere() {
        return this.defaultMQPushConsumer.getConsumeFromWhere();
    }

    /**
     * 获得消费端相关的订阅信息，保存在消费端的rebalance中
     *
     * @return
     */
    @Override
    public Set<SubscriptionData> subscriptions() {
        Set<SubscriptionData> subSet = new HashSet<SubscriptionData>();

        subSet.addAll(this.rebalanceImpl.getSubscriptionInner().values());

        return subSet;
    }

    @Override
    public void doRebalance() {
        if (!this.pause) {//没暂停才进行
            this.rebalanceImpl.doRebalance(this.isConsumeOrderly());
        }
    }

    @Override
    public void persistConsumerOffset() {
        try {
            this.makeSureStateOK();
            Set<MessageQueue> mqs = new HashSet<MessageQueue>();
            Set<MessageQueue> allocateMq = this.rebalanceImpl.getProcessQueueTable().keySet();
            mqs.addAll(allocateMq);

            this.offsetStore.persistAll(mqs);
        } catch (Exception e) {
            log.error("group: " + this.defaultMQPushConsumer.getConsumerGroup() + " persistConsumerOffset exception", e);
        }
    }

    /**
     * 更新订阅的信息
     *
     * @param topic
     * @param info
     */
    @Override
    public void updateTopicSubscribeInfo(String topic, Set<MessageQueue> info) {
        Map<String, SubscriptionData> subTable = this.getSubscriptionInner();
        //获得我订阅德 信息
        if (subTable != null) {
            if (subTable.containsKey(topic)) {
                //符合的是我的订阅的topic，我们存储相关的数据
                this.rebalanceImpl.topicSubscribeInfoTable.put(topic, info);
            }
        }
    }

    @Override
    public boolean isSubscribeTopicNeedUpdate(String topic) {
        Map<String, SubscriptionData> subTable = this.getSubscriptionInner();
        if (subTable != null) {
            if (subTable.containsKey(topic)) {
                return !this.rebalanceImpl.topicSubscribeInfoTable.containsKey(topic);
            }
        }

        return false;
    }

    @Override
    public boolean isUnitMode() {
        return this.defaultMQPushConsumer.isUnitMode();
    }

    @Override
    public ConsumerRunningInfo consumerRunningInfo() {
        ConsumerRunningInfo info = new ConsumerRunningInfo();

        Properties prop = MixAll.object2Properties(this.defaultMQPushConsumer);

        prop.put(ConsumerRunningInfo.PROP_CONSUME_ORDERLY, String.valueOf(this.consumeOrderly));
        prop.put(ConsumerRunningInfo.PROP_THREADPOOL_CORE_SIZE, String.valueOf(this.consumeMessageService.getCorePoolSize()));
        prop.put(ConsumerRunningInfo.PROP_CONSUMER_START_TIMESTAMP, String.valueOf(this.consumerStartTimestamp));

        info.setProperties(prop);

        Set<SubscriptionData> subSet = this.subscriptions();
        info.getSubscriptionSet().addAll(subSet);

        Iterator<Entry<MessageQueue, ProcessQueue>> it = this.rebalanceImpl.getProcessQueueTable().entrySet().iterator();
        while (it.hasNext()) {
            Entry<MessageQueue, ProcessQueue> next = it.next();
            MessageQueue mq = next.getKey();
            ProcessQueue pq = next.getValue();

            ProcessQueueInfo pqinfo = new ProcessQueueInfo();
            pqinfo.setCommitOffset(this.offsetStore.readOffset(mq, ReadOffsetType.MEMORY_FIRST_THEN_STORE));
            pq.fillProcessQueueInfo(pqinfo);
            info.getMqTable().put(mq, pqinfo);
        }

        for (SubscriptionData sd : subSet) {
            ConsumeStatus consumeStatus = this.mQClientFactory.getConsumerStatsManager().consumeStatus(this.groupName(), sd.getTopic());
            info.getStatusTable().put(sd.getTopic(), consumeStatus);
        }

        return info;
    }

    public MQClientInstance getmQClientFactory() {
        return mQClientFactory;
    }

    public void setmQClientFactory(MQClientInstance mQClientFactory) {
        this.mQClientFactory = mQClientFactory;
    }

    public ServiceState getServiceState() {
        return serviceState;
    }

    //Don't use this deprecated setter, which will be removed soon.
    @Deprecated
    public synchronized void setServiceState(ServiceState serviceState) {
        this.serviceState = serviceState;
    }

    /**
     * 调整相关的线程池
     */
    public void adjustThreadPool() {
        long computeAccTotal = this.computeAccumulationTotal();
        long adjustThreadPoolNumsThreshold = this.defaultMQPushConsumer.getAdjustThreadPoolNumsThreshold();

        long incThreshold = (long) (adjustThreadPoolNumsThreshold * 1.0);

        long decThreshold = (long) (adjustThreadPoolNumsThreshold * 0.8);

        if (computeAccTotal >= incThreshold) {
            //最终反映到消费服务上
            this.consumeMessageService.incCorePoolSize();
        }

        if (computeAccTotal < decThreshold) {
            //最终反映到消费的服务上
            this.consumeMessageService.decCorePoolSize();
        }
    }

    private long computeAccumulationTotal() {
        long msgAccTotal = 0;
        ConcurrentMap<MessageQueue, ProcessQueue> processQueueTable = this.rebalanceImpl.getProcessQueueTable();
        Iterator<Entry<MessageQueue, ProcessQueue>> it = processQueueTable.entrySet().iterator();
        while (it.hasNext()) {
            Entry<MessageQueue, ProcessQueue> next = it.next();
            ProcessQueue value = next.getValue();
            msgAccTotal += value.getMsgAccCnt();
        }

        return msgAccTotal;
    }

    public List<QueueTimeSpan> queryConsumeTimeSpan(final String topic)
            throws RemotingException, MQClientException, InterruptedException, MQBrokerException {
        List<QueueTimeSpan> queueTimeSpan = new ArrayList<QueueTimeSpan>();
        TopicRouteData routeData = this.mQClientFactory.getMQClientAPIImpl().getTopicRouteInfoFromNameServer(topic, 3000);
        for (BrokerData brokerData : routeData.getBrokerDatas()) {
            String addr = brokerData.selectBrokerAddr();
            queueTimeSpan.addAll(this.mQClientFactory.getMQClientAPIImpl().queryConsumeTimeSpan(addr, topic, groupName(), 3000));
        }

        return queueTimeSpan;
    }

    public ConsumeMessageService getConsumeMessageService() {
        return consumeMessageService;
    }

    public void setConsumeMessageService(ConsumeMessageService consumeMessageService) {
        this.consumeMessageService = consumeMessageService;

    }
}
