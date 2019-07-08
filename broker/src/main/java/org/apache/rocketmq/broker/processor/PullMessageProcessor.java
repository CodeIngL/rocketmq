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
package org.apache.rocketmq.broker.processor;

import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.FileRegion;
import java.nio.ByteBuffer;
import java.util.List;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.broker.client.ConsumerGroupInfo;
import org.apache.rocketmq.broker.filter.ConsumerFilterData;
import org.apache.rocketmq.broker.filter.ConsumerFilterManager;
import org.apache.rocketmq.broker.filter.ExpressionForRetryMessageFilter;
import org.apache.rocketmq.broker.filter.ExpressionMessageFilter;
import org.apache.rocketmq.broker.longpolling.PullRequest;
import org.apache.rocketmq.broker.mqtrace.ConsumeMessageContext;
import org.apache.rocketmq.broker.mqtrace.ConsumeMessageHook;
import org.apache.rocketmq.broker.pagecache.ManyMessageTransfer;
import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.common.TopicFilterType;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.filter.ExpressionType;
import org.apache.rocketmq.common.help.FAQUrl;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.protocol.ResponseCode;
import org.apache.rocketmq.common.protocol.header.PullMessageRequestHeader;
import org.apache.rocketmq.common.protocol.header.PullMessageResponseHeader;
import org.apache.rocketmq.common.protocol.heartbeat.SubscriptionData;
import org.apache.rocketmq.common.protocol.topic.OffsetMovedEvent;
import org.apache.rocketmq.common.subscription.SubscriptionGroupConfig;
import org.apache.rocketmq.remoting.common.RemotingUtil;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;
import org.apache.rocketmq.remoting.netty.NettyRequestProcessor;
import org.apache.rocketmq.remoting.netty.RequestTask;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.store.*;
import org.apache.rocketmq.store.config.BrokerRole;
import org.apache.rocketmq.store.config.MessageStoreConfig;
import org.apache.rocketmq.store.stats.BrokerStatsManager;

import static org.apache.rocketmq.common.MixAll.MASTER_ID;
import static org.apache.rocketmq.common.constant.PermName.isReadable;
import static org.apache.rocketmq.common.filter.ExpressionType.isTagType;
import static org.apache.rocketmq.common.filter.FilterAPI.build;
import static org.apache.rocketmq.common.help.FAQUrl.suggestTodo;
import static org.apache.rocketmq.common.protocol.ResponseCode.*;
import static org.apache.rocketmq.common.protocol.heartbeat.MessageModel.BROADCASTING;
import static org.apache.rocketmq.common.sysflag.PullSysFlag.hasCommitOffsetFlag;
import static org.apache.rocketmq.common.sysflag.PullSysFlag.hasSubscriptionFlag;
import static org.apache.rocketmq.common.sysflag.PullSysFlag.hasSuspendFlag;
import static org.apache.rocketmq.remoting.common.RemotingHelper.parseChannelRemoteAddr;
import static org.apache.rocketmq.remoting.protocol.RemotingCommand.createResponseCommand;
import static org.apache.rocketmq.remoting.protocol.RemotingSysResponseCode.SUCCESS;
import static org.apache.rocketmq.remoting.protocol.RemotingSysResponseCode.SYSTEM_ERROR;
import static org.apache.rocketmq.store.stats.BrokerStatsManager.COMMERCIAL_OWNER;
import static org.apache.rocketmq.store.stats.BrokerStatsManager.StatsType.RCV_EPOLLS;
import static org.apache.rocketmq.store.stats.BrokerStatsManager.StatsType.RCV_SUCCESS;

/**
 * 拉取消息处理器
 */
public class PullMessageProcessor implements NettyRequestProcessor {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.BROKER_LOGGER_NAME);
    private final BrokerController brokerController;
    private List<ConsumeMessageHook> consumeMessageHookList;

    public PullMessageProcessor(final BrokerController brokerController) {
        this.brokerController = brokerController;
    }

    @Override
    public RemotingCommand processRequest(final ChannelHandlerContext ctx, RemotingCommand req) throws RemotingCommandException {
        return this.processRequest(ctx.channel(), req, true);
    }

    @Override
    public boolean rejectRequest() {
        return false;
    }

    /**
     * 处理拉取消息的请求
     * @param channel
     * @param req
     * @param brokerAllowSuspend
     * @return
     * @throws RemotingCommandException
     */
    private RemotingCommand processRequest(final Channel channel, RemotingCommand req, boolean brokerAllowSuspend) throws RemotingCommandException {
        //构建相应命令
        RemotingCommand resp = createResponseCommand(PullMessageResponseHeader.class);
        //构建拉取消息响应命令的头部
        final PullMessageResponseHeader respHeader = (PullMessageResponseHeader) resp.readCustomHeader();
        //获得拉取消息请求的头部
        final PullMessageRequestHeader reqHeader = (PullMessageRequestHeader) req.decodeCommandCustomHeader(PullMessageRequestHeader.class);

        //设置特征值,请求
        resp.setOpaque(req.getOpaque());

        log.debug("receive PullMessage request command, {}", req);

        //校验不通过返回
        if (checkBrokerPermission(resp)) return resp;

        //消费组概念
        String consumerGroup = reqHeader.getConsumerGroup();
        //获得消费组对应的订阅组的配置概念
        SubscriptionGroupConfig subscriptionGroupConfig = this.brokerController.getSubscriptionGroupManager().findSubscriptionGroupConfig(consumerGroup);
        if (checkSubscriptionGroupConfig(resp, consumerGroup, subscriptionGroupConfig)) return resp;

        final boolean hasSuspendFlag = hasSuspendFlag(reqHeader.getSysFlag()); //支持挂起
        final boolean hasCommitOffsetFlag = hasCommitOffsetFlag(reqHeader.getSysFlag());//支持存在offset
        final boolean hasSubscriptionFlag = hasSubscriptionFlag(reqHeader.getSysFlag()); //支持订阅描述符

        final long suspendTimeoutMillisLong = hasSuspendFlag ? reqHeader.getSuspendTimeoutMillis() : 0; //如果支持挂起，我们得出挂起的超时时间

        //获得topic消息
        String topic =  reqHeader.getTopic();
        //获得topic的配置项
        TopicConfig topicConfig = this.brokerController.getTopicConfigManager().selectTopicConfig(topic);
        if (checkTopicConfig(channel, resp, topic, topicConfig)) return resp;

        //队列id非法，或者超过最大值
        int queueId = reqHeader.getQueueId();
        if (checkQueueId(channel, resp, topic, topicConfig, queueId)) return resp;

        String expressionType = reqHeader.getExpressionType();
        String subscription = reqHeader.getSubscription();
        Long subVersion = reqHeader.getSubVersion();

        //订阅的数据
        SubscriptionData subscriptionData;
        //消息过滤的数据
        ConsumerFilterData consumerFilterData = null;
        if (hasSubscriptionFlag) { //存在订阅标记，请求中带着相关订阅数据，我们从中华解析即可
            try {
                subscriptionData = build(topic, subscription, expressionType); //构建订阅的数据
                if (!isTagType(subscriptionData.getExpressionType())) { //是sql92类型的，或者其他的类型，tag不需要，因为tag是默认支持的
                    //订阅的相关数据，可以构建我们过滤消息的核心数据
                    consumerFilterData = ConsumerFilterManager.build(topic, consumerGroup, subscription, expressionType, subVersion);
                    assert consumerFilterData != null;
                }
            } catch (Exception e) {
                log.warn("Parse the consumer's subscription[{}] failed, group: {}", subscription, consumerGroup);
                resp.setCode(SUBSCRIPTION_PARSE_FAILED);
                resp.setRemark("parse the consumer's subscription failed");
                return resp;
            }
        } else {
            //消费组
            ConsumerGroupInfo consumerGroupInfo = this.brokerController.getConsumerManager().getConsumerGroupInfo(consumerGroup);
            if (null == consumerGroupInfo) { //消费组部存在
                log.warn("the consumer's group info not exist, group: {}", consumerGroup);
                resp.setCode(SUBSCRIPTION_NOT_EXIST);
                resp.setRemark("the consumer's group info not exist" + suggestTodo(FAQUrl.SAME_GROUP_DIFFERENT_TOPIC));
                return resp;
            }

            if (!subscriptionGroupConfig.isConsumeBroadcastEnable() && (consumerGroupInfo.getMessageModel() == BROADCASTING)) { //广播的检查
                resp.setCode(NO_PERMISSION);
                resp.setRemark("the consumer group[" + consumerGroup + "] can not consume by broadcast way");
                return resp;
            }

            //可以从消息组获得订阅的相关消息，如果存在订阅关系，我们知道是构建了相关的订阅数据
            subscriptionData = consumerGroupInfo.findSubscriptionData(topic);
            if (null == subscriptionData) {//如果不提供订阅标记，消息组必须在broker中获得相关订阅关系，否则，出错
                log.warn("the consumer's subscription not exist, group: {}, topic:{}", consumerGroup, topic);
                resp.setCode(SUBSCRIPTION_NOT_EXIST);
                resp.setRemark("the consumer's subscription not exist" + suggestTodo(FAQUrl.SAME_GROUP_DIFFERENT_TOPIC));
                return resp;
            }

            if (subscriptionData.getSubVersion() < subVersion) { //版本问题
                log.warn("The broker's subscription is not latest, group: {} {}", consumerGroup, subscriptionData.getSubString());
                resp.setCode(SUBSCRIPTION_NOT_LATEST);
                resp.setRemark("the consumer's subscription not latest");
                return resp;
            }
            if (!isTagType(subscriptionData.getExpressionType())) { //不是tag订阅，同理我们要获得相关消费过滤数据，如果不存在，则会报错，前面我们知道，如果是存在订阅标记，我们是通过构建处理
                //没有订阅数据是非法的，我们不使用远程请求而是使用broker内部存储相关的数据，我们需要获得有订阅数据构成消费过滤数据信息
                consumerFilterData = this.brokerController.getConsumerFilterManager().get(topic, consumerGroup);
                if (consumerFilterData == null) {
                    resp.setCode(FILTER_DATA_NOT_EXIST);
                    resp.setRemark("The broker's consumer filter data is not exist!Your expression may be wrong!");
                    return resp;
                }
                Long clientVersion = consumerFilterData.getClientVersion();
                if (clientVersion < subVersion) {
                    log.warn("The broker's consumer filter data is not latest, group: {}, topic: {}, serverV: {}, clientV: {}",
                        consumerGroup, topic, clientVersion, subVersion);
                    resp.setCode(FILTER_DATA_NOT_LATEST);
                    resp.setRemark("the consumer's consumer filter data not latest");
                    return resp;
                }
            }
        }

        if (!isTagType(subscriptionData.getExpressionType()) && !this.brokerController.getBrokerConfig().isEnablePropertyFilter()) {
            resp.setRemark("The broker does not support consumer to filter message by " + subscriptionData.getExpressionType());
            return resp;
        }

        //消息过滤器
        MessageFilter messageFilter;
        if (this.brokerController.getBrokerConfig().isFilterSupportRetry()) { //是否支持重试
            messageFilter = new ExpressionForRetryMessageFilter(subscriptionData, consumerFilterData, this.brokerController.getConsumerFilterManager()); //支持重试
        } else {
            messageFilter = new ExpressionMessageFilter(subscriptionData, consumerFilterData, this.brokerController.getConsumerFilterManager()); //不支持重试
        }

        BrokerConfig brokerConfig = this.brokerController.getBrokerConfig();
        MessageStoreConfig storeConfig = this.brokerController.getMessageStoreConfig();
        MessageStore messageStore = this.brokerController.getMessageStore();
        BrokerRole brokerRole = storeConfig.getBrokerRole();
        //消息结果
        final GetMessageResult result = messageStore.getMessage(consumerGroup, topic, queueId, reqHeader.getQueueOffset(), reqHeader.getMaxMsgNums(), messageFilter);
        if (result != null) {
            resp.setRemark(result.getStatus().name()); //状态
            respHeader.setNextBeginOffset(result.getNextBeginOffset()); //下一次开始的offset
            respHeader.setMinOffset(result.getMinOffset());//最小的offset
            respHeader.setMaxOffset(result.getMaxOffset()); //最大的offset

            if (result.isSuggestPullingFromSlave()) { //建议从slave去消费
                respHeader.setSuggestWhichBrokerId(subscriptionGroupConfig.getWhichBrokerWhenConsumeSlowly()); //消费满，建议去其他节点
            } else { //masterId
                respHeader.setSuggestWhichBrokerId(MASTER_ID);
            }

            switch (brokerRole) {
                case ASYNC_MASTER:
                case SYNC_MASTER:
                    break;
                case SLAVE:
                    if (!brokerConfig.isSlaveReadEnable()) {
                        resp.setCode(PULL_RETRY_IMMEDIATELY);
                        respHeader.setSuggestWhichBrokerId(MASTER_ID);
                    }
                    break;
            }

            if (brokerConfig.isSlaveReadEnable()) { //如果指出的slave读
                // consume too slow ,redirect to another machine
                // 消费太慢了，重定向到其他的机器上
                if (result.isSuggestPullingFromSlave()) {
                    respHeader.setSuggestWhichBrokerId(subscriptionGroupConfig.getWhichBrokerWhenConsumeSlowly()); //消费满，建议去其他节点
                }
                // consume ok
                else {
                    respHeader.setSuggestWhichBrokerId(subscriptionGroupConfig.getBrokerId());
                }
            } else {
                respHeader.setSuggestWhichBrokerId(MASTER_ID);
            }

            //校验结果
            switch (result.getStatus()) {
                case FOUND:
                    resp.setCode(SUCCESS);
                    break;
                case MESSAGE_WAS_REMOVING:
                    resp.setCode(PULL_RETRY_IMMEDIATELY);
                    break;
                case NO_MATCHED_LOGIC_QUEUE:
                case NO_MESSAGE_IN_QUEUE:
                    if (0 != reqHeader.getQueueOffset()) {
                        resp.setCode(PULL_OFFSET_MOVED);

                        // XXX: warn and notify me
                        log.info("the broker store no queue data, fix the request offset {} to {}, Topic: {} QueueId: {} Consumer Group: {}",
                            reqHeader.getQueueOffset(),
                            result.getNextBeginOffset(),
                            topic,
                            queueId,
                            consumerGroup
                        );
                    } else {
                        resp.setCode(PULL_NOT_FOUND);
                    }
                    break;
                case NO_MATCHED_MESSAGE:
                    resp.setCode(PULL_RETRY_IMMEDIATELY);
                    break;
                case OFFSET_FOUND_NULL:
                    resp.setCode(PULL_NOT_FOUND);
                    break;
                case OFFSET_OVERFLOW_BADLY:
                    resp.setCode(PULL_OFFSET_MOVED);
                    // XXX: warn and notify me
                    log.info("the request offset: {} over flow badly, broker max offset: {}, consumer: {}",
                        reqHeader.getQueueOffset(), result.getMaxOffset(), channel.remoteAddress());
                    break;
                case OFFSET_OVERFLOW_ONE:
                    resp.setCode(PULL_NOT_FOUND);
                    break;
                case OFFSET_TOO_SMALL:
                    resp.setCode(PULL_OFFSET_MOVED);
                    log.info("the request offset too small. group={}, topic={}, requestOffset={}, brokerMinOffset={}, clientIp={}",
                        consumerGroup, topic, reqHeader.getQueueOffset(), result.getMinOffset(), channel.remoteAddress());
                    break;
                default:
                    assert false;
                    break;
            }

            //存在钩子
            if (this.hasConsumeMessageHook()) {
                //构建上下文
                ConsumeMessageContext context = new ConsumeMessageContext();
                context.setConsumerGroup(consumerGroup);
                context.setTopic(topic);
                context.setQueueId(queueId);

                String owner = req.getExtFields().get(COMMERCIAL_OWNER);

                switch (resp.getCode()) {
                    case SUCCESS:
                        int commercialBaseCount = brokerConfig.getCommercialBaseCount();
                        int incValue = result.getMsgCount4Commercial() * commercialBaseCount;

                        context.setCommercialRcvStats(RCV_SUCCESS);
                        context.setCommercialRcvTimes(incValue);
                        context.setCommercialRcvSize(result.getBufferTotalSize());
                        context.setCommercialOwner(owner);

                        break;
                    case PULL_NOT_FOUND:
                        if (!brokerAllowSuspend) {
                            context.setCommercialRcvStats(RCV_EPOLLS);
                            context.setCommercialRcvTimes(1);
                            context.setCommercialOwner(owner);
                        }
                        break;
                    case PULL_RETRY_IMMEDIATELY:
                    case PULL_OFFSET_MOVED:
                        context.setCommercialRcvStats(RCV_EPOLLS);
                        context.setCommercialRcvTimes(1);
                        context.setCommercialOwner(owner);
                        break;
                    default:
                        assert false;
                        break;
                }

                this.executeConsumeMessageHookBefore(context);
            }

            //统计
            switch (resp.getCode()) {
                case SUCCESS:
                    this.brokerController.getBrokerStatsManager().incGroupGetNums(consumerGroup, topic, result.getMessageCount());
                    this.brokerController.getBrokerStatsManager().incGroupGetSize(consumerGroup, topic, result.getBufferTotalSize());
                    this.brokerController.getBrokerStatsManager().incBrokerGetNums(result.getMessageCount());
                    if (brokerConfig.isTransferMsgByHeap()) { //使用堆内进行发送
                        final long beginTimeMills = messageStore.now();
                        final byte[] r = this.readGetMessageResult(result, consumerGroup, topic, queueId);
                        this.brokerController.getBrokerStatsManager().incGroupGetLatency(consumerGroup, topic, queueId, (int) (messageStore.now() - beginTimeMills));
                        resp.setBody(r);
                    } else { //直接发送
                        try {
                            FileRegion fileRegion = new ManyMessageTransfer(resp.encodeHeader(result.getBufferTotalSize()), result);
                            channel.writeAndFlush(fileRegion).addListener((ChannelFutureListener) future -> {
                                result.release();
                                if (!future.isSuccess()) {
                                    log.error("transfer many message by pagecache failed, {}", channel.remoteAddress(), future.cause());
                                }
                            });
                        } catch (Throwable e) {
                            log.error("transfer many message by pagecache exception", e);
                            result.release();
                        }

                        resp = null;
                    }
                    break;
                case PULL_NOT_FOUND: //当消息没有的时候，我们通过构造一个被短暂挂起的请求，来让后端的线程进行触发，回写

                    if (brokerAllowSuspend && hasSuspendFlag) { //broker支持挂起的请求，并且请求明确指出这是一个可以被挂起的请求
                        long pollingTimeMills = suspendTimeoutMillisLong;
                        if (!brokerConfig.isLongPollingEnable()) {//broker支持长轮训
                            pollingTimeMills = brokerConfig.getShortPollingTimeMills(); //设置的短轮训
                        }

                        long offset = reqHeader.getQueueOffset();
                        PullRequest pullRequest = new PullRequest(req, channel, pollingTimeMills, messageStore.now(), offset, subscriptionData, messageFilter);
                        this.brokerController.getPullRequestHoldService().suspendPullRequest(topic, queueId, pullRequest); //添加到挂起的请求
                        resp = null;
                        break;
                    }

                case PULL_RETRY_IMMEDIATELY:
                    break;
                case PULL_OFFSET_MOVED:
                    if (brokerRole != BrokerRole.SLAVE || storeConfig.isOffsetCheckInSlave()) {
                        MessageQueue mq = new MessageQueue();
                        mq.setTopic(topic);
                        mq.setQueueId(queueId);
                        mq.setBrokerName(brokerConfig.getBrokerName());

                        OffsetMovedEvent event = new OffsetMovedEvent();
                        event.setConsumerGroup(consumerGroup);
                        event.setMessageQueue(mq);
                        event.setOffsetRequest(reqHeader.getQueueOffset());
                        event.setOffsetNew(result.getNextBeginOffset());
                        this.generateOffsetMovedEvent(event);
                        log.warn("PULL_OFFSET_MOVED:correction offset. topic={}, groupId={}, requestOffset={}, newOffset={}, suggestBrokerId={}",
                            topic, consumerGroup, event.getOffsetRequest(), event.getOffsetNew(), respHeader.getSuggestWhichBrokerId());
                    } else {
                        respHeader.setSuggestWhichBrokerId(subscriptionGroupConfig.getBrokerId());
                        resp.setCode(PULL_RETRY_IMMEDIATELY);
                        log.warn("PULL_OFFSET_MOVED:none correction. topic={}, groupId={}, requestOffset={}, suggestBrokerId={}",
                                consumerGroup, consumerGroup, reqHeader.getQueueOffset(),
                            respHeader.getSuggestWhichBrokerId());
                    }

                    break;
                default:
                    assert false;
            }
        } else {
            resp.setRemark("store getMessage return null");
        }

        boolean storeOffsetEnable = brokerAllowSuspend; //broker允许挂起
        storeOffsetEnable = storeOffsetEnable && hasCommitOffsetFlag; //存在commitOffset
        storeOffsetEnable = storeOffsetEnable && brokerRole != BrokerRole.SLAVE; //不是slave
        if (storeOffsetEnable) {
            this.brokerController.getConsumerOffsetManager().commitOffset(parseChannelRemoteAddr(channel),
                consumerGroup, topic, queueId, reqHeader.getCommitOffset());
        }
        //返回响应
        return resp;
    }

    private boolean checkQueueId(Channel channel, RemotingCommand resp, String topic, TopicConfig topicConfig, int queueId) {
        if (queueId < 0 || queueId >= topicConfig.getReadQueueNums()) {
            String errorInfo = String.format("queueId[%d] is illegal, topic:[%s] topicConfig.readQueueNums:[%d] consumer:[%s]", queueId, topic, topicConfig.getReadQueueNums(), channel.remoteAddress());
            log.warn(errorInfo);
            resp.setCode(SYSTEM_ERROR);
            resp.setRemark(errorInfo);
            return true;
        }
        return false;
    }

    private boolean checkTopicConfig(Channel channel, RemotingCommand resp, String topic, TopicConfig topicConfig) {
        if (null == topicConfig) {
            log.error("the topic {} not exist, consumer: {}",topic, parseChannelRemoteAddr(channel));
            resp.setCode(TOPIC_NOT_EXIST);
            resp.setRemark(String.format("topic[%s] not exist, apply first please! %s", topic, suggestTodo(FAQUrl.APPLY_TOPIC_URL)));
            return true;
        }

        if (!isReadable(topicConfig.getPerm())) {
            resp.setCode(NO_PERMISSION);
            resp.setRemark("the topic[" + topic + "] pulling message is forbidden");
            return true;
        }
        return false;
    }

    private boolean checkSubscriptionGroupConfig(RemotingCommand resp, String consumerGroup, SubscriptionGroupConfig subscriptionGroupConfig) {
        if (null == subscriptionGroupConfig) {
            resp.setCode(SUBSCRIPTION_GROUP_NOT_EXIST);
            resp.setRemark(String.format("subscription group [%s] does not exist, %s", consumerGroup, suggestTodo(FAQUrl.SUBSCRIPTION_GROUP_NOT_EXIST)));
            return true;
        }

        //未开启消费
        if (!subscriptionGroupConfig.isConsumeEnable()) {
            resp.setCode(NO_PERMISSION);
            resp.setRemark("subscription group no permission, " + consumerGroup);
            return true;
        }
        return false;
    }

    /**
     * 校验broker的权限
     * @param resp
     * @return
     */
    private boolean checkBrokerPermission(RemotingCommand resp) {
        BrokerConfig brokerConfig = this.brokerController.getBrokerConfig();
        if (!isReadable(brokerConfig.getBrokerPermission())) {
            resp.setCode(NO_PERMISSION);
            resp.setRemark(String.format("the broker[%s] pulling message is forbidden", brokerConfig.getBrokerIP1()));
            return true;
        }
        return false;
    }

    public boolean hasConsumeMessageHook() {
        return consumeMessageHookList != null && !this.consumeMessageHookList.isEmpty();
    }

    public void executeConsumeMessageHookBefore(final ConsumeMessageContext context) {
        if (hasConsumeMessageHook()) {
            for (ConsumeMessageHook hook : this.consumeMessageHookList) {
                try {
                    hook.consumeMessageBefore(context);
                } catch (Throwable e) {
                }
            }
        }
    }

    /**
     * 进行一次copy使得堆外内存转换为堆内分配的内存
     * @param getMessageResult
     * @param group
     * @param topic
     * @param queueId
     * @return
     */
    private byte[] readGetMessageResult(final GetMessageResult getMessageResult, final String group, final String topic, final int queueId) {
        final ByteBuffer byteBuffer = ByteBuffer.allocate(getMessageResult.getBufferTotalSize());

        long storeTimestamp = 0;
        try {
            List<ByteBuffer> messageBufferList = getMessageResult.getMessageBufferList();
            for (ByteBuffer bb : messageBufferList) {
                byteBuffer.put(bb);
                storeTimestamp = bb.getLong(MessageDecoder.MESSAGE_STORE_TIMESTAMP_POSTION);
            }
        } finally {
            getMessageResult.release();
        }

        this.brokerController.getBrokerStatsManager().recordDiskFallBehindTime(group, topic, queueId, this.brokerController.getMessageStore().now() - storeTimestamp);
        return byteBuffer.array();
    }

    /**
     * 生成offset移动事件
     * @param event
     */
    private void generateOffsetMovedEvent(final OffsetMovedEvent event) {
        try {
            MessageExtBrokerInner msgInner = new MessageExtBrokerInner();
            msgInner.setTopic(MixAll.OFFSET_MOVED_EVENT);
            msgInner.setTags(event.getConsumerGroup());
            msgInner.setDelayTimeLevel(0);
            msgInner.setKeys(event.getConsumerGroup());
            msgInner.setBody(event.encode());
            msgInner.setFlag(0);
            msgInner.setPropertiesString(MessageDecoder.messageProperties2String(msgInner.getProperties()));
            msgInner.setTagsCode(MessageExtBrokerInner.tagsString2tagsCode(TopicFilterType.SINGLE_TAG, msgInner.getTags()));

            msgInner.setQueueId(0);
            msgInner.setSysFlag(0);
            msgInner.setBornTimestamp(System.currentTimeMillis());
            msgInner.setBornHost(RemotingUtil.string2SocketAddress(this.brokerController.getBrokerAddr()));
            msgInner.setStoreHost(msgInner.getBornHost());

            msgInner.setReconsumeTimes(0);

            this.brokerController.getMessageStore().putMessage(msgInner);
        } catch (Exception e) {
            log.warn(String.format("generateOffsetMovedEvent Exception, %s", event.toString()), e);
        }
    }

    /**
     * 执行并返回连接
     * @param channel
     * @param req
     * @throws RemotingCommandException
     */
    public void executeRequestWhenWakeup(final Channel channel, final RemotingCommand req) throws RemotingCommandException {
        Runnable run = () -> {
            try {
                final RemotingCommand resp = PullMessageProcessor.this.processRequest(channel, req, false);
                if (resp == null){
                    return;
                }
                resp.setOpaque(req.getOpaque());
                resp.markResponseType();
                try {
                    channel.writeAndFlush(resp).addListener((ChannelFutureListener) future -> {
                        if (!future.isSuccess()) {
                            log.error("processRequestWrapper response to {} failed", future.channel().remoteAddress(), future.cause());
                            log.error(req.toString());
                            log.error(resp.toString());
                        }
                    });
                } catch (Throwable e) {
                    log.error("processRequestWrapper process request over, but response failed", e);
                    log.error(req.toString());
                    log.error(resp.toString());
                }
            } catch (RemotingCommandException e1) {
                log.error("excuteRequestWhenWakeup run", e1);
            }
        };
        //提交任务到线程池中执行
        this.brokerController.getPullMessageExecutor().submit(new RequestTask(run, channel, req));
    }

    public void registerConsumeMessageHook(List<ConsumeMessageHook> sendMessageHookList) {
        this.consumeMessageHookList = sendMessageHookList;
    }
}
