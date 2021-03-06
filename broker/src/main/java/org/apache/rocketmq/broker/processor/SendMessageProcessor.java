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

import io.netty.channel.ChannelHandlerContext;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.broker.mqtrace.ConsumeMessageContext;
import org.apache.rocketmq.broker.mqtrace.ConsumeMessageHook;
import org.apache.rocketmq.broker.mqtrace.SendMessageContext;
import org.apache.rocketmq.common.*;
import org.apache.rocketmq.common.constant.PermName;
import org.apache.rocketmq.common.help.FAQUrl;
import org.apache.rocketmq.common.message.MessageAccessor;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageExtBatch;
import org.apache.rocketmq.common.protocol.RequestCode;
import org.apache.rocketmq.common.protocol.ResponseCode;
import org.apache.rocketmq.common.protocol.header.ConsumerSendMsgBackRequestHeader;
import org.apache.rocketmq.common.protocol.header.SendMessageRequestHeader;
import org.apache.rocketmq.common.protocol.header.SendMessageResponseHeader;
import org.apache.rocketmq.common.subscription.SubscriptionGroupConfig;
import org.apache.rocketmq.common.sysflag.MessageSysFlag;
import org.apache.rocketmq.common.sysflag.TopicSysFlag;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;
import org.apache.rocketmq.remoting.netty.NettyRequestProcessor;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.store.MessageExtBrokerInner;
import org.apache.rocketmq.store.PutMessageResult;
import org.apache.rocketmq.store.config.StorePathConfigHelper;
import org.apache.rocketmq.store.stats.BrokerStatsManager;

import java.net.SocketAddress;
import java.util.List;
import java.util.Map;

import static org.apache.rocketmq.common.help.FAQUrl.suggestTodo;
import static org.apache.rocketmq.common.message.MessageConst.PROPERTY_MSG_REGION;
import static org.apache.rocketmq.common.message.MessageConst.PROPERTY_TRACE_SWITCH;
import static org.apache.rocketmq.common.message.MessageDecoder.string2messageProperties;
import static org.apache.rocketmq.common.protocol.RequestCode.CONSUMER_SEND_MSG_BACK;
import static org.apache.rocketmq.remoting.protocol.RemotingCommand.createResponseCommand;
import static org.apache.rocketmq.remoting.protocol.RemotingSysResponseCode.SYSTEM_ERROR;

/**
 * 远程发送请求，对应的处理器相关解析
 */
public class SendMessageProcessor extends AbstractSendMessageProcessor implements NettyRequestProcessor {

    //消息重发的hook
    private List<ConsumeMessageHook> consumeMessageHookList;

    public SendMessageProcessor(final BrokerController brokerController) {
        super(brokerController);
    }


    /**
     * 处理请求
     * @param ctx
     * @param req
     * @return
     * @throws RemotingCommandException
     */
    @Override
    public RemotingCommand processRequest(ChannelHandlerContext ctx, RemotingCommand req) throws RemotingCommandException {
        switch (req.getCode()) {
            case CONSUMER_SEND_MSG_BACK: //消息重发
                return this.consumerSendMsgBack(ctx, req);
            default:
                SendMessageRequestHeader reqHeader = parseRequestHeader(req);//解析消息头部
                if (reqHeader == null) {
                    return null;
                }

                //上下文
                SendMessageContext context = buildMsgContext(ctx, reqHeader);
                this.executeSendMessageHookBefore(ctx, req, context);//先执行钩子before，里面存在不必要的代码
                RemotingCommand resp = reqHeader.isBatch() ? sendBatchMessage(ctx, req, context, reqHeader) : sendMessage(ctx, req, context, reqHeader);
                this.executeSendMessageHookAfter(resp, context);//再执行钩子after，里面存在着不必要的代码
                return resp;
        }
    }

    @Override
    public boolean rejectRequest() {
        return this.brokerController.getMessageStore().isOSPageCacheBusy() ||
            this.brokerController.getMessageStore().isTransientStorePoolDeficient();
    }

    /**
     * 处理消费端要求的消息重发，钩子在这里执行
     * @param ctx
     * @param req
     * @return
     * @throws RemotingCommandException
     */
    private RemotingCommand consumerSendMsgBack(final ChannelHandlerContext ctx, final RemotingCommand req)
        throws RemotingCommandException {
        final RemotingCommand resp = createResponseCommand(null);
        final ConsumerSendMsgBackRequestHeader header = (ConsumerSendMsgBackRequestHeader)req.decodeCommandCustomHeader(ConsumerSendMsgBackRequestHeader.class);

        if (this.hasConsumeMessageHook() && !UtilAll.isBlank(header.getOriginMsgId())) { //
            ConsumeMessageContext context = new ConsumeMessageContext();
            context.setConsumerGroup(header.getGroup());
            context.setTopic(header.getOriginTopic());
            context.setCommercialRcvStats(BrokerStatsManager.StatsType.SEND_BACK);
            context.setCommercialRcvTimes(1);
            context.setCommercialOwner(req.getExtFields().get(BrokerStatsManager.COMMERCIAL_OWNER));
            this.executeConsumeMessageHookAfter(context); //执行hook后，之前存在hook前，在拉取消息的时候先执行，这里将处理拉取后进行消息重发的操作
        }

        //获得内存中的订阅关系
        SubscriptionGroupConfig subscriptionGroupConfig = this.brokerController.getSubscriptionGroupManager().findSubscriptionGroupConfig(header.getGroup());
        if (null == subscriptionGroupConfig) {
            resp.setCode(ResponseCode.SUBSCRIPTION_GROUP_NOT_EXIST);
            resp.setRemark("subscription group not exist, " + header.getGroup() + " "
                + suggestTodo(FAQUrl.SUBSCRIPTION_GROUP_NOT_EXIST));
            return resp;
        }

        if (!PermName.isWriteable(this.brokerController.getBrokerConfig().getBrokerPermission())) {
            resp.setCode(ResponseCode.NO_PERMISSION);
            resp.setRemark("the broker[" + this.brokerController.getBrokerConfig().getBrokerIP1() + "] sending message is forbidden");
            return resp;
        }

        if (subscriptionGroupConfig.getRetryQueueNums() <= 0) {
            resp.setCode(ResponseCode.SUCCESS);
            resp.setRemark(null);
            return resp;
        }

        String newTopic = MixAll.getRetryTopic(header.getGroup()); //构建重试topic也就是，重试前缀加上真是topic
        int queueIdInt = Math.abs(this.random.nextInt() % 99999999) % subscriptionGroupConfig.getRetryQueueNums(); //选取相关的queueId

        int topicSysFlag = 0;
        if (header.isUnitMode()) {
            topicSysFlag = TopicSysFlag.buildSysFlag(false, true);
        }

        //构建消费重发topic
        TopicConfig topicConfig = this.brokerController.getTopicConfigManager().createTopicInSendMessageBackMethod(
            newTopic,
            subscriptionGroupConfig.getRetryQueueNums(),
            PermName.PERM_WRITE | PermName.PERM_READ, topicSysFlag);
        if (null == topicConfig) {
            resp.setCode(SYSTEM_ERROR);
            resp.setRemark("topic[" + newTopic + "] not exist");
            return resp;
        }

        if (!PermName.isWriteable(topicConfig.getPerm())) {
            resp.setCode(ResponseCode.NO_PERMISSION);
            resp.setRemark(String.format("the topic[%s] sending message is forbidden", newTopic));
            return resp;
        }

        //根据提供的offset。我们直接寻找相关的消息，
        MessageExt msgExt = this.brokerController.getMessageStore().lookMessageByOffset(header.getOffset());
        if (null == msgExt) {
            resp.setCode(SYSTEM_ERROR);
            resp.setRemark("look message by offset failed, " + header.getOffset());
            return resp;
        }

        final String retryTopic = msgExt.getProperty(MessageConst.PROPERTY_RETRY_TOPIC); //获得一下重试的topic
        if (null == retryTopic) {
            MessageAccessor.putProperty(msgExt, MessageConst.PROPERTY_RETRY_TOPIC, msgExt.getTopic());
        }
        msgExt.setWaitStoreMsgOK(false);

        int delayLevel = header.getDelayLevel(); //延迟级别

        int maxReconsumeTimes = subscriptionGroupConfig.getRetryMaxTimes();
        if (req.getVersion() >= MQVersion.Version.V3_4_9.ordinal()) {
            maxReconsumeTimes = header.getMaxReconsumeTimes();
        }

        if (msgExt.getReconsumeTimes() >= maxReconsumeTimes || delayLevel < 0) { //重发次数到达最大的次数，或者延时小于0,发送到死信队列中
            newTopic = MixAll.getDLQTopic(header.getGroup());
            queueIdInt = Math.abs(this.random.nextInt() % 99999999) % DLQ_NUMS_PER_GROUP;

            topicConfig = this.brokerController.getTopicConfigManager().createTopicInSendMessageBackMethod(newTopic, DLQ_NUMS_PER_GROUP, PermName.PERM_WRITE, 0);
            if (null == topicConfig) {
                resp.setRemark("topic[" + newTopic + "] not exist");
                return resp;
            }
        } else {
            if (0 == delayLevel) {
                delayLevel = 3 + msgExt.getReconsumeTimes();
            }

            msgExt.setDelayTimeLevel(delayLevel);
        }

        MessageExtBrokerInner msgInner = new MessageExtBrokerInner();
        msgInner.setTopic(newTopic);
        msgInner.setBody(msgExt.getBody());
        msgInner.setFlag(msgExt.getFlag());
        MessageAccessor.setProperties(msgInner, msgExt.getProperties());
        msgInner.setPropertiesString(MessageDecoder.messageProperties2String(msgExt.getProperties()));
        msgInner.setTagsCode(MessageExtBrokerInner.tagsString2tagsCode(null, msgExt.getTags()));

        msgInner.setQueueId(queueIdInt);
        msgInner.setSysFlag(msgExt.getSysFlag());
        msgInner.setBornTimestamp(msgExt.getBornTimestamp());
        msgInner.setBornHost(msgExt.getBornHost());
        msgInner.setStoreHost(this.getStoreHost());
        msgInner.setReconsumeTimes(msgExt.getReconsumeTimes() + 1);

        String originMsgId = MessageAccessor.getOriginMessageId(msgExt);
        MessageAccessor.setOriginMessageId(msgInner, UtilAll.isBlank(originMsgId) ? msgExt.getMsgId() : originMsgId);

        PutMessageResult result = this.brokerController.getMessageStore().putMessage(msgInner);
        if (result != null) {
            switch (result.getPutMessageStatus()) {
                case PUT_OK:
                    String backTopic = msgExt.getTopic();
                    String correctTopic = msgExt.getProperty(MessageConst.PROPERTY_RETRY_TOPIC);
                    if (correctTopic != null) {
                        backTopic = correctTopic;
                    }
                    this.brokerController.getBrokerStatsManager().incSendBackNums(header.getGroup(), backTopic);
                    resp.setCode(ResponseCode.SUCCESS);
                    resp.setRemark(null);
                    return resp;
                default:
                    break;
            }
            resp.setRemark(result.getPutMessageStatus().name());
            return resp;
        }
        resp.setRemark("putMessageResult is null");
        return resp;
    }

    /**
     * 是否能处理消息重发和死信消息
     * @param reqHeader
     * @param response
     * @param req
     * @param msg
     * @param topicConfig
     * @return
     */
    private boolean handleRetryAndDLQ(SendMessageRequestHeader reqHeader, RemotingCommand response,
                                      RemotingCommand req,
                                      MessageExt msg, TopicConfig topicConfig) {
        // topic
        String newTopic = reqHeader.getTopic();
        // 消息重发，属于消息重发
        if (null != newTopic && newTopic.startsWith(MixAll.RETRY_GROUP_TOPIC_PREFIX)) {
            //提取重发消息中的消费组
            String groupName = newTopic.substring(MixAll.RETRY_GROUP_TOPIC_PREFIX.length());
            //获得消费组订阅配置信息
            SubscriptionGroupConfig subscriptionGroupConfig = this.brokerController.getSubscriptionGroupManager().findSubscriptionGroupConfig(groupName);

            if (null == subscriptionGroupConfig) {
                //消费组没有相关的订阅配置，我们返回
                response.setCode(ResponseCode.SUBSCRIPTION_GROUP_NOT_EXIST);
                response.setRemark("subscription group not exist, " + groupName + " " + suggestTodo(FAQUrl.SUBSCRIPTION_GROUP_NOT_EXIST));
                return false;
            }

            //最大重发次数
            int maxReconsumeTimes = subscriptionGroupConfig.getRetryMaxTimes();
            if (req.getVersion() >= MQVersion.Version.V3_4_9.ordinal()) {
                //可以设定的重发次数
                maxReconsumeTimes = reqHeader.getMaxReconsumeTimes();
            }
            //提取已经重试次数
            int reconsumeTimes = reqHeader.getReconsumeTimes() == null ? 0 : reqHeader.getReconsumeTimes();
            if (reconsumeTimes >= maxReconsumeTimes) {
                //大于最大消息重试次数，让我们放置到死信队列
                newTopic = MixAll.getDLQTopic(groupName);
                //构建死信队列topic
                int queueIdInt = Math.abs(this.random.nextInt() % 99999999) % DLQ_NUMS_PER_GROUP;
                //构建或拿到队列对应的死信队列的配置
                topicConfig = this.brokerController.getTopicConfigManager().createTopicInSendMessageBackMethod(newTopic, DLQ_NUMS_PER_GROUP, PermName.PERM_WRITE, 0);
                msg.setTopic(newTopic);
                msg.setQueueId(queueIdInt);
                if (null == topicConfig) {
                    //topic配置信息不存在
                    response.setCode(SYSTEM_ERROR);
                    response.setRemark("topic[" + newTopic + "] not exist");
                    return false;
                }
            }
        }
        int sysFlag = reqHeader.getSysFlag();
        if (TopicFilterType.MULTI_TAG == topicConfig.getTopicFilterType()) {
            sysFlag |= MessageSysFlag.MULTI_TAGS_FLAG;
        }
        msg.setSysFlag(sysFlag);
        return true;
    }

    /**
     * 发送单个消息、处理来着produce发送过啦的消息，我们的代理服务器， 需要处理这些消息
     * @param ctx
     * @param req
     * @param context
     * @param reqHeader
     * @return
     * @throws RemotingCommandException
     */
    private RemotingCommand sendMessage(final ChannelHandlerContext ctx,
                                        final RemotingCommand req,
                                        final SendMessageContext context,
                                        final SendMessageRequestHeader reqHeader) throws RemotingCommandException {

        //构建响应命令，用于返回给客户端
        final RemotingCommand resp = createResponseCommand(SendMessageResponseHeader.class);

        //构造响应的头部
        final SendMessageResponseHeader respHeader = (SendMessageResponseHeader)resp.readCustomHeader();

        //设置唯一id
        resp.setOpaque(req.getOpaque());

        //该broker的配置信息
        BrokerConfig brokerConfig = this.brokerController.getBrokerConfig();

        //支持regionId，支持TraceID
        //添加消息所属的regioin，broker使用regionid来区分集群下不同的字节,tip这里存在一个regionId
        resp.addExtField(PROPERTY_MSG_REGION, brokerConfig.getRegionId());
        //添加追踪是否开启字段，broker配置文件支持
        resp.addExtField(PROPERTY_TRACE_SWITCH, String.valueOf(brokerConfig.isTraceOn()));



        log.debug("receive SendMessage request command, {}", req);

        //收到发送消息的开始时间
        final long startTimestamp = brokerConfig.getStartAcceptSendRequestTimeStamp();
        if (this.brokerController.getMessageStore().now() < startTimestamp) {//无法服务，收到消息的时间是未来的
            resp.setCode(SYSTEM_ERROR);
            resp.setRemark(String.format("broker unable to service, until %s", UtilAll.timeMillisToHumanString2(startTimestamp)));
            return resp;
        }

        resp.setCode(-1);
        super.msgCheck(ctx, reqHeader, resp);
        //校验不通过，返回producer响应
        if (resp.getCode() != -1) {
            return resp;
        }


        //获得消息对应的topic的配置信息
        String topic = reqHeader.getTopic();
        //获得topic的配置信息
        TopicConfig topicConfig = this.brokerController.getTopicConfigManager().selectTopicConfig(topic);

        //获得queueId，总是一个数字
        int queueId = reqHeader.getQueueId();
        if (queueId < 0) {
            //在可选的写队列中随机选择一个进行写入
            queueId = Math.abs(this.random.nextInt() % 99999999) % topicConfig.getWriteQueueNums();
        }

        //消息转换为broker内部流程的实例
        MessageExtBrokerInner msgInner = new MessageExtBrokerInner();
        //设置主题
        msgInner.setTopic(topic);
        //设置queueId
        msgInner.setQueueId(queueId);

        //不能处理重发消息和死信消息，返回
        if (!handleRetryAndDLQ(reqHeader, resp, req, msgInner, topicConfig)) {
            return resp;
        }

        //设置body
        msgInner.setBody(req.getBody());
        msgInner.setFlag(reqHeader.getFlag());
        MessageAccessor.setProperties(msgInner, string2messageProperties(reqHeader.getProperties()));
        msgInner.setPropertiesString(reqHeader.getProperties());
        msgInner.setBornTimestamp(reqHeader.getBornTimestamp());
        msgInner.setBornHost(ctx.channel().remoteAddress());
        msgInner.setStoreHost(this.getStoreHost());
        msgInner.setReconsumeTimes(reqHeader.getReconsumeTimes() == null ? 0 : reqHeader.getReconsumeTimes());

        //投递消息结果
        PutMessageResult result = null;
        Map<String, String> oriProps = string2messageProperties(reqHeader.getProperties());
        //获得事务标记
        String traFlag = oriProps.get(MessageConst.PROPERTY_TRANSACTION_PREPARED);
        if (traFlag != null && Boolean.parseBoolean(traFlag)) { //是否事务消息
            //broker配置不支持事务，无法支持事务消息，就是broker能够配置不支持事务消息的特性
            if (brokerConfig.isRejectTransactionMessage()) {
                resp.setCode(ResponseCode.NO_PERMISSION);
                resp.setRemark("the broker[" + brokerConfig.getBrokerIP1() + "] sending transaction message is forbidden");
                return resp;
            }
            //一阶段hal事务消息，我们需要处理这个消息，然后再进行投递，返回投递half消息结果
            result = this.brokerController.getTransactionalMessageService().prepareMessage(msgInner);
        } else {
            //普通的消息，非事务消息，我们不想做那么多的处理
            // 使用消息存储处理，返回消息投递结果
            result = this.brokerController.getMessageStore().putMessage(msgInner); //非事务消息的投递
        }

        //处理消息投递结果
        return handlePutMessageResult(result, resp, req, msgInner, respHeader, context, ctx, queueId);

    }

    /**
     * 处理消息投递
     * @param putMessageResult
     * @param response
     * @param request
     * @param msg
     * @param responseHeader
     * @param sendMessageContext
     * @param ctx
     * @param queueIdInt
     * @return
     */
    private RemotingCommand handlePutMessageResult(PutMessageResult putMessageResult, RemotingCommand response,
                                                   RemotingCommand request, MessageExt msg,
                                                   SendMessageResponseHeader responseHeader, SendMessageContext sendMessageContext, ChannelHandlerContext ctx,
                                                   int queueIdInt) {
        if (putMessageResult == null) {
            response.setCode(SYSTEM_ERROR);
            response.setRemark("store putMessage return null");
            return response;
        }
        boolean sendOK = false;

        /**
         * 消息的状态
         */
        switch (putMessageResult.getPutMessageStatus()) {
            // Success 成功的状态
            case PUT_OK: //成功
                sendOK = true;
                response.setCode(ResponseCode.SUCCESS);
                break;
            case FLUSH_DISK_TIMEOUT: //刷盘超时
                response.setCode(ResponseCode.FLUSH_DISK_TIMEOUT);
                sendOK = true;
                break;
            case FLUSH_SLAVE_TIMEOUT: //同步从节点超时
                response.setCode(ResponseCode.FLUSH_SLAVE_TIMEOUT);
                sendOK = true;
                break;
            case SLAVE_NOT_AVAILABLE: //从节点不可用
                response.setCode(ResponseCode.SLAVE_NOT_AVAILABLE);
                sendOK = true;
                break;

            // Failed 失败的状态
            case CREATE_MAPEDFILE_FAILED: //创建对应的mapfile文件失败了
                response.setCode(SYSTEM_ERROR);
                response.setRemark("create mapped file failed, server is busy or broken.");
                break;
            case MESSAGE_ILLEGAL: //消息非法
            case PROPERTIES_SIZE_EXCEEDED: //properties大小过大
                response.setCode(ResponseCode.MESSAGE_ILLEGAL);
                response.setRemark(
                    "the message is illegal, maybe msg body or properties length not matched. msg body length limit 128k, msg properties length limit 32k.");
                break;
            case SERVICE_NOT_AVAILABLE: //服务不可用
                response.setCode(ResponseCode.SERVICE_NOT_AVAILABLE);
                response.setRemark(
                    "service not available now, maybe disk full, " + diskUtil() + ", maybe your broker machine memory too small.");
                break;
            case OS_PAGECACHE_BUSY: // 繁忙
                response.setCode(SYSTEM_ERROR);
                response.setRemark("[PC_SYNCHRONIZED]broker busy, start flow control for a while");
                break;
            case UNKNOWN_ERROR: //未知错误
                response.setCode(SYSTEM_ERROR);
                response.setRemark("UNKNOWN_ERROR");
                break;
            default: //其他的不知道的错误
                response.setCode(SYSTEM_ERROR);
                response.setRemark("UNKNOWN_ERROR DEFAULT");
                break;
        }

        //所有者
        String owner = request.getExtFields().get(BrokerStatsManager.COMMERCIAL_OWNER);
        if (sendOK) {//发送成功

            //增加投递的数量
            this.brokerController.getBrokerStatsManager().incTopicPutNums(msg.getTopic(), putMessageResult.getAppendMessageResult().getMsgNum(), 1);
            //增加投递的数量
            this.brokerController.getBrokerStatsManager().incTopicPutSize(msg.getTopic(),
                putMessageResult.getAppendMessageResult().getWroteBytes());
            //增加该broker投递的数量
            this.brokerController.getBrokerStatsManager().incBrokerPutNums(putMessageResult.getAppendMessageResult().getMsgNum());

            //
            response.setRemark(null);

            //设置msgid
            responseHeader.setMsgId(putMessageResult.getAppendMessageResult().getMsgId());
            //设置
            responseHeader.setQueueId(queueIdInt);
            //设置偏移量
            responseHeader.setQueueOffset(putMessageResult.getAppendMessageResult().getLogicsOffset());

            //进行响应
            doResponse(ctx, request, response);

            //存在响应的钩子
            if (hasSendMessageHook()) {
                sendMessageContext.setMsgId(responseHeader.getMsgId());
                sendMessageContext.setQueueId(responseHeader.getQueueId());
                sendMessageContext.setQueueOffset(responseHeader.getQueueOffset());

                int commercialBaseCount = brokerController.getBrokerConfig().getCommercialBaseCount();
                int wroteSize = putMessageResult.getAppendMessageResult().getWroteBytes();
                int incValue = (int)Math.ceil(wroteSize / BrokerStatsManager.SIZE_PER_COUNT) * commercialBaseCount;

                sendMessageContext.setCommercialSendStats(BrokerStatsManager.StatsType.SEND_SUCCESS);
                sendMessageContext.setCommercialSendTimes(incValue);
                sendMessageContext.setCommercialSendSize(wroteSize);
                sendMessageContext.setCommercialOwner(owner);
            }
            return null;
        } else { //发送失败
            if (hasSendMessageHook()) {
                int wroteSize = request.getBody().length;
                int incValue = (int)Math.ceil(wroteSize / BrokerStatsManager.SIZE_PER_COUNT);

                sendMessageContext.setCommercialSendStats(BrokerStatsManager.StatsType.SEND_FAILURE);
                sendMessageContext.setCommercialSendTimes(incValue);
                sendMessageContext.setCommercialSendSize(wroteSize);
                sendMessageContext.setCommercialOwner(owner);
            }
        }
        return response;
    }

    /**
     * 批量发送相关消息
     * @param ctx
     * @param request
     * @param sendMessageContext
     * @param requestHeader
     * @return
     * @throws RemotingCommandException
     */
    private RemotingCommand sendBatchMessage(final ChannelHandlerContext ctx,
                                             final RemotingCommand request,
                                             final SendMessageContext sendMessageContext,
                                             final SendMessageRequestHeader requestHeader) throws RemotingCommandException {


        final RemotingCommand response = createResponseCommand(SendMessageResponseHeader.class);
        final SendMessageResponseHeader responseHeader = (SendMessageResponseHeader)response.readCustomHeader();

        response.setOpaque(request.getOpaque());

        response.addExtField(PROPERTY_MSG_REGION, this.brokerController.getBrokerConfig().getRegionId());
        response.addExtField(PROPERTY_TRACE_SWITCH, String.valueOf(this.brokerController.getBrokerConfig().isTraceOn()));

        log.debug("Receive SendMessage request command {}", request);

        final long startTimstamp = this.brokerController.getBrokerConfig().getStartAcceptSendRequestTimeStamp();
        if (this.brokerController.getMessageStore().now() < startTimstamp) {
            response.setCode(SYSTEM_ERROR);
            response.setRemark(String.format("broker unable to service, until %s", UtilAll.timeMillisToHumanString2(startTimstamp)));
            return response;
        }

        response.setCode(-1);
        super.msgCheck(ctx, requestHeader, response);
        if (response.getCode() != -1) {
            return response;
        }

        int queueIdInt = requestHeader.getQueueId();
        TopicConfig topicConfig = this.brokerController.getTopicConfigManager().selectTopicConfig(requestHeader.getTopic());

        if (queueIdInt < 0) {
            queueIdInt = Math.abs(this.random.nextInt() % 99999999) % topicConfig.getWriteQueueNums();
        }

        if (requestHeader.getTopic().length() > Byte.MAX_VALUE) {
            response.setCode(ResponseCode.MESSAGE_ILLEGAL);
            response.setRemark("message topic length too long " + requestHeader.getTopic().length());
            return response;
        }

        if (requestHeader.getTopic() != null && requestHeader.getTopic().startsWith(MixAll.RETRY_GROUP_TOPIC_PREFIX)) {
            response.setCode(ResponseCode.MESSAGE_ILLEGAL);
            response.setRemark("batch request does not support retry group " + requestHeader.getTopic());
            return response;
        }
        MessageExtBatch messageExtBatch = new MessageExtBatch();
        messageExtBatch.setTopic(requestHeader.getTopic());
        messageExtBatch.setQueueId(queueIdInt);

        int sysFlag = requestHeader.getSysFlag();
        if (TopicFilterType.MULTI_TAG == topicConfig.getTopicFilterType()) {
            sysFlag |= MessageSysFlag.MULTI_TAGS_FLAG;
        }
        messageExtBatch.setSysFlag(sysFlag);

        messageExtBatch.setFlag(requestHeader.getFlag());
        MessageAccessor.setProperties(messageExtBatch, string2messageProperties(requestHeader.getProperties()));
        messageExtBatch.setBody(request.getBody());
        messageExtBatch.setBornTimestamp(requestHeader.getBornTimestamp());
        messageExtBatch.setBornHost(ctx.channel().remoteAddress());
        messageExtBatch.setStoreHost(this.getStoreHost());
        messageExtBatch.setReconsumeTimes(requestHeader.getReconsumeTimes() == null ? 0 : requestHeader.getReconsumeTimes());

        PutMessageResult putMessageResult = this.brokerController.getMessageStore().putMessages(messageExtBatch);

        return handlePutMessageResult(putMessageResult, response, request, messageExtBatch, responseHeader, sendMessageContext, ctx, queueIdInt);
    }

    public boolean hasConsumeMessageHook() {
        return consumeMessageHookList != null && !this.consumeMessageHookList.isEmpty();
    }

    /**
     * 消费者消息重发消费的，因此是消费消息后的事情
     * @param context
     */
    public void executeConsumeMessageHookAfter(final ConsumeMessageContext context) {
        if (hasConsumeMessageHook()) {
            for (ConsumeMessageHook hook : this.consumeMessageHookList) {
                try {
                    hook.consumeMessageAfter(context);
                } catch (Throwable e) {
                    // Ignore
                }
            }
        }
    }

    public SocketAddress getStoreHost() {
        return storeHost;
    }

    private String diskUtil() {
        String storePathPhysic = this.brokerController.getMessageStoreConfig().getStorePathCommitLog();
        double physicRatio = UtilAll.getDiskPartitionSpaceUsedPercent(storePathPhysic);

        String storePathLogis =
            StorePathConfigHelper.getStorePathConsumeQueue(this.brokerController.getMessageStoreConfig().getStorePathRootDir());
        double logisRatio = UtilAll.getDiskPartitionSpaceUsedPercent(storePathLogis);

        String storePathIndex =
            StorePathConfigHelper.getStorePathIndex(this.brokerController.getMessageStoreConfig().getStorePathRootDir());
        double indexRatio = UtilAll.getDiskPartitionSpaceUsedPercent(storePathIndex);

        return String.format("CL: %5.2f CQ: %5.2f INDEX: %5.2f", physicRatio, logisRatio, indexRatio);
    }

    public void registerConsumeMessageHook(List<ConsumeMessageHook> consumeMessageHookList) {
        this.consumeMessageHookList = consumeMessageHookList;
    }
}
