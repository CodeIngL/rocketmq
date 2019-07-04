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

import com.alibaba.fastjson.JSON;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import java.io.UnsupportedEncodingException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.broker.client.ClientChannelInfo;
import org.apache.rocketmq.broker.client.ConsumerGroupInfo;
import org.apache.rocketmq.broker.filter.ConsumerFilterData;
import org.apache.rocketmq.broker.filter.ExpressionMessageFilter;
import org.apache.rocketmq.common.MQVersion;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.admin.ConsumeStats;
import org.apache.rocketmq.common.admin.OffsetWrapper;
import org.apache.rocketmq.common.admin.TopicOffset;
import org.apache.rocketmq.common.admin.TopicStatsTable;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageId;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.protocol.RequestCode;
import org.apache.rocketmq.common.protocol.ResponseCode;
import org.apache.rocketmq.common.protocol.body.BrokerStatsData;
import org.apache.rocketmq.common.protocol.body.BrokerStatsItem;
import org.apache.rocketmq.common.protocol.body.Connection;
import org.apache.rocketmq.common.protocol.body.ConsumeQueueData;
import org.apache.rocketmq.common.protocol.body.ConsumeStatsList;
import org.apache.rocketmq.common.protocol.body.ConsumerConnection;
import org.apache.rocketmq.common.protocol.body.GroupList;
import org.apache.rocketmq.common.protocol.body.KVTable;
import org.apache.rocketmq.common.protocol.body.LockBatchRequestBody;
import org.apache.rocketmq.common.protocol.body.LockBatchResponseBody;
import org.apache.rocketmq.common.protocol.body.ProducerConnection;
import org.apache.rocketmq.common.protocol.body.QueryConsumeQueueResponseBody;
import org.apache.rocketmq.common.protocol.body.QueryConsumeTimeSpanBody;
import org.apache.rocketmq.common.protocol.body.QueryCorrectionOffsetBody;
import org.apache.rocketmq.common.protocol.body.QueueTimeSpan;
import org.apache.rocketmq.common.protocol.body.TopicList;
import org.apache.rocketmq.common.protocol.body.UnlockBatchRequestBody;
import org.apache.rocketmq.common.protocol.header.CloneGroupOffsetRequestHeader;
import org.apache.rocketmq.common.protocol.header.ConsumeMessageDirectlyResultRequestHeader;
import org.apache.rocketmq.common.protocol.header.CreateTopicRequestHeader;
import org.apache.rocketmq.common.protocol.header.DeleteSubscriptionGroupRequestHeader;
import org.apache.rocketmq.common.protocol.header.DeleteTopicRequestHeader;
import org.apache.rocketmq.common.protocol.header.GetAllTopicConfigResponseHeader;
import org.apache.rocketmq.common.protocol.header.GetBrokerConfigResponseHeader;
import org.apache.rocketmq.common.protocol.header.GetConsumeStatsInBrokerHeader;
import org.apache.rocketmq.common.protocol.header.GetConsumeStatsRequestHeader;
import org.apache.rocketmq.common.protocol.header.GetConsumerConnectionListRequestHeader;
import org.apache.rocketmq.common.protocol.header.GetConsumerRunningInfoRequestHeader;
import org.apache.rocketmq.common.protocol.header.GetConsumerStatusRequestHeader;
import org.apache.rocketmq.common.protocol.header.GetEarliestMsgStoretimeRequestHeader;
import org.apache.rocketmq.common.protocol.header.GetEarliestMsgStoretimeResponseHeader;
import org.apache.rocketmq.common.protocol.header.GetMaxOffsetRequestHeader;
import org.apache.rocketmq.common.protocol.header.GetMaxOffsetResponseHeader;
import org.apache.rocketmq.common.protocol.header.GetMinOffsetRequestHeader;
import org.apache.rocketmq.common.protocol.header.GetMinOffsetResponseHeader;
import org.apache.rocketmq.common.protocol.header.GetProducerConnectionListRequestHeader;
import org.apache.rocketmq.common.protocol.header.GetTopicStatsInfoRequestHeader;
import org.apache.rocketmq.common.protocol.header.QueryConsumeQueueRequestHeader;
import org.apache.rocketmq.common.protocol.header.QueryConsumeTimeSpanRequestHeader;
import org.apache.rocketmq.common.protocol.header.QueryCorrectionOffsetHeader;
import org.apache.rocketmq.common.protocol.header.QueryTopicConsumeByWhoRequestHeader;
import org.apache.rocketmq.common.protocol.header.ResetOffsetRequestHeader;
import org.apache.rocketmq.common.protocol.header.SearchOffsetRequestHeader;
import org.apache.rocketmq.common.protocol.header.SearchOffsetResponseHeader;
import org.apache.rocketmq.common.protocol.header.ViewBrokerStatsDataRequestHeader;
import org.apache.rocketmq.common.protocol.header.filtersrv.RegisterFilterServerRequestHeader;
import org.apache.rocketmq.common.protocol.header.filtersrv.RegisterFilterServerResponseHeader;
import org.apache.rocketmq.common.protocol.heartbeat.SubscriptionData;
import org.apache.rocketmq.common.stats.StatsItem;
import org.apache.rocketmq.common.stats.StatsSnapshot;
import org.apache.rocketmq.common.subscription.SubscriptionGroupConfig;
import org.apache.rocketmq.filter.util.BitsArray;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;
import org.apache.rocketmq.remoting.exception.RemotingTimeoutException;
import org.apache.rocketmq.remoting.netty.NettyRequestProcessor;
import org.apache.rocketmq.remoting.protocol.LanguageCode;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.protocol.RemotingSerializable;
import org.apache.rocketmq.store.ConsumeQueue;
import org.apache.rocketmq.store.ConsumeQueueExt;
import org.apache.rocketmq.store.DefaultMessageStore;
import org.apache.rocketmq.store.MessageFilter;
import org.apache.rocketmq.store.MessageStore;
import org.apache.rocketmq.store.SelectMappedBufferResult;

import static org.apache.rocketmq.remoting.common.RemotingHelper.parseChannelRemoteAddr;
import static org.apache.rocketmq.remoting.protocol.RemotingCommand.createRequestCommand;
import static org.apache.rocketmq.remoting.protocol.RemotingCommand.createResponseCommand;
import static org.apache.rocketmq.remoting.protocol.RemotingSysResponseCode.SUCCESS;
import static org.apache.rocketmq.remoting.protocol.RemotingSysResponseCode.SYSTEM_ERROR;

/**
 * 管理员的操作命令处理，broker操作
 */
public class AdminBrokerProcessor implements NettyRequestProcessor {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.BROKER_LOGGER_NAME);
    private final BrokerController brokerController;

    public AdminBrokerProcessor(final BrokerController brokerController) {
        this.brokerController = brokerController;
    }

    /**
     * 处理请求
     * @param ctx
     * @param req
     * @return
     * @throws RemotingCommandException
     */
    @Override
    public RemotingCommand processRequest(ChannelHandlerContext ctx,
        RemotingCommand req) throws RemotingCommandException {
        /**
         * 根据不同的命令执行不同的操作
         */
        switch (req.getCode()) {
            case RequestCode.UPDATE_AND_CREATE_TOPIC: //修改或者创建topic
                return this.updateAndCreateTopic(ctx, req);
            case RequestCode.DELETE_TOPIC_IN_BROKER: //删除
                return this.deleteTopic(ctx, req);
            case RequestCode.GET_ALL_TOPIC_CONFIG: //获得所有主题的配置
                return this.getAllTopicConfig(ctx, req);
            case RequestCode.UPDATE_BROKER_CONFIG: //更新broker的配置
                return this.updateBrokerConfig(ctx, req);
            case RequestCode.GET_BROKER_CONFIG: //获得broker的配置
                return this.getBrokerConfig(ctx, req);
            case RequestCode.SEARCH_OFFSET_BY_TIMESTAMP: //通过时间搜索offser
                return this.searchOffsetByTimestamp(ctx, req);
            case RequestCode.GET_MAX_OFFSET:    //获得最大maxoffset
                return this.getMaxOffset(ctx, req);
            case RequestCode.GET_MIN_OFFSET:    //获得最大minoffset
                return this.getMinOffset(ctx, req);
            case RequestCode.GET_EARLIEST_MSG_STORETIME:
                return this.getEarliestMsgStoretime(ctx, req);
            case RequestCode.GET_BROKER_RUNTIME_INFO:
                return this.getBrokerRuntimeInfo(ctx, req);
            case RequestCode.LOCK_BATCH_MQ:
                return this.lockBatchMQ(ctx, req);
            case RequestCode.UNLOCK_BATCH_MQ:
                return this.unlockBatchMQ(ctx, req);
            case RequestCode.UPDATE_AND_CREATE_SUBSCRIPTIONGROUP: //更新或者构建消费组对应的订阅组
                return this.updateAndCreateSubscriptionGroup(ctx, req);
            case RequestCode.GET_ALL_SUBSCRIPTIONGROUP_CONFIG:
                return this.getAllSubscriptionGroup(ctx, req);
            case RequestCode.DELETE_SUBSCRIPTIONGROUP:
                return this.deleteSubscriptionGroup(ctx, req);
            case RequestCode.GET_TOPIC_STATS_INFO:
                return this.getTopicStatsInfo(ctx, req);
            case RequestCode.GET_CONSUMER_CONNECTION_LIST:
                return this.getConsumerConnectionList(ctx, req);
            case RequestCode.GET_PRODUCER_CONNECTION_LIST:
                return this.getProducerConnectionList(ctx, req);
            case RequestCode.GET_CONSUME_STATS:
                return this.getConsumeStats(ctx, req);
            case RequestCode.GET_ALL_CONSUMER_OFFSET:
                return this.getAllConsumerOffset(ctx, req);
            case RequestCode.GET_ALL_DELAY_OFFSET:
                return this.getAllDelayOffset(ctx, req);
            case RequestCode.INVOKE_BROKER_TO_RESET_OFFSET:
                return this.resetOffset(ctx, req);
            case RequestCode.INVOKE_BROKER_TO_GET_CONSUMER_STATUS:
                return this.getConsumerStatus(ctx, req);
            case RequestCode.QUERY_TOPIC_CONSUME_BY_WHO:
                return this.queryTopicConsumeByWho(ctx, req);
            case RequestCode.REGISTER_FILTER_SERVER: //注册filterServer
                return this.registerFilterServer(ctx, req);
            case RequestCode.QUERY_CONSUME_TIME_SPAN:
                return this.queryConsumeTimeSpan(ctx, req);
            case RequestCode.GET_SYSTEM_TOPIC_LIST_FROM_BROKER:
                return this.getSystemTopicListFromBroker(ctx, req);
            case RequestCode.CLEAN_EXPIRED_CONSUMEQUEUE:
                return this.cleanExpiredConsumeQueue();
            case RequestCode.CLEAN_UNUSED_TOPIC:
                return this.cleanUnusedTopic();
            case RequestCode.GET_CONSUMER_RUNNING_INFO:
                return this.getConsumerRunningInfo(ctx, req);
            case RequestCode.QUERY_CORRECTION_OFFSET:
                return this.queryCorrectionOffset(ctx, req);
            case RequestCode.CONSUME_MESSAGE_DIRECTLY:
                return this.consumeMessageDirectly(ctx, req);
            case RequestCode.CLONE_GROUP_OFFSET:
                return this.cloneGroupOffset(ctx, req);
            case RequestCode.VIEW_BROKER_STATS_DATA:
                return ViewBrokerStatsData(ctx, req);
            case RequestCode.GET_BROKER_CONSUME_STATS:
                return fetchAllConsumeStatsInBroker(ctx, req);
            case RequestCode.QUERY_CONSUME_QUEUE:
                return queryConsumeQueue(ctx, req);
            default:
                break;
        }

        return null;
    }

    @Override
    public boolean rejectRequest() {
        return false;
    }

    /**
     * 更新或者构建topic
     * @param ctx
     * @param request
     * @return
     * @throws RemotingCommandException
     */
    private synchronized RemotingCommand updateAndCreateTopic(ChannelHandlerContext ctx,
        RemotingCommand request) throws RemotingCommandException {
        final RemotingCommand response = createResponseCommand(null);
        final CreateTopicRequestHeader requestHeader = (CreateTopicRequestHeader) request.decodeCommandCustomHeader(CreateTopicRequestHeader.class);
        log.info("updateAndCreateTopic called by {}", parseChannelRemoteAddr(ctx.channel()));

        if (requestHeader.getTopic().equals(this.brokerController.getBrokerConfig().getBrokerClusterName())) { //名字校验，topic名字不可以使用系统保留字
            String errorMsg = "the topic[" + requestHeader.getTopic() + "] is conflict with system reserved words.";
            log.warn(errorMsg);
            response.setCode(SYSTEM_ERROR);
            response.setRemark(errorMsg);
            return response;
        }

        try {
            response.setCode(SUCCESS);
            response.setOpaque(request.getOpaque());
            response.markResponseType();
            response.setRemark(null);
            ctx.writeAndFlush(response);
        } catch (Exception e) {
            log.error("Failed to produce a proper response", e);
        }

        //topic配置
        TopicConfig topicConfig = new TopicConfig(requestHeader.getTopic());
        //设置要被读的队列
        topicConfig.setReadQueueNums(requestHeader.getReadQueueNums());
        //设置要被写的队列
        topicConfig.setWriteQueueNums(requestHeader.getWriteQueueNums());
        //类型
        topicConfig.setTopicFilterType(requestHeader.getTopicFilterTypeEnum());
        //权限
        topicConfig.setPerm(requestHeader.getPerm());
        //系统标记
        topicConfig.setTopicSysFlag(requestHeader.getTopicSysFlag() == null ? 0 : requestHeader.getTopicSysFlag());

        this.brokerController.getTopicConfigManager().updateTopicConfig(topicConfig);

        this.brokerController.registerIncrementBrokerData(topicConfig,this.brokerController.getTopicConfigManager().getDataVersion());

        return null;
    }

    private synchronized RemotingCommand deleteTopic(ChannelHandlerContext ctx,
        RemotingCommand request) throws RemotingCommandException {
        final RemotingCommand response = createResponseCommand(null);
        DeleteTopicRequestHeader requestHeader =
            (DeleteTopicRequestHeader) request.decodeCommandCustomHeader(DeleteTopicRequestHeader.class);

        log.info("deleteTopic called by {}", parseChannelRemoteAddr(ctx.channel()));

        this.brokerController.getTopicConfigManager().deleteTopicConfig(requestHeader.getTopic());
        this.brokerController.getMessageStore()
            .cleanUnusedTopic(this.brokerController.getTopicConfigManager().getTopicConfigTable().keySet());

        response.setCode(SUCCESS);
        response.setRemark(null);
        return response;
    }

    private RemotingCommand getAllTopicConfig(ChannelHandlerContext ctx, RemotingCommand request) {
        final RemotingCommand response = createResponseCommand(GetAllTopicConfigResponseHeader.class);
        // final GetAllTopicConfigResponseHeader responseHeader =
        // (GetAllTopicConfigResponseHeader) response.readCustomHeader();

        String content = this.brokerController.getTopicConfigManager().encode();
        if (content != null && content.length() > 0) {
            try {
                response.setBody(content.getBytes(MixAll.DEFAULT_CHARSET));
            } catch (UnsupportedEncodingException e) {
                log.error("", e);

                response.setCode(SYSTEM_ERROR);
                response.setRemark("UnsupportedEncodingException " + e);
                return response;
            }
        } else {
            log.error("No topic in this broker, client: {}", ctx.channel().remoteAddress());
            response.setCode(SYSTEM_ERROR);
            response.setRemark("No topic in this broker");
            return response;
        }

        response.setCode(SUCCESS);
        response.setRemark(null);

        return response;
    }

    private synchronized RemotingCommand updateBrokerConfig(ChannelHandlerContext ctx, RemotingCommand request) {
        final RemotingCommand response = createResponseCommand(null);

        log.info("updateBrokerConfig called by {}", parseChannelRemoteAddr(ctx.channel()));

        byte[] body = request.getBody();
        if (body != null) {
            try {
                String bodyStr = new String(body, MixAll.DEFAULT_CHARSET);
                Properties properties = MixAll.string2Properties(bodyStr);
                if (properties != null) {
                    log.info("updateBrokerConfig, new config: [{}] client: {} ", properties, ctx.channel().remoteAddress());
                    this.brokerController.getConfiguration().update(properties);
                    if (properties.containsKey("brokerPermission")) {
                        this.brokerController.getTopicConfigManager().getDataVersion().nextVersion();
                        this.brokerController.registerBrokerAll(false, false, true);
                    }
                } else {
                    log.error("string2Properties error");
                    response.setCode(SYSTEM_ERROR);
                    response.setRemark("string2Properties error");
                    return response;
                }
            } catch (UnsupportedEncodingException e) {
                log.error("", e);
                response.setCode(SYSTEM_ERROR);
                response.setRemark("UnsupportedEncodingException " + e);
                return response;
            }
        }

        response.setCode(SUCCESS);
        response.setRemark(null);
        return response;
    }

    private RemotingCommand getBrokerConfig(ChannelHandlerContext ctx, RemotingCommand request) {

        final RemotingCommand response = createResponseCommand(GetBrokerConfigResponseHeader.class);
        final GetBrokerConfigResponseHeader responseHeader = (GetBrokerConfigResponseHeader) response.readCustomHeader();

        String content = this.brokerController.getConfiguration().getAllConfigsFormatString();
        if (content != null && content.length() > 0) {
            try {
                response.setBody(content.getBytes(MixAll.DEFAULT_CHARSET));
            } catch (UnsupportedEncodingException e) {
                log.error("", e);

                response.setCode(SYSTEM_ERROR);
                response.setRemark("UnsupportedEncodingException " + e);
                return response;
            }
        }

        responseHeader.setVersion(this.brokerController.getConfiguration().getDataVersionJson());

        response.setCode(SUCCESS);
        response.setRemark(null);
        return response;
    }

    private RemotingCommand searchOffsetByTimestamp(ChannelHandlerContext ctx,
        RemotingCommand request) throws RemotingCommandException {
        final RemotingCommand response = createResponseCommand(SearchOffsetResponseHeader.class);
        final SearchOffsetResponseHeader responseHeader = (SearchOffsetResponseHeader) response.readCustomHeader();
        final SearchOffsetRequestHeader requestHeader =
            (SearchOffsetRequestHeader) request.decodeCommandCustomHeader(SearchOffsetRequestHeader.class);

        long offset = this.brokerController.getMessageStore().getOffsetInQueueByTime(requestHeader.getTopic(), requestHeader.getQueueId(),
            requestHeader.getTimestamp());

        responseHeader.setOffset(offset);

        response.setCode(SUCCESS);
        response.setRemark(null);
        return response;
    }

    private RemotingCommand getMaxOffset(ChannelHandlerContext ctx,
        RemotingCommand request) throws RemotingCommandException {
        final RemotingCommand response = createResponseCommand(GetMaxOffsetResponseHeader.class);
        final GetMaxOffsetResponseHeader responseHeader = (GetMaxOffsetResponseHeader) response.readCustomHeader();
        final GetMaxOffsetRequestHeader requestHeader =
            (GetMaxOffsetRequestHeader) request.decodeCommandCustomHeader(GetMaxOffsetRequestHeader.class);

        long offset = this.brokerController.getMessageStore().getMaxOffsetInQueue(requestHeader.getTopic(), requestHeader.getQueueId());

        responseHeader.setOffset(offset);

        response.setCode(SUCCESS);
        response.setRemark(null);
        return response;
    }

    private RemotingCommand getMinOffset(ChannelHandlerContext ctx,
        RemotingCommand request) throws RemotingCommandException {
        final RemotingCommand response = createResponseCommand(GetMinOffsetResponseHeader.class);
        final GetMinOffsetResponseHeader responseHeader = (GetMinOffsetResponseHeader) response.readCustomHeader();
        final GetMinOffsetRequestHeader requestHeader =
            (GetMinOffsetRequestHeader) request.decodeCommandCustomHeader(GetMinOffsetRequestHeader.class);

        long offset = this.brokerController.getMessageStore().getMinOffsetInQueue(requestHeader.getTopic(), requestHeader.getQueueId());

        responseHeader.setOffset(offset);
        response.setCode(SUCCESS);
        response.setRemark(null);
        return response;
    }

    private RemotingCommand getEarliestMsgStoretime(ChannelHandlerContext ctx,
        RemotingCommand request) throws RemotingCommandException {
        final RemotingCommand response = createResponseCommand(GetEarliestMsgStoretimeResponseHeader.class);
        final GetEarliestMsgStoretimeResponseHeader responseHeader = (GetEarliestMsgStoretimeResponseHeader) response.readCustomHeader();
        final GetEarliestMsgStoretimeRequestHeader requestHeader =
            (GetEarliestMsgStoretimeRequestHeader) request.decodeCommandCustomHeader(GetEarliestMsgStoretimeRequestHeader.class);

        long timestamp =
            this.brokerController.getMessageStore().getEarliestMessageTime(requestHeader.getTopic(), requestHeader.getQueueId());

        responseHeader.setTimestamp(timestamp);
        response.setCode(SUCCESS);
        response.setRemark(null);
        return response;
    }

    private RemotingCommand getBrokerRuntimeInfo(ChannelHandlerContext ctx, RemotingCommand request) {
        final RemotingCommand response = createResponseCommand(null);

        HashMap<String, String> runtimeInfo = this.prepareRuntimeInfo();
        KVTable kvTable = new KVTable();
        kvTable.setTable(runtimeInfo);

        byte[] body = kvTable.encode();
        response.setBody(body);
        response.setCode(SUCCESS);
        response.setRemark(null);
        return response;
    }

    /**
     * 批量锁住mq
     * @param ctx
     * @param req
     * @return
     * @throws RemotingCommandException
     */
    private RemotingCommand lockBatchMQ(ChannelHandlerContext ctx, RemotingCommand req) throws RemotingCommandException {
        final RemotingCommand resp = createResponseCommand(null);
        LockBatchRequestBody reqBody = LockBatchRequestBody.decode(req.getBody(), LockBatchRequestBody.class);

        //获得成功被锁定的消息队列
        Set<MessageQueue> lockOKMQSet = this.brokerController.getRebalanceLockManager().tryLockBatch(
            reqBody.getConsumerGroup(),
            reqBody.getMqSet(),
            reqBody.getClientId());

        LockBatchResponseBody respBody = new LockBatchResponseBody();
        respBody.setLockOKMQSet(lockOKMQSet);

        resp.setBody(respBody.encode());
        resp.setCode(SUCCESS);
        resp.setRemark(null);
        return resp;
    }

    private RemotingCommand unlockBatchMQ(ChannelHandlerContext ctx,
        RemotingCommand request) throws RemotingCommandException {
        final RemotingCommand response = createResponseCommand(null);
        UnlockBatchRequestBody requestBody = UnlockBatchRequestBody.decode(request.getBody(), UnlockBatchRequestBody.class);

        this.brokerController.getRebalanceLockManager().unlockBatch(
            requestBody.getConsumerGroup(),
            requestBody.getMqSet(),
            requestBody.getClientId());

        response.setCode(SUCCESS);
        response.setRemark(null);
        return response;
    }

    /**
     * 更新或者构建订阅组
     * @param ctx
     * @param req
     * @return
     * @throws RemotingCommandException
     */
    private RemotingCommand updateAndCreateSubscriptionGroup(ChannelHandlerContext ctx, RemotingCommand req) throws RemotingCommandException {
        final RemotingCommand resp = createResponseCommand(null);

        log.info("updateAndCreateSubscriptionGroup called by {}", parseChannelRemoteAddr(ctx.channel()));

        SubscriptionGroupConfig config = RemotingSerializable.decode(req.getBody(), SubscriptionGroupConfig.class);
        if (config != null) {
            this.brokerController.getSubscriptionGroupManager().updateSubscriptionGroupConfig(config);
        }

        resp.setCode(SUCCESS);
        resp.setRemark(null);
        return resp;
    }

    private RemotingCommand getAllSubscriptionGroup(ChannelHandlerContext ctx,
        RemotingCommand request) throws RemotingCommandException {
        final RemotingCommand response = createResponseCommand(null);
        String content = this.brokerController.getSubscriptionGroupManager().encode();
        if (content != null && content.length() > 0) {
            try {
                response.setBody(content.getBytes(MixAll.DEFAULT_CHARSET));
            } catch (UnsupportedEncodingException e) {
                log.error("", e);

                response.setCode(SYSTEM_ERROR);
                response.setRemark("UnsupportedEncodingException " + e);
                return response;
            }
        } else {
            log.error("No subscription group in this broker, client:{} ", ctx.channel().remoteAddress());
            response.setCode(SYSTEM_ERROR);
            response.setRemark("No subscription group in this broker");
            return response;
        }

        response.setCode(SUCCESS);
        response.setRemark(null);

        return response;
    }

    private RemotingCommand deleteSubscriptionGroup(ChannelHandlerContext ctx,
        RemotingCommand request) throws RemotingCommandException {
        final RemotingCommand response = createResponseCommand(null);
        DeleteSubscriptionGroupRequestHeader requestHeader =
            (DeleteSubscriptionGroupRequestHeader) request.decodeCommandCustomHeader(DeleteSubscriptionGroupRequestHeader.class);

        log.info("deleteSubscriptionGroup called by {}", parseChannelRemoteAddr(ctx.channel()));

        this.brokerController.getSubscriptionGroupManager().deleteSubscriptionGroupConfig(requestHeader.getGroupName());

        response.setCode(SUCCESS);
        response.setRemark(null);
        return response;
    }

    private RemotingCommand getTopicStatsInfo(ChannelHandlerContext ctx,
        RemotingCommand request) throws RemotingCommandException {
        final RemotingCommand response = createResponseCommand(null);
        final GetTopicStatsInfoRequestHeader requestHeader =
            (GetTopicStatsInfoRequestHeader) request.decodeCommandCustomHeader(GetTopicStatsInfoRequestHeader.class);

        final String topic = requestHeader.getTopic();
        TopicConfig topicConfig = this.brokerController.getTopicConfigManager().selectTopicConfig(topic);
        if (null == topicConfig) {
            response.setCode(ResponseCode.TOPIC_NOT_EXIST);
            response.setRemark("topic[" + topic + "] not exist");
            return response;
        }

        TopicStatsTable topicStatsTable = new TopicStatsTable();
        for (int i = 0; i < topicConfig.getWriteQueueNums(); i++) {
            MessageQueue mq = new MessageQueue();
            mq.setTopic(topic);
            mq.setBrokerName(this.brokerController.getBrokerConfig().getBrokerName());
            mq.setQueueId(i);

            TopicOffset topicOffset = new TopicOffset();
            long min = this.brokerController.getMessageStore().getMinOffsetInQueue(topic, i);
            if (min < 0)
                min = 0;

            long max = this.brokerController.getMessageStore().getMaxOffsetInQueue(topic, i);
            if (max < 0)
                max = 0;

            long timestamp = 0;
            if (max > 0) {
                timestamp = this.brokerController.getMessageStore().getMessageStoreTimeStamp(topic, i, max - 1);
            }

            topicOffset.setMinOffset(min);
            topicOffset.setMaxOffset(max);
            topicOffset.setLastUpdateTimestamp(timestamp);

            topicStatsTable.getOffsetTable().put(mq, topicOffset);
        }

        byte[] body = topicStatsTable.encode();
        response.setBody(body);
        response.setCode(SUCCESS);
        response.setRemark(null);
        return response;
    }

    private RemotingCommand getConsumerConnectionList(ChannelHandlerContext ctx,
        RemotingCommand request) throws RemotingCommandException {
        final RemotingCommand response = createResponseCommand(null);
        final GetConsumerConnectionListRequestHeader requestHeader =
            (GetConsumerConnectionListRequestHeader) request.decodeCommandCustomHeader(GetConsumerConnectionListRequestHeader.class);

        ConsumerGroupInfo consumerGroupInfo =
            this.brokerController.getConsumerManager().getConsumerGroupInfo(requestHeader.getConsumerGroup());
        if (consumerGroupInfo != null) {
            ConsumerConnection bodydata = new ConsumerConnection();
            bodydata.setConsumeFromWhere(consumerGroupInfo.getConsumeFromWhere());
            bodydata.setConsumeType(consumerGroupInfo.getConsumeType());
            bodydata.setMessageModel(consumerGroupInfo.getMessageModel());
            bodydata.getSubscriptionTable().putAll(consumerGroupInfo.getSubscriptionTable());

            Iterator<Map.Entry<Channel, ClientChannelInfo>> it = consumerGroupInfo.getChannelInfoTable().entrySet().iterator();
            while (it.hasNext()) {
                ClientChannelInfo info = it.next().getValue();
                Connection connection = new Connection();
                connection.setClientId(info.getClientId());
                connection.setLanguage(info.getLanguage());
                connection.setVersion(info.getVersion());
                connection.setClientAddr(parseChannelRemoteAddr(info.getChannel()));

                bodydata.getConnectionSet().add(connection);
            }

            byte[] body = bodydata.encode();
            response.setBody(body);
            response.setCode(SUCCESS);
            response.setRemark(null);

            return response;
        }

        response.setCode(ResponseCode.CONSUMER_NOT_ONLINE);
        response.setRemark("the consumer group[" + requestHeader.getConsumerGroup() + "] not online");
        return response;
    }

    private RemotingCommand getProducerConnectionList(ChannelHandlerContext ctx,
        RemotingCommand request) throws RemotingCommandException {
        final RemotingCommand response = createResponseCommand(null);
        final GetProducerConnectionListRequestHeader requestHeader =
            (GetProducerConnectionListRequestHeader) request.decodeCommandCustomHeader(GetProducerConnectionListRequestHeader.class);

        ProducerConnection bodydata = new ProducerConnection();
        HashMap<Channel, ClientChannelInfo> channelInfoHashMap =
            this.brokerController.getProducerManager().getGroupChannelTable().get(requestHeader.getProducerGroup());
        if (channelInfoHashMap != null) {
            Iterator<Map.Entry<Channel, ClientChannelInfo>> it = channelInfoHashMap.entrySet().iterator();
            while (it.hasNext()) {
                ClientChannelInfo info = it.next().getValue();
                Connection connection = new Connection();
                connection.setClientId(info.getClientId());
                connection.setLanguage(info.getLanguage());
                connection.setVersion(info.getVersion());
                connection.setClientAddr(parseChannelRemoteAddr(info.getChannel()));

                bodydata.getConnectionSet().add(connection);
            }

            byte[] body = bodydata.encode();
            response.setBody(body);
            response.setCode(SUCCESS);
            response.setRemark(null);
            return response;
        }

        response.setCode(SYSTEM_ERROR);
        response.setRemark("the producer group[" + requestHeader.getProducerGroup() + "] not exist");
        return response;
    }

    private RemotingCommand getConsumeStats(ChannelHandlerContext ctx,
        RemotingCommand request) throws RemotingCommandException {
        final RemotingCommand response = createResponseCommand(null);
        final GetConsumeStatsRequestHeader requestHeader =
            (GetConsumeStatsRequestHeader) request.decodeCommandCustomHeader(GetConsumeStatsRequestHeader.class);

        ConsumeStats consumeStats = new ConsumeStats();

        Set<String> topics = new HashSet<String>();
        if (UtilAll.isBlank(requestHeader.getTopic())) {
            topics = this.brokerController.getConsumerOffsetManager().whichTopicByConsumer(requestHeader.getConsumerGroup());
        } else {
            topics.add(requestHeader.getTopic());
        }

        for (String topic : topics) {
            TopicConfig topicConfig = this.brokerController.getTopicConfigManager().selectTopicConfig(topic);
            if (null == topicConfig) {
                log.warn("consumeStats, topic config not exist, {}", topic);
                continue;
            }

            {
                SubscriptionData findSubscriptionData =
                    this.brokerController.getConsumerManager().findSubscriptionData(requestHeader.getConsumerGroup(), topic);

                if (null == findSubscriptionData
                    && this.brokerController.getConsumerManager().findSubscriptionDataCount(requestHeader.getConsumerGroup()) > 0) {
                    log.warn("consumeStats, the consumer group[{}], topic[{}] not exist", requestHeader.getConsumerGroup(), topic);
                    continue;
                }
            }

            for (int i = 0; i < topicConfig.getReadQueueNums(); i++) {
                MessageQueue mq = new MessageQueue();
                mq.setTopic(topic);
                mq.setBrokerName(this.brokerController.getBrokerConfig().getBrokerName());
                mq.setQueueId(i);

                OffsetWrapper offsetWrapper = new OffsetWrapper();

                long brokerOffset = this.brokerController.getMessageStore().getMaxOffsetInQueue(topic, i);
                if (brokerOffset < 0)
                    brokerOffset = 0;

                long consumerOffset = this.brokerController.getConsumerOffsetManager().queryOffset(
                    requestHeader.getConsumerGroup(),
                    topic,
                    i);
                if (consumerOffset < 0)
                    consumerOffset = 0;

                offsetWrapper.setBrokerOffset(brokerOffset);
                offsetWrapper.setConsumerOffset(consumerOffset);

                long timeOffset = consumerOffset - 1;
                if (timeOffset >= 0) {
                    long lastTimestamp = this.brokerController.getMessageStore().getMessageStoreTimeStamp(topic, i, timeOffset);
                    if (lastTimestamp > 0) {
                        offsetWrapper.setLastTimestamp(lastTimestamp);
                    }
                }

                consumeStats.getOffsetTable().put(mq, offsetWrapper);
            }

            double consumeTps = this.brokerController.getBrokerStatsManager().tpsGroupGetNums(requestHeader.getConsumerGroup(), topic);

            consumeTps += consumeStats.getConsumeTps();
            consumeStats.setConsumeTps(consumeTps);
        }

        byte[] body = consumeStats.encode();
        response.setBody(body);
        response.setCode(SUCCESS);
        response.setRemark(null);
        return response;
    }

    private RemotingCommand getAllConsumerOffset(ChannelHandlerContext ctx, RemotingCommand request) {
        final RemotingCommand response = createResponseCommand(null);

        String content = this.brokerController.getConsumerOffsetManager().encode();
        if (content != null && content.length() > 0) {
            try {
                response.setBody(content.getBytes(MixAll.DEFAULT_CHARSET));
            } catch (UnsupportedEncodingException e) {
                log.error("get all consumer offset from master error.", e);

                response.setCode(SYSTEM_ERROR);
                response.setRemark("UnsupportedEncodingException " + e);
                return response;
            }
        } else {
            log.error("No consumer offset in this broker, client: {} ", ctx.channel().remoteAddress());
            response.setCode(SYSTEM_ERROR);
            response.setRemark("No consumer offset in this broker");
            return response;
        }

        response.setCode(SUCCESS);
        response.setRemark(null);

        return response;
    }

    private RemotingCommand getAllDelayOffset(ChannelHandlerContext ctx, RemotingCommand request) {
        final RemotingCommand response = createResponseCommand(null);

        if (!(this.brokerController.getMessageStore() instanceof DefaultMessageStore)) {
            log.error("Delay offset not supported in this messagetore, client: {} ", ctx.channel().remoteAddress());
            response.setCode(SYSTEM_ERROR);
            response.setRemark("Delay offset not supported in this messagetore");
            return response;
        }

        String content = ((DefaultMessageStore) this.brokerController.getMessageStore()).getScheduleMessageService().encode();
        if (content != null && content.length() > 0) {
            try {
                response.setBody(content.getBytes(MixAll.DEFAULT_CHARSET));
            } catch (UnsupportedEncodingException e) {
                log.error("Get all delay offset from master error.", e);

                response.setCode(SYSTEM_ERROR);
                response.setRemark("UnsupportedEncodingException " + e);
                return response;
            }
        } else {
            log.error("No delay offset in this broker, client: {} ", ctx.channel().remoteAddress());
            response.setCode(SYSTEM_ERROR);
            response.setRemark("No delay offset in this broker");
            return response;
        }

        response.setCode(SUCCESS);
        response.setRemark(null);

        return response;
    }

    public RemotingCommand resetOffset(ChannelHandlerContext ctx,
        RemotingCommand request) throws RemotingCommandException {
        final ResetOffsetRequestHeader requestHeader =
            (ResetOffsetRequestHeader) request.decodeCommandCustomHeader(ResetOffsetRequestHeader.class);
        log.info("[reset-offset] reset offset started by {}. topic={}, group={}, timestamp={}, isForce={}",
            parseChannelRemoteAddr(ctx.channel()), requestHeader.getTopic(), requestHeader.getGroup(),
            requestHeader.getTimestamp(), requestHeader.isForce());
        boolean isC = false;
        LanguageCode language = request.getLanguage();
        switch (language) {
            case CPP:
                isC = true;
                break;
        }
        return this.brokerController.getBroker2Client().resetOffset(requestHeader.getTopic(), requestHeader.getGroup(),
            requestHeader.getTimestamp(), requestHeader.isForce(), isC);
    }

    public RemotingCommand getConsumerStatus(ChannelHandlerContext ctx,
        RemotingCommand request) throws RemotingCommandException {
        final GetConsumerStatusRequestHeader requestHeader =
            (GetConsumerStatusRequestHeader) request.decodeCommandCustomHeader(GetConsumerStatusRequestHeader.class);

        log.info("[get-consumer-status] get consumer status by {}. topic={}, group={}",
            parseChannelRemoteAddr(ctx.channel()), requestHeader.getTopic(), requestHeader.getGroup());

        return this.brokerController.getBroker2Client().getConsumeStatus(requestHeader.getTopic(), requestHeader.getGroup(),
            requestHeader.getClientAddr());
    }

    private RemotingCommand queryTopicConsumeByWho(ChannelHandlerContext ctx,
        RemotingCommand request) throws RemotingCommandException {
        final RemotingCommand response = createResponseCommand(null);
        QueryTopicConsumeByWhoRequestHeader requestHeader =
            (QueryTopicConsumeByWhoRequestHeader) request.decodeCommandCustomHeader(QueryTopicConsumeByWhoRequestHeader.class);

        HashSet<String> groups = this.brokerController.getConsumerManager().queryTopicConsumeByWho(requestHeader.getTopic());

        Set<String> groupInOffset = this.brokerController.getConsumerOffsetManager().whichGroupByTopic(requestHeader.getTopic());
        if (groupInOffset != null && !groupInOffset.isEmpty()) {
            groups.addAll(groupInOffset);
        }

        GroupList groupList = new GroupList();
        groupList.setGroupList(groups);
        byte[] body = groupList.encode();

        response.setBody(body);
        response.setCode(SUCCESS);
        response.setRemark(null);
        return response;
    }

    private RemotingCommand registerFilterServer(ChannelHandlerContext ctx, RemotingCommand req) throws RemotingCommandException {
        final RemotingCommand resp = createResponseCommand(RegisterFilterServerResponseHeader.class);
        final RegisterFilterServerResponseHeader respHeader = (RegisterFilterServerResponseHeader) resp.readCustomHeader();
        final RegisterFilterServerRequestHeader reqHeader = (RegisterFilterServerRequestHeader) req.decodeCommandCustomHeader(RegisterFilterServerRequestHeader.class);

        this.brokerController.getFilterServerManager().registerFilterServer(ctx.channel(), reqHeader.getFilterServerAddr());

        respHeader.setBrokerId(this.brokerController.getBrokerConfig().getBrokerId());
        respHeader.setBrokerName(this.brokerController.getBrokerConfig().getBrokerName());
        resp.setCode(SUCCESS);
        resp.setRemark(null);
        return resp;
    }

    private RemotingCommand queryConsumeTimeSpan(ChannelHandlerContext ctx, RemotingCommand req) throws RemotingCommandException {
        final RemotingCommand resp = createResponseCommand(null);
        QueryConsumeTimeSpanRequestHeader requestHeader =
            (QueryConsumeTimeSpanRequestHeader) req.decodeCommandCustomHeader(QueryConsumeTimeSpanRequestHeader.class);

        final String topic = requestHeader.getTopic();
        TopicConfig topicConfig = this.brokerController.getTopicConfigManager().selectTopicConfig(topic);
        if (null == topicConfig) {
            resp.setCode(ResponseCode.TOPIC_NOT_EXIST);
            resp.setRemark("topic[" + topic + "] not exist");
            return resp;
        }

        List<QueueTimeSpan> timeSpanSet = new ArrayList<QueueTimeSpan>();
        for (int i = 0; i < topicConfig.getWriteQueueNums(); i++) {
            QueueTimeSpan timeSpan = new QueueTimeSpan();
            MessageQueue mq = new MessageQueue();
            mq.setTopic(topic);
            mq.setBrokerName(this.brokerController.getBrokerConfig().getBrokerName());
            mq.setQueueId(i);
            timeSpan.setMessageQueue(mq);

            long minTime = this.brokerController.getMessageStore().getEarliestMessageTime(topic, i);
            timeSpan.setMinTimeStamp(minTime);

            long max = this.brokerController.getMessageStore().getMaxOffsetInQueue(topic, i);
            long maxTime = this.brokerController.getMessageStore().getMessageStoreTimeStamp(topic, i, max - 1);
            timeSpan.setMaxTimeStamp(maxTime);

            long consumeTime;
            long consumerOffset = this.brokerController.getConsumerOffsetManager().queryOffset(
                requestHeader.getGroup(), topic, i);
            if (consumerOffset > 0) {
                consumeTime = this.brokerController.getMessageStore().getMessageStoreTimeStamp(topic, i, consumerOffset - 1);
            } else {
                consumeTime = minTime;
            }
            timeSpan.setConsumeTimeStamp(consumeTime);

            long maxBrokerOffset = this.brokerController.getMessageStore().getMaxOffsetInQueue(requestHeader.getTopic(), i);
            if (consumerOffset < maxBrokerOffset) {
                long nextTime = this.brokerController.getMessageStore().getMessageStoreTimeStamp(topic, i, consumerOffset);
                timeSpan.setDelayTime(System.currentTimeMillis() - nextTime);
            }
            timeSpanSet.add(timeSpan);
        }

        QueryConsumeTimeSpanBody queryConsumeTimeSpanBody = new QueryConsumeTimeSpanBody();
        queryConsumeTimeSpanBody.setConsumeTimeSpanSet(timeSpanSet);
        resp.setBody(queryConsumeTimeSpanBody.encode());
        resp.setCode(SUCCESS);
        resp.setRemark(null);
        return resp;
    }

    /**
     * 获得系统的Topic
     * @param ctx
     * @param request
     * @return
     * @throws RemotingCommandException
     */
    private RemotingCommand getSystemTopicListFromBroker(ChannelHandlerContext ctx, RemotingCommand request) throws RemotingCommandException {
        final RemotingCommand response = createResponseCommand(null);

        Set<String> topics = this.brokerController.getTopicConfigManager().getSystemTopic();
        TopicList topicList = new TopicList();
        topicList.setTopicList(topics);
        response.setBody(topicList.encode());
        response.setCode(SUCCESS);
        response.setRemark(null);
        return response;
    }

    public RemotingCommand cleanExpiredConsumeQueue() {
        log.warn("invoke cleanExpiredConsumeQueue start.");
        final RemotingCommand response = createResponseCommand(null);
        brokerController.getMessageStore().cleanExpiredConsumerQueue();
        log.warn("invoke cleanExpiredConsumeQueue end.");
        response.setCode(SUCCESS);
        response.setRemark(null);
        return response;
    }

    public RemotingCommand cleanUnusedTopic() {
        log.warn("invoke cleanUnusedTopic start.");
        final RemotingCommand response = createResponseCommand(null);
        brokerController.getMessageStore().cleanUnusedTopic(brokerController.getTopicConfigManager().getTopicConfigTable().keySet());
        log.warn("invoke cleanUnusedTopic end.");
        response.setCode(SUCCESS);
        response.setRemark(null);
        return response;
    }

    private RemotingCommand getConsumerRunningInfo(ChannelHandlerContext ctx,
        RemotingCommand request) throws RemotingCommandException {
        final GetConsumerRunningInfoRequestHeader requestHeader =
            (GetConsumerRunningInfoRequestHeader) request.decodeCommandCustomHeader(GetConsumerRunningInfoRequestHeader.class);

        return this.callConsumer(RequestCode.GET_CONSUMER_RUNNING_INFO, request, requestHeader.getConsumerGroup(),
            requestHeader.getClientId());
    }

    private RemotingCommand queryCorrectionOffset(ChannelHandlerContext ctx,
        RemotingCommand request) throws RemotingCommandException {
        final RemotingCommand response = createResponseCommand(null);
        QueryCorrectionOffsetHeader requestHeader =
            (QueryCorrectionOffsetHeader) request.decodeCommandCustomHeader(QueryCorrectionOffsetHeader.class);

        Map<Integer, Long> correctionOffset = this.brokerController.getConsumerOffsetManager()
            .queryMinOffsetInAllGroup(requestHeader.getTopic(), requestHeader.getFilterGroups());

        Map<Integer, Long> compareOffset =
            this.brokerController.getConsumerOffsetManager().queryOffset(requestHeader.getTopic(), requestHeader.getCompareGroup());

        if (compareOffset != null && !compareOffset.isEmpty()) {
            for (Map.Entry<Integer, Long> entry : compareOffset.entrySet()) {
                Integer queueId = entry.getKey();
                correctionOffset.put(queueId,
                    correctionOffset.get(queueId) > entry.getValue() ? Long.MAX_VALUE : correctionOffset.get(queueId));
            }
        }

        QueryCorrectionOffsetBody body = new QueryCorrectionOffsetBody();
        body.setCorrectionOffsets(correctionOffset);
        response.setBody(body.encode());
        response.setCode(SUCCESS);
        response.setRemark(null);
        return response;
    }

    /**
     * 直接消费消息
     * @param ctx
     * @param req
     * @return
     * @throws RemotingCommandException
     */
    private RemotingCommand consumeMessageDirectly(ChannelHandlerContext ctx, RemotingCommand req) throws RemotingCommandException {
        final ConsumeMessageDirectlyResultRequestHeader reqHeader = (ConsumeMessageDirectlyResultRequestHeader) req.decodeCommandCustomHeader(ConsumeMessageDirectlyResultRequestHeader.class);

        req.getExtFields().put("brokerName", this.brokerController.getBrokerConfig().getBrokerName());
        SelectMappedBufferResult selectMappedBufferResult = null;
        try {
            //消息id
            MessageId messageId = MessageDecoder.decodeMessageId(reqHeader.getMsgId());
            selectMappedBufferResult = this.brokerController.getMessageStore().selectOneMessageByOffset(messageId.getOffset());

            byte[] body = new byte[selectMappedBufferResult.getSize()];
            selectMappedBufferResult.getByteBuffer().get(body);
            req.setBody(body);
        } catch (UnknownHostException e) {
        } finally {
            if (selectMappedBufferResult != null) {
                selectMappedBufferResult.release();
            }
        }

        //调用直接消息消息
        return this.callConsumer(RequestCode.CONSUME_MESSAGE_DIRECTLY, req, reqHeader.getConsumerGroup(), reqHeader.getClientId());
    }

    private RemotingCommand cloneGroupOffset(ChannelHandlerContext ctx,
        RemotingCommand request) throws RemotingCommandException {
        final RemotingCommand response = createResponseCommand(null);
        CloneGroupOffsetRequestHeader requestHeader =
            (CloneGroupOffsetRequestHeader) request.decodeCommandCustomHeader(CloneGroupOffsetRequestHeader.class);

        Set<String> topics;
        if (UtilAll.isBlank(requestHeader.getTopic())) {
            topics = this.brokerController.getConsumerOffsetManager().whichTopicByConsumer(requestHeader.getSrcGroup());
        } else {
            topics = new HashSet<String>();
            topics.add(requestHeader.getTopic());
        }

        for (String topic : topics) {
            TopicConfig topicConfig = this.brokerController.getTopicConfigManager().selectTopicConfig(topic);
            if (null == topicConfig) {
                log.warn("[cloneGroupOffset], topic config not exist, {}", topic);
                continue;
            }

            if (!requestHeader.isOffline()) {

                SubscriptionData findSubscriptionData =
                    this.brokerController.getConsumerManager().findSubscriptionData(requestHeader.getSrcGroup(), topic);
                if (this.brokerController.getConsumerManager().findSubscriptionDataCount(requestHeader.getSrcGroup()) > 0
                    && findSubscriptionData == null) {
                    log.warn("[cloneGroupOffset], the consumer group[{}], topic[{}] not exist", requestHeader.getSrcGroup(), topic);
                    continue;
                }
            }

            this.brokerController.getConsumerOffsetManager().cloneOffset(requestHeader.getSrcGroup(), requestHeader.getDestGroup(),
                requestHeader.getTopic());
        }

        response.setCode(SUCCESS);
        response.setRemark(null);
        return response;
    }

    private RemotingCommand ViewBrokerStatsData(ChannelHandlerContext ctx,
        RemotingCommand request) throws RemotingCommandException {
        final ViewBrokerStatsDataRequestHeader requestHeader =
            (ViewBrokerStatsDataRequestHeader) request.decodeCommandCustomHeader(ViewBrokerStatsDataRequestHeader.class);
        final RemotingCommand response = createResponseCommand(null);
        MessageStore messageStore = this.brokerController.getMessageStore();

        StatsItem statsItem = messageStore.getBrokerStatsManager().getStatsItem(requestHeader.getStatsName(), requestHeader.getStatsKey());
        if (null == statsItem) {
            response.setCode(SYSTEM_ERROR);
            response.setRemark(String.format("The stats <%s> <%s> not exist", requestHeader.getStatsName(), requestHeader.getStatsKey()));
            return response;
        }

        BrokerStatsData brokerStatsData = new BrokerStatsData();

        {
            BrokerStatsItem it = new BrokerStatsItem();
            StatsSnapshot ss = statsItem.getStatsDataInMinute();
            it.setSum(ss.getSum());
            it.setTps(ss.getTps());
            it.setAvgpt(ss.getAvgpt());
            brokerStatsData.setStatsMinute(it);
        }

        {
            BrokerStatsItem it = new BrokerStatsItem();
            StatsSnapshot ss = statsItem.getStatsDataInHour();
            it.setSum(ss.getSum());
            it.setTps(ss.getTps());
            it.setAvgpt(ss.getAvgpt());
            brokerStatsData.setStatsHour(it);
        }

        {
            BrokerStatsItem it = new BrokerStatsItem();
            StatsSnapshot ss = statsItem.getStatsDataInDay();
            it.setSum(ss.getSum());
            it.setTps(ss.getTps());
            it.setAvgpt(ss.getAvgpt());
            brokerStatsData.setStatsDay(it);
        }

        response.setBody(brokerStatsData.encode());
        response.setCode(SUCCESS);
        response.setRemark(null);
        return response;
    }

    private RemotingCommand fetchAllConsumeStatsInBroker(ChannelHandlerContext ctx, RemotingCommand request)
        throws RemotingCommandException {
        final RemotingCommand response = createResponseCommand(null);
        GetConsumeStatsInBrokerHeader requestHeader =
            (GetConsumeStatsInBrokerHeader) request.decodeCommandCustomHeader(GetConsumeStatsInBrokerHeader.class);
        boolean isOrder = requestHeader.isOrder();
        ConcurrentMap<String, SubscriptionGroupConfig> subscriptionGroups =
            brokerController.getSubscriptionGroupManager().getSubscriptionGroupTable();

        List<Map<String/* subscriptionGroupName */, List<ConsumeStats>>> brokerConsumeStatsList =
            new ArrayList<Map<String, List<ConsumeStats>>>();

        long totalDiff = 0L;

        for (String group : subscriptionGroups.keySet()) {
            Map<String, List<ConsumeStats>> subscripTopicConsumeMap = new HashMap<String, List<ConsumeStats>>();
            Set<String> topics = this.brokerController.getConsumerOffsetManager().whichTopicByConsumer(group);
            List<ConsumeStats> consumeStatsList = new ArrayList<ConsumeStats>();
            for (String topic : topics) {
                ConsumeStats consumeStats = new ConsumeStats();
                TopicConfig topicConfig = this.brokerController.getTopicConfigManager().selectTopicConfig(topic);
                if (null == topicConfig) {
                    log.warn("consumeStats, topic config not exist, {}", topic);
                    continue;
                }

                if (isOrder && !topicConfig.isOrder()) {
                    continue;
                }

                {
                    SubscriptionData findSubscriptionData = this.brokerController.getConsumerManager().findSubscriptionData(group, topic);

                    if (null == findSubscriptionData
                        && this.brokerController.getConsumerManager().findSubscriptionDataCount(group) > 0) {
                        log.warn("consumeStats, the consumer group[{}], topic[{}] not exist", group, topic);
                        continue;
                    }
                }

                for (int i = 0; i < topicConfig.getWriteQueueNums(); i++) {
                    MessageQueue mq = new MessageQueue();
                    mq.setTopic(topic);
                    mq.setBrokerName(this.brokerController.getBrokerConfig().getBrokerName());
                    mq.setQueueId(i);
                    OffsetWrapper offsetWrapper = new OffsetWrapper();
                    long brokerOffset = this.brokerController.getMessageStore().getMaxOffsetInQueue(topic, i);
                    if (brokerOffset < 0)
                        brokerOffset = 0;
                    long consumerOffset = this.brokerController.getConsumerOffsetManager().queryOffset(
                        group,
                        topic,
                        i);
                    if (consumerOffset < 0)
                        consumerOffset = 0;

                    offsetWrapper.setBrokerOffset(brokerOffset);
                    offsetWrapper.setConsumerOffset(consumerOffset);

                    long timeOffset = consumerOffset - 1;
                    if (timeOffset >= 0) {
                        long lastTimestamp = this.brokerController.getMessageStore().getMessageStoreTimeStamp(topic, i, timeOffset);
                        if (lastTimestamp > 0) {
                            offsetWrapper.setLastTimestamp(lastTimestamp);
                        }
                    }
                    consumeStats.getOffsetTable().put(mq, offsetWrapper);
                }
                double consumeTps = this.brokerController.getBrokerStatsManager().tpsGroupGetNums(group, topic);
                consumeTps += consumeStats.getConsumeTps();
                consumeStats.setConsumeTps(consumeTps);
                totalDiff += consumeStats.computeTotalDiff();
                consumeStatsList.add(consumeStats);
            }
            subscripTopicConsumeMap.put(group, consumeStatsList);
            brokerConsumeStatsList.add(subscripTopicConsumeMap);
        }
        ConsumeStatsList consumeStats = new ConsumeStatsList();
        consumeStats.setBrokerAddr(brokerController.getBrokerAddr());
        consumeStats.setConsumeStatsList(brokerConsumeStatsList);
        consumeStats.setTotalDiff(totalDiff);
        response.setBody(consumeStats.encode());
        response.setCode(SUCCESS);
        response.setRemark(null);
        return response;
    }

    private HashMap<String, String> prepareRuntimeInfo() {
        HashMap<String, String> runtimeInfo = this.brokerController.getMessageStore().getRuntimeInfo();
        runtimeInfo.put("brokerVersionDesc", MQVersion.getVersionDesc(MQVersion.CURRENT_VERSION));
        runtimeInfo.put("brokerVersion", String.valueOf(MQVersion.CURRENT_VERSION));

        runtimeInfo.put("msgPutTotalYesterdayMorning",
            String.valueOf(this.brokerController.getBrokerStats().getMsgPutTotalYesterdayMorning()));
        runtimeInfo.put("msgPutTotalTodayMorning", String.valueOf(this.brokerController.getBrokerStats().getMsgPutTotalTodayMorning()));
        runtimeInfo.put("msgPutTotalTodayNow", String.valueOf(this.brokerController.getBrokerStats().getMsgPutTotalTodayNow()));

        runtimeInfo.put("msgGetTotalYesterdayMorning",
            String.valueOf(this.brokerController.getBrokerStats().getMsgGetTotalYesterdayMorning()));
        runtimeInfo.put("msgGetTotalTodayMorning", String.valueOf(this.brokerController.getBrokerStats().getMsgGetTotalTodayMorning()));
        runtimeInfo.put("msgGetTotalTodayNow", String.valueOf(this.brokerController.getBrokerStats().getMsgGetTotalTodayNow()));

        runtimeInfo.put("sendThreadPoolQueueSize", String.valueOf(this.brokerController.getSendThreadPoolQueue().size()));

        runtimeInfo.put("sendThreadPoolQueueCapacity",
            String.valueOf(this.brokerController.getBrokerConfig().getSendThreadPoolQueueCapacity()));

        runtimeInfo.put("pullThreadPoolQueueSize", String.valueOf(this.brokerController.getPullThreadPoolQueue().size()));
        runtimeInfo.put("pullThreadPoolQueueCapacity",
            String.valueOf(this.brokerController.getBrokerConfig().getPullThreadPoolQueueCapacity()));

        runtimeInfo.put("queryThreadPoolQueueSize", String.valueOf(this.brokerController.getQueryThreadPoolQueue().size()));
        runtimeInfo.put("queryThreadPoolQueueCapacity",
            String.valueOf(this.brokerController.getBrokerConfig().getQueryThreadPoolQueueCapacity()));

        runtimeInfo.put("EndTransactionQueueSize", String.valueOf(this.brokerController.getEndTransactionThreadPoolQueue().size()));
        runtimeInfo.put("EndTransactionThreadPoolQueueCapacity",
            String.valueOf(this.brokerController.getBrokerConfig().getEndTransactionPoolQueueCapacity()));

        runtimeInfo.put("dispatchBehindBytes", String.valueOf(this.brokerController.getMessageStore().dispatchBehindBytes()));
        runtimeInfo.put("pageCacheLockTimeMills", String.valueOf(this.brokerController.getMessageStore().lockTimeMills()));

        runtimeInfo.put("sendThreadPoolQueueHeadWaitTimeMills", String.valueOf(this.brokerController.headSlowTimeMills4SendThreadPoolQueue()));
        runtimeInfo.put("pullThreadPoolQueueHeadWaitTimeMills", String.valueOf(this.brokerController.headSlowTimeMills4PullThreadPoolQueue()));
        runtimeInfo.put("queryThreadPoolQueueHeadWaitTimeMills", String.valueOf(this.brokerController.headSlowTimeMills4QueryThreadPoolQueue()));

        runtimeInfo.put("earliestMessageTimeStamp", String.valueOf(this.brokerController.getMessageStore().getEarliestMessageTime()));
        runtimeInfo.put("startAcceptSendRequestTimeStamp", String.valueOf(this.brokerController.getBrokerConfig().getStartAcceptSendRequestTimeStamp()));
        if (this.brokerController.getMessageStore() instanceof DefaultMessageStore) {
            DefaultMessageStore defaultMessageStore = (DefaultMessageStore) this.brokerController.getMessageStore();
            runtimeInfo.put("remainTransientStoreBufferNumbs", String.valueOf(defaultMessageStore.remainTransientStoreBufferNumbs()));
            if (defaultMessageStore.getMessageStoreConfig().isTransientStorePoolEnable()) {
                runtimeInfo.put("remainHowManyDataToCommit", MixAll.humanReadableByteCount(defaultMessageStore.getCommitLog().remainHowManyDataToCommit(), false));
            }
            runtimeInfo.put("remainHowManyDataToFlush", MixAll.humanReadableByteCount(defaultMessageStore.getCommitLog().remainHowManyDataToFlush(), false));
        }

        java.io.File commitLogDir = new java.io.File(this.brokerController.getMessageStoreConfig().getStorePathRootDir());
        if (commitLogDir.exists()) {
            runtimeInfo.put("commitLogDirCapacity", String.format("Total : %s, Free : %s.", MixAll.humanReadableByteCount(commitLogDir.getTotalSpace(), false), MixAll.humanReadableByteCount(commitLogDir.getFreeSpace(), false)));
        }

        return runtimeInfo;
    }

    /**
     * 调用消费者来消费这个歌消息
     * @param requestCode
     * @param req
     * @param consumerGroup
     * @param clientId
     * @return
     * @throws RemotingCommandException
     */
    private RemotingCommand callConsumer(final int requestCode, final RemotingCommand req, final String consumerGroup, final String clientId) throws RemotingCommandException {
        final RemotingCommand response = createResponseCommand(null);
        //找到对应的channel
        ClientChannelInfo clientChannelInfo = this.brokerController.getConsumerManager().findChannel(consumerGroup, clientId);

        if (null == clientChannelInfo) {
            response.setCode(SYSTEM_ERROR);
            response.setRemark(String.format("The Consumer <%s> <%s> not online", consumerGroup, clientId));
            return response;
        }

        if (clientChannelInfo.getVersion() < MQVersion.Version.V3_1_8_SNAPSHOT.ordinal()) { //3.1.8才出现的特征
            response.setCode(SYSTEM_ERROR);
            response.setRemark(String.format("The Consumer <%s> Version <%s> too low to finish, please upgrade it to V3_1_8_SNAPSHOT",
                clientId, MQVersion.getVersionDesc(clientChannelInfo.getVersion())));
            return response;
        }

        try {
            RemotingCommand newRequest = createRequestCommand(requestCode, null);
            newRequest.setExtFields(req.getExtFields());
            newRequest.setBody(req.getBody());

            //调用
            return this.brokerController.getBroker2Client().callClient(clientChannelInfo.getChannel(), newRequest);
        } catch (RemotingTimeoutException e) {
            response.setCode(ResponseCode.CONSUME_MSG_TIMEOUT);
            response
                .setRemark(String.format("consumer <%s> <%s> Timeout: %s", consumerGroup, clientId, RemotingHelper.exceptionSimpleDesc(e)));
            return response;
        } catch (Exception e) {
            response.setCode(SYSTEM_ERROR);
            response.setRemark(
                String.format("invoke consumer <%s> <%s> Exception: %s", consumerGroup, clientId, RemotingHelper.exceptionSimpleDesc(e)));
            return response;
        }
    }

    private RemotingCommand queryConsumeQueue(ChannelHandlerContext ctx, RemotingCommand req) throws RemotingCommandException {
        QueryConsumeQueueRequestHeader reqHeader = (QueryConsumeQueueRequestHeader) req.decodeCommandCustomHeader(QueryConsumeQueueRequestHeader.class);

        RemotingCommand resp = createResponseCommand(null);

        ConsumeQueue consumeQueue = this.brokerController.getMessageStore().getConsumeQueue(reqHeader.getTopic(), reqHeader.getQueueId());
        if (consumeQueue == null) {
            resp.setCode(SYSTEM_ERROR);
            resp.setRemark(String.format("%d@%s is not exist!", reqHeader.getQueueId(), reqHeader.getTopic()));
            return resp;
        }

        QueryConsumeQueueResponseBody body = new QueryConsumeQueueResponseBody();
        resp.setCode(SUCCESS);
        resp.setBody(body.encode());

        body.setMaxQueueIndex(consumeQueue.getMaxOffsetInQueue());
        body.setMinQueueIndex(consumeQueue.getMinOffsetInQueue());

        MessageFilter messageFilter = null;
        if (reqHeader.getConsumerGroup() != null) {
            SubscriptionData subscriptionData = this.brokerController.getConsumerManager().findSubscriptionData(reqHeader.getConsumerGroup(), reqHeader.getTopic());
            body.setSubscriptionData(subscriptionData);
            if (subscriptionData == null) {
                body.setFilterData(String.format("%s@%s is not online!", reqHeader.getConsumerGroup(), reqHeader.getTopic()));
            } else {
                ConsumerFilterData filterData = this.brokerController.getConsumerFilterManager().get(reqHeader.getTopic(), reqHeader.getConsumerGroup());
                body.setFilterData(JSON.toJSONString(filterData, true));

                messageFilter = new ExpressionMessageFilter(subscriptionData, filterData,
                    this.brokerController.getConsumerFilterManager());
            }
        }

        SelectMappedBufferResult result = consumeQueue.getIndexBuffer(reqHeader.getIndex());
        if (result == null) {
            resp.setRemark(String.format("Index %d of %d@%s is not exist!", reqHeader.getIndex(), reqHeader.getQueueId(), reqHeader.getTopic()));
            return resp;
        }
        try {
            List<ConsumeQueueData> queues = new ArrayList<>();
            for (int i = 0; i < result.getSize() && i < reqHeader.getCount() * ConsumeQueue.CQ_STORE_UNIT_SIZE; i += ConsumeQueue.CQ_STORE_UNIT_SIZE) {
                ConsumeQueueData one = new ConsumeQueueData();
                one.setPhysicOffset(result.getByteBuffer().getLong());
                one.setPhysicSize(result.getByteBuffer().getInt());
                one.setTagsCode(result.getByteBuffer().getLong());

                if (!consumeQueue.isExtAddr(one.getTagsCode())) {
                    queues.add(one);
                    continue;
                }

                ConsumeQueueExt.CqExtUnit cqExtUnit = consumeQueue.getExt(one.getTagsCode());
                if (cqExtUnit != null) {
                    one.setExtendDataJson(JSON.toJSONString(cqExtUnit));
                    if (cqExtUnit.getFilterBitMap() != null) {
                        one.setBitMap(BitsArray.create(cqExtUnit.getFilterBitMap()).toString());
                    }
                    if (messageFilter != null) {
                        one.setEval(messageFilter.isMatchedByConsumeQueue(cqExtUnit.getTagsCode(), cqExtUnit));
                    }
                } else {
                    one.setMsg("Cq extend not exist!addr: " + one.getTagsCode());
                }

                queues.add(one);
            }
            body.setQueueData(queues);
        } finally {
            result.release();
        }

        return resp;
    }
}
