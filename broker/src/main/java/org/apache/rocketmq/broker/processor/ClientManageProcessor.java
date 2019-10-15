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
import org.apache.rocketmq.broker.client.ClientChannelInfo;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.constant.PermName;
import org.apache.rocketmq.common.filter.ExpressionType;
import org.apache.rocketmq.common.protocol.RequestCode;
import org.apache.rocketmq.common.protocol.ResponseCode;
import org.apache.rocketmq.common.protocol.body.CheckClientRequestBody;
import org.apache.rocketmq.common.protocol.header.UnregisterClientRequestHeader;
import org.apache.rocketmq.common.protocol.header.UnregisterClientResponseHeader;
import org.apache.rocketmq.common.protocol.heartbeat.ConsumerData;
import org.apache.rocketmq.common.protocol.heartbeat.HeartbeatData;
import org.apache.rocketmq.common.protocol.heartbeat.ProducerData;
import org.apache.rocketmq.common.protocol.heartbeat.SubscriptionData;
import org.apache.rocketmq.common.subscription.SubscriptionGroupConfig;
import org.apache.rocketmq.common.sysflag.TopicSysFlag;
import org.apache.rocketmq.filter.FilterFactory;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;
import org.apache.rocketmq.remoting.netty.NettyRequestProcessor;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;

import static org.apache.rocketmq.common.protocol.RequestCode.CHECK_CLIENT_CONFIG;
import static org.apache.rocketmq.common.protocol.RequestCode.HEART_BEAT;
import static org.apache.rocketmq.common.protocol.RequestCode.UNREGISTER_CLIENT;
import static org.apache.rocketmq.remoting.common.RemotingHelper.parseChannelRemoteAddr;

/**
 * 客户端管理操作器，提供了对客户端的管理，主要处理客户端的心跳，客户端注销，注册等等相关请求
 */
public class ClientManageProcessor implements NettyRequestProcessor {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.BROKER_LOGGER_NAME);
    private final BrokerController brokerController;

    public ClientManageProcessor(final BrokerController brokerController) {
        this.brokerController = brokerController;
    }

    @Override
    public RemotingCommand processRequest(ChannelHandlerContext ctx, RemotingCommand req)
        throws RemotingCommandException {
        switch (req.getCode()) {
            case HEART_BEAT: //处理心跳
                return this.heartBeat(ctx, req);
            case UNREGISTER_CLIENT: //注销客户端
                return this.unregisterClient(ctx, req);
            case CHECK_CLIENT_CONFIG: //校验客户端的配置
                return this.checkClientConfig(ctx, req);
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
     * 处理客户端上来的心跳信息
     * @param ctx
     * @param req
     * @return
     */
    public RemotingCommand heartBeat(ChannelHandlerContext ctx, RemotingCommand req) {

        RemotingCommand resp = RemotingCommand.createResponseCommand(null);
        HeartbeatData heartbeatData = HeartbeatData.decode(req.getBody(), HeartbeatData.class);
        //构建客户网络信息
        ClientChannelInfo channelInfo = new ClientChannelInfo(ctx.channel(), heartbeatData.getClientID(), req.getLanguage(), req.getVersion());

        // 尝试处理消息端数据，即处理ConsumerData数据
        for (ConsumerData data : heartbeatData.getConsumerDataSet()) {
            //发现消费组对应订阅的配置
            SubscriptionGroupConfig subscriptionGroupConfig = this.brokerController.getSubscriptionGroupManager().findSubscriptionGroupConfig(data.getGroupName());
            boolean isNotifyConsumerIdsChangedEnable = true;
            //是否支持回调通知该消费组下面组成的消费者客户端
            if (null != subscriptionGroupConfig) {
                //存在相关配置，我们使用这个配置来确定这个开关
                isNotifyConsumerIdsChangedEnable = subscriptionGroupConfig.isNotifyConsumerIdsChangedEnable();
                int topicSysFlag = 0;
                if (data.isUnitMode()) {
                    topicSysFlag = TopicSysFlag.buildSysFlag(false, true);
                }
                //获得一个重试的topic，用于构建消息重发的这个消费组的特性的topic
                String newTopic = MixAll.getRetryTopic(data.getGroupName());
                this.brokerController.getTopicConfigManager().createTopicInSendMessageBackMethod(newTopic, subscriptionGroupConfig.getRetryQueueNums(), PermName.PERM_WRITE | PermName.PERM_READ, topicSysFlag);
            }

            /**
             * 注册相关的的consumer，返回是否发生了相关的变更
             */
            boolean changed = this.brokerController.getConsumerManager().registerConsumer(
                data.getGroupName(), channelInfo, data.getConsumeType(), data.getMessageModel(), data.getConsumeFromWhere(),
                data.getSubscriptionDataSet(), isNotifyConsumerIdsChangedEnable);

            if (changed) {
                //如果发生了变更，我们要简单的输出一些信息
                log.info("registerConsumer info changed {} {}", data.toString(), parseChannelRemoteAddr(ctx.channel()));
            }
        }

        // 尝试处生产端数据，即处理ProducerData数据
        for (ProducerData data : heartbeatData.getProducerDataSet()) {
            this.brokerController.getProducerManager().registerProducer(data.getGroupName(), channelInfo);
        }

        //返回相应
        resp.setCode(ResponseCode.SUCCESS);
        resp.setRemark(null);
        return resp;
    }

    /**
     * 注销客户端
     * @param ctx
     * @param req
     * @return
     * @throws RemotingCommandException
     */
    public RemotingCommand unregisterClient(ChannelHandlerContext ctx, RemotingCommand req) throws RemotingCommandException {
        final RemotingCommand resp = RemotingCommand.createResponseCommand(UnregisterClientResponseHeader.class);
        final UnregisterClientRequestHeader reqHeader = (UnregisterClientRequestHeader) req.decodeCommandCustomHeader(UnregisterClientRequestHeader.class);
        //构建客户网络信息
        ClientChannelInfo channelInfo = new ClientChannelInfo(ctx.channel(), reqHeader.getClientID(), req.getLanguage(), req.getVersion());
        //尝试注销producer
        {
            final String group = reqHeader.getProducerGroup();
            if (group != null) {
                this.brokerController.getProducerManager().unregisterProducer(group, channelInfo);
            }
        }

        //尝试注销consumer
        {
            final String group = reqHeader.getConsumerGroup();
            if (group != null) {
                SubscriptionGroupConfig subscriptionGroupConfig = this.brokerController.getSubscriptionGroupManager().findSubscriptionGroupConfig(group);
                boolean isNotifyConsumerIdsChangedEnable = true;
                if (null != subscriptionGroupConfig) {
                    isNotifyConsumerIdsChangedEnable = subscriptionGroupConfig.isNotifyConsumerIdsChangedEnable(); //是否进行通知
                }
                this.brokerController.getConsumerManager().unregisterConsumer(group, channelInfo, isNotifyConsumerIdsChangedEnable);
            }
        }

        //返回响应
        resp.setCode(ResponseCode.SUCCESS);
        resp.setRemark(null);
        return resp;
    }

    /**
     * 校验客户端的配置
     * @param ctx
     * @param req
     * @return
     * @throws RemotingCommandException
     */
    public RemotingCommand checkClientConfig(ChannelHandlerContext ctx, RemotingCommand req) throws RemotingCommandException {
        final RemotingCommand resp = RemotingCommand.createResponseCommand(null);
        CheckClientRequestBody reqBody = CheckClientRequestBody.decode(req.getBody(), CheckClientRequestBody.class);

        //我们检查一个客户端的请求
        if (reqBody != null && reqBody.getSubscriptionData() != null) {

            SubscriptionData subscriptionData = reqBody.getSubscriptionData();

            if (ExpressionType.isTagType(subscriptionData.getExpressionType())) { //tag，系统默认支持，直接返回
                resp.setCode(ResponseCode.SUCCESS);
                resp.setRemark(null);
                return resp;
            }

            if (!this.brokerController.getBrokerConfig().isEnablePropertyFilter()) { //broker不支持属性filter，因此该broker不支持其客户端相关filter操作
                resp.setCode(ResponseCode.SYSTEM_ERROR);
                resp.setRemark("The broker does not support consumer to filter message by " + subscriptionData.getExpressionType());
                return resp;
            }

            try {
                FilterFactory.INSTANCE.get(subscriptionData.getExpressionType()).compile(subscriptionData.getSubString()); //测试一下，本broker是否支持连上了的filter相关类型，存在并能处理，我们返回
            } catch (Exception e) {
                log.warn("Client {}@{} filter message, but failed to compile expression! sub={}, error={}",
                    reqBody.getClientId(), reqBody.getGroup(), reqBody.getSubscriptionData(), e.getMessage());
                resp.setCode(ResponseCode.SUBSCRIPTION_PARSE_FAILED);
                resp.setRemark(e.getMessage());
                return resp;
            }
        }

        resp.setCode(ResponseCode.SUCCESS);
        resp.setRemark(null);
        return resp;
    }
}
