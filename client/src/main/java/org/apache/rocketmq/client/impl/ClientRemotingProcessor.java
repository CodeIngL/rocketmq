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
package org.apache.rocketmq.client.impl;

import io.netty.channel.ChannelHandlerContext;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import org.apache.rocketmq.client.impl.factory.MQClientInstance;
import org.apache.rocketmq.client.impl.producer.MQProducerInner;
import org.apache.rocketmq.client.log.ClientLogger;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.protocol.RequestCode;
import org.apache.rocketmq.common.protocol.ResponseCode;
import org.apache.rocketmq.common.protocol.body.ConsumeMessageDirectlyResult;
import org.apache.rocketmq.common.protocol.body.ConsumerRunningInfo;
import org.apache.rocketmq.common.protocol.body.GetConsumerStatusBody;
import org.apache.rocketmq.common.protocol.body.ResetOffsetBody;
import org.apache.rocketmq.common.protocol.header.CheckTransactionStateRequestHeader;
import org.apache.rocketmq.common.protocol.header.ConsumeMessageDirectlyResultRequestHeader;
import org.apache.rocketmq.common.protocol.header.GetConsumerRunningInfoRequestHeader;
import org.apache.rocketmq.common.protocol.header.GetConsumerStatusRequestHeader;
import org.apache.rocketmq.common.protocol.header.NotifyConsumerIdsChangedRequestHeader;
import org.apache.rocketmq.common.protocol.header.ResetOffsetRequestHeader;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;
import org.apache.rocketmq.remoting.netty.NettyRequestProcessor;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;

import static org.apache.rocketmq.common.protocol.RequestCode.*;
import static org.apache.rocketmq.remoting.common.RemotingHelper.parseChannelRemoteAddr;
import static org.apache.rocketmq.remoting.protocol.RemotingCommand.createResponseCommand;


/**
 * 客户端处理由broker发送的信息
 */
public class ClientRemotingProcessor implements NettyRequestProcessor {

    private final InternalLogger log = ClientLogger.getLog();

    private final MQClientInstance mqClientFactory;

    public ClientRemotingProcessor(final MQClientInstance mqClientFactory) {
        this.mqClientFactory = mqClientFactory;
    }

    /**
     * 处理来自broker的请求
     * @param ctx
     * @param req
     * @return
     * @throws RemotingCommandException
     */
    @Override
    public RemotingCommand processRequest(ChannelHandlerContext ctx, RemotingCommand req) throws RemotingCommandException {
        switch (req.getCode()) {
            case CHECK_TRANSACTION_STATE:
                //校验事务状态，由broker进行对客户端的回查
                return this.checkTransactionState(ctx, req);
            case NOTIFY_CONSUMER_IDS_CHANGED:
                //消费端组发生更改，由broker进行对客户端的通知
                return this.notifyConsumerIdsChanged(ctx, req);
            case RESET_CONSUMER_CLIENT_OFFSET:
                //重置消费客户端的offset的，由broker要求，broker则是由相关管理端要求
                return this.resetOffset(ctx, req);
            case GET_CONSUMER_STATUS_FROM_CLIENT:
                //获得消费客户端的状态，有broker要求，broker则是由相关管理端要求
                return this.getConsumeStatus(ctx, req);
            case GET_CONSUMER_RUNNING_INFO:
                //获得消费客户端的运行时状态，有broker要求，broker则是由相关管理端要求
                return this.getConsumerRunningInfo(ctx, req);
            case CONSUME_MESSAGE_DIRECTLY:
                //要求消费客户端直接消费消息，有broker要求，broker则是由相关管理端要求
                return this.consumeMessageDirectly(ctx, req);
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
     * 校验事务状态
     * @param ctx
     * @param req
     * @return
     * @throws RemotingCommandException
     */
    public RemotingCommand checkTransactionState(ChannelHandlerContext ctx, RemotingCommand req) throws RemotingCommandException {
        final CheckTransactionStateRequestHeader reqHeader = (CheckTransactionStateRequestHeader) req.decodeCommandCustomHeader(CheckTransactionStateRequestHeader.class);
        final ByteBuffer byteBuffer = ByteBuffer.wrap(req.getBody());
        final MessageExt messageExt = MessageDecoder.decode(byteBuffer);
        if (messageExt != null) {
            //消息的事务id
            String transactionId = messageExt.getProperty(MessageConst.PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX);
            if (null != transactionId && !"".equals(transactionId)) {
                messageExt.setTransactionId(transactionId);
            }
            //消息所属生产组
            final String group = messageExt.getProperty(MessageConst.PROPERTY_PRODUCER_GROUP);
            if (group != null) {
                //找到相应的生产者
                MQProducerInner producer = this.mqClientFactory.selectProducer(group);
                if (producer != null) {
                    final String addr = parseChannelRemoteAddr(ctx.channel());
                    //由生产者校验事务状态
                    producer.checkTransactionState(addr, messageExt, reqHeader);
                } else {
                    log.debug("checkTransactionState, pick producer by group[{}] failed", group);
                }
            } else {
                log.warn("checkTransactionState, pick producer group failed");
            }
        } else {
            log.warn("checkTransactionState, decode message failed");
        }

        return null;
    }

    /**
     * 通知客户端相关consumerId发生了变更。我们的rebalance需要进行及时的重新负载均衡
     * @param ctx
     * @param req
     * @return
     * @throws RemotingCommandException
     */
    public RemotingCommand notifyConsumerIdsChanged(ChannelHandlerContext ctx, RemotingCommand req) throws RemotingCommandException {
        try {
            final NotifyConsumerIdsChangedRequestHeader reqHeader = (NotifyConsumerIdsChangedRequestHeader) req.decodeCommandCustomHeader(NotifyConsumerIdsChangedRequestHeader.class);
            log.info("receive broker's notification[{}], the consumer group: {} changed, rebalance immediately", parseChannelRemoteAddr(ctx.channel()), reqHeader.getConsumerGroup());
            //立即发生重平衡操作，甚至忽略了唯一的参数也就是consumerGroup
            this.mqClientFactory.rebalanceImmediately();
        } catch (Exception e) {
            log.error("notifyConsumerIdsChanged exception", RemotingHelper.exceptionSimpleDesc(e));
        }
        return null;
    }

    /**
     * 重置客户端的offset，这里的逻辑有客户端本身操作
     * @param ctx
     * @param request
     * @return
     * @throws RemotingCommandException
     */
    public RemotingCommand resetOffset(ChannelHandlerContext ctx, RemotingCommand request) throws RemotingCommandException {
        final ResetOffsetRequestHeader requestHeader = (ResetOffsetRequestHeader) request.decodeCommandCustomHeader(ResetOffsetRequestHeader.class);
        log.info("invoke reset offset operation from broker. brokerAddr={}, topic={}, group={}, timestamp={}",
            parseChannelRemoteAddr(ctx.channel()), requestHeader.getTopic(), requestHeader.getGroup(),
            requestHeader.getTimestamp());
        Map<MessageQueue, Long> offsetTable = new HashMap<MessageQueue, Long>();
        if (request.getBody() != null) {
            ResetOffsetBody body = ResetOffsetBody.decode(request.getBody(), ResetOffsetBody.class);
            offsetTable = body.getOffsetTable();
        }
        this.mqClientFactory.resetOffset(requestHeader.getTopic(), requestHeader.getGroup(), offsetTable);
        return null;
    }

    @Deprecated
    public RemotingCommand getConsumeStatus(ChannelHandlerContext ctx,
        RemotingCommand request) throws RemotingCommandException {
        final RemotingCommand response = createResponseCommand(null);
        final GetConsumerStatusRequestHeader requestHeader =
            (GetConsumerStatusRequestHeader) request.decodeCommandCustomHeader(GetConsumerStatusRequestHeader.class);

        Map<MessageQueue, Long> offsetTable = this.mqClientFactory.getConsumerStatus(requestHeader.getTopic(), requestHeader.getGroup());
        GetConsumerStatusBody body = new GetConsumerStatusBody();
        body.setMessageQueueTable(offsetTable);
        response.setBody(body.encode());
        response.setCode(ResponseCode.SUCCESS);
        return response;
    }

    /**
     * 获得consumer运行时的信息
     * @param ctx
     * @param req
     * @return
     * @throws RemotingCommandException
     */
    private RemotingCommand getConsumerRunningInfo(ChannelHandlerContext ctx,
        RemotingCommand req) throws RemotingCommandException {
        final RemotingCommand resp = createResponseCommand(null);
        final GetConsumerRunningInfoRequestHeader reqHeader =
            (GetConsumerRunningInfoRequestHeader) req.decodeCommandCustomHeader(GetConsumerRunningInfoRequestHeader.class);

        ConsumerRunningInfo consumerRunningInfo = this.mqClientFactory.consumerRunningInfo(reqHeader.getConsumerGroup());
        if (null != consumerRunningInfo) {
            if (reqHeader.isJstackEnable()) {
                Map<Thread, StackTraceElement[]> map = Thread.getAllStackTraces();
                String jstack = UtilAll.jstack(map);
                consumerRunningInfo.setJstack(jstack);
            }

            resp.setCode(ResponseCode.SUCCESS);
            resp.setBody(consumerRunningInfo.encode());
        } else {
            resp.setCode(ResponseCode.SYSTEM_ERROR);
            resp.setRemark(String.format("The Consumer Group <%s> not exist in this consumer", reqHeader.getConsumerGroup()));
        }

        return resp;
    }

    /**
     * 直接消费消息
     * @param ctx
     * @param req
     * @return
     * @throws RemotingCommandException
     */
    private RemotingCommand consumeMessageDirectly(ChannelHandlerContext ctx, RemotingCommand req) throws RemotingCommandException {
        final RemotingCommand resp = createResponseCommand(null);
        final ConsumeMessageDirectlyResultRequestHeader reqHeader = (ConsumeMessageDirectlyResultRequestHeader) req.decodeCommandCustomHeader(ConsumeMessageDirectlyResultRequestHeader.class);

        final MessageExt msg = MessageDecoder.decode(ByteBuffer.wrap(req.getBody()));

        //直接写消费消息
        ConsumeMessageDirectlyResult result = this.mqClientFactory.consumeMessageDirectly(msg, reqHeader.getConsumerGroup(), reqHeader.getBrokerName());

        if (null != result) {
            resp.setCode(ResponseCode.SUCCESS);
            resp.setBody(result.encode());
        } else {
            resp.setCode(ResponseCode.SYSTEM_ERROR);
            resp.setRemark(String.format("The Consumer Group <%s> not exist in this consumer", reqHeader.getConsumerGroup()));
        }

        return resp;
    }
}
