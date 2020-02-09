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
import java.util.List;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.broker.client.ConsumerGroupInfo;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.common.protocol.RequestCode;
import org.apache.rocketmq.common.protocol.ResponseCode;
import org.apache.rocketmq.common.protocol.header.GetConsumerListByGroupRequestHeader;
import org.apache.rocketmq.common.protocol.header.GetConsumerListByGroupResponseBody;
import org.apache.rocketmq.common.protocol.header.GetConsumerListByGroupResponseHeader;
import org.apache.rocketmq.common.protocol.header.QueryConsumerOffsetRequestHeader;
import org.apache.rocketmq.common.protocol.header.QueryConsumerOffsetResponseHeader;
import org.apache.rocketmq.common.protocol.header.UpdateConsumerOffsetRequestHeader;
import org.apache.rocketmq.common.protocol.header.UpdateConsumerOffsetResponseHeader;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;
import org.apache.rocketmq.remoting.netty.NettyRequestProcessor;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;

import static org.apache.rocketmq.common.protocol.RequestCode.GET_CONSUMER_LIST_BY_GROUP;
import static org.apache.rocketmq.common.protocol.RequestCode.QUERY_CONSUMER_OFFSET;
import static org.apache.rocketmq.common.protocol.RequestCode.UPDATE_CONSUMER_OFFSET;
import static org.apache.rocketmq.remoting.common.RemotingHelper.parseChannelRemoteAddr;
import static org.apache.rocketmq.remoting.protocol.RemotingCommand.createResponseCommand;

/**
 * 消费端的管理器，管理连接到该broker节点上的相关的consumer信息
 * 提供查询消费组相关信息，和更新消费组相关的offset
 */
public class ConsumerManageProcessor implements NettyRequestProcessor {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.BROKER_LOGGER_NAME);

    private final BrokerController brokerController;

    public ConsumerManageProcessor(final BrokerController brokerController) {
        this.brokerController = brokerController;
    }

    //都是由消费端本身发起
    @Override
    public RemotingCommand processRequest(ChannelHandlerContext ctx, RemotingCommand req) throws RemotingCommandException {
        switch (req.getCode()) {
            case GET_CONSUMER_LIST_BY_GROUP: //查询消费组中消费端列表
                return this.getConsumerListByGroup(ctx, req);
            case UPDATE_CONSUMER_OFFSET: //更新消费端的offset
                return this.updateConsumerOffset(ctx, req);
            case QUERY_CONSUMER_OFFSET: //查询消费offset
                return this.queryConsumerOffset(ctx, req);
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
     * 获得对应的consumer列表
     * @param ctx
     * @param req
     * @return
     * @throws RemotingCommandException
     */
    public RemotingCommand getConsumerListByGroup(ChannelHandlerContext ctx, RemotingCommand req)
        throws RemotingCommandException {
        final RemotingCommand resp = createResponseCommand(GetConsumerListByGroupResponseHeader.class);
        final GetConsumerListByGroupRequestHeader reqHeader = (GetConsumerListByGroupRequestHeader) req.decodeCommandCustomHeader(GetConsumerListByGroupRequestHeader.class);

        //获得消费组的信息
        ConsumerGroupInfo consumerGroupInfo = this.brokerController.getConsumerManager().getConsumerGroupInfo(reqHeader.getConsumerGroup());
        if (consumerGroupInfo != null) {
            //存在相关信息
            List<String> clientIds = consumerGroupInfo.getAllClientId();
            //获得所有的clientId
            if (!clientIds.isEmpty()) {
                GetConsumerListByGroupResponseBody body = new GetConsumerListByGroupResponseBody();
                body.setConsumerIdList(clientIds);
                resp.setBody(body.encode());
                resp.setCode(ResponseCode.SUCCESS);
                resp.setRemark(null);
                return resp;
            } else {
                log.warn("getAllClientId failed, {} {}", reqHeader.getConsumerGroup(), parseChannelRemoteAddr(ctx.channel()));
            }
        } else {
            log.warn("getConsumerGroupInfo failed, {} {}", reqHeader.getConsumerGroup(), parseChannelRemoteAddr(ctx.channel()));
        }
        resp.setRemark("no consumer for this group, " + reqHeader.getConsumerGroup());
        return resp;
    }

    /**
     * 更新offset
     * @param ctx
     * @param req
     * @return
     * @throws RemotingCommandException
     */
    private RemotingCommand updateConsumerOffset(ChannelHandlerContext ctx, RemotingCommand req)
        throws RemotingCommandException {
        final RemotingCommand resp = createResponseCommand(UpdateConsumerOffsetResponseHeader.class);
        final UpdateConsumerOffsetRequestHeader reqHeader = (UpdateConsumerOffsetRequestHeader) req.decodeCommandCustomHeader(UpdateConsumerOffsetRequestHeader.class);
        this.brokerController.getConsumerOffsetManager().commitOffset(parseChannelRemoteAddr(ctx.channel()), reqHeader.getConsumerGroup(),
            reqHeader.getTopic(), reqHeader.getQueueId(), reqHeader.getCommitOffset());
        resp.setCode(ResponseCode.SUCCESS);
        resp.setRemark(null);
        return resp;
    }

    /**
     * 查询要消费的offset
     * @param ctx
     * @param req
     * @return
     * @throws RemotingCommandException
     */
    private RemotingCommand queryConsumerOffset(ChannelHandlerContext ctx, RemotingCommand req) throws RemotingCommandException {
        final RemotingCommand resp = createResponseCommand(QueryConsumerOffsetResponseHeader.class);
        final QueryConsumerOffsetResponseHeader respHeader = (QueryConsumerOffsetResponseHeader) resp.readCustomHeader();
        final QueryConsumerOffsetRequestHeader reqHeader = (QueryConsumerOffsetRequestHeader) req.decodeCommandCustomHeader(QueryConsumerOffsetRequestHeader.class);

        //查询相关offset
        long offset = this.brokerController.getConsumerOffsetManager().queryOffset(reqHeader.getConsumerGroup(), reqHeader.getTopic(), reqHeader.getQueueId());

        if (offset >= 0) {
            //存在offset,直接返回offset
            respHeader.setOffset(offset);
            resp.setCode(ResponseCode.SUCCESS);
            resp.setRemark(null);
        } else {
            //最小
            long minOffset = this.brokerController.getMessageStore().getMinOffsetInQueue(reqHeader.getTopic(), reqHeader.getQueueId());
            if (minOffset <= 0 && !this.brokerController.getMessageStore().checkInDiskByConsumeOffset(reqHeader.getTopic(), reqHeader.getQueueId(), 0)) {
                respHeader.setOffset(0L);
                resp.setCode(ResponseCode.SUCCESS);
                resp.setRemark(null);
            } else {
                //找不到
                resp.setCode(ResponseCode.QUERY_NOT_FOUND);
                resp.setRemark("Not found, V3_0_6_SNAPSHOT maybe this group consumer boot first");
            }
        }

        return resp;
    }
}
