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

import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.FileRegion;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.broker.pagecache.OneMessageTransfer;
import org.apache.rocketmq.broker.pagecache.QueryMessageTransfer;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.common.protocol.RequestCode;
import org.apache.rocketmq.common.protocol.ResponseCode;
import org.apache.rocketmq.common.protocol.header.QueryMessageRequestHeader;
import org.apache.rocketmq.common.protocol.header.QueryMessageResponseHeader;
import org.apache.rocketmq.common.protocol.header.ViewMessageRequestHeader;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;
import org.apache.rocketmq.remoting.netty.NettyRequestProcessor;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.store.QueryMessageResult;
import org.apache.rocketmq.store.SelectMappedBufferResult;

import static org.apache.rocketmq.remoting.protocol.RemotingCommand.createResponseCommand;

/**
 * push方式的消费，使用处理
 */
public class QueryMessageProcessor implements NettyRequestProcessor {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.BROKER_LOGGER_NAME);

    private final BrokerController brokerController;

    public QueryMessageProcessor(final BrokerController brokerController) {
        this.brokerController = brokerController;
    }

    /**
     * 查询消息处理，支持消息id和消息key方式的查询
     * @param ctx
     * @param req
     * @return
     * @throws RemotingCommandException
     */
    @Override
    public RemotingCommand processRequest(ChannelHandlerContext ctx, RemotingCommand req)
        throws RemotingCommandException {
        switch (req.getCode()) {
            case RequestCode.QUERY_MESSAGE: //通过消息的key进行查找
                return this.queryMessage(ctx, req);
            case RequestCode.VIEW_MESSAGE_BY_ID: //通过消息id进行查询
                return this.viewMessageById(ctx, req);
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
     * 主动的查询消息
     * @param ctx
     * @param req
     * @return
     * @throws RemotingCommandException
     */
    public RemotingCommand queryMessage(ChannelHandlerContext ctx, RemotingCommand req)
        throws RemotingCommandException {
        final RemotingCommand resp = createResponseCommand(QueryMessageResponseHeader.class);
        final QueryMessageResponseHeader respHeader = (QueryMessageResponseHeader) resp.readCustomHeader();
        final QueryMessageRequestHeader reqHeader = (QueryMessageRequestHeader) req.decodeCommandCustomHeader(QueryMessageRequestHeader.class);

        resp.setOpaque(req.getOpaque());

        String isUniqueKey = req.getExtFields().get(MixAll.UNIQUE_MSG_QUERY_FLAG); //uniqueKey
        if (isUniqueKey != null && isUniqueKey.equals("true")) {
            reqHeader.setMaxNum(this.brokerController.getMessageStoreConfig().getDefaultQueryMaxNum());
        }

        //构建查询结果，提供对消息存储的直接查询
        final QueryMessageResult queryResult = this.brokerController.getMessageStore().queryMessage(
                reqHeader.getTopic(), reqHeader.getKey(), reqHeader.getMaxNum(), reqHeader.getBeginTimestamp(), reqHeader.getEndTimestamp());
        assert queryResult != null;

        respHeader.setIndexLastUpdatePhyoffset(queryResult.getIndexLastUpdatePhyoffset());
        respHeader.setIndexLastUpdateTimestamp(queryResult.getIndexLastUpdateTimestamp());

        if (queryResult.getBufferTotalSize() > 0) {
            resp.setCode(ResponseCode.SUCCESS);
            resp.setRemark(null);

            try {
                FileRegion fileRegion = new QueryMessageTransfer(resp.encodeHeader(queryResult.getBufferTotalSize()), queryResult);
                ctx.channel().writeAndFlush(fileRegion).addListener((ChannelFutureListener) future -> {
                    queryResult.release();
                    if (!future.isSuccess()) {
                        log.error("transfer query message by page cache failed, ", future.cause());
                    }
                });
            } catch (Throwable e) {
                log.error("", e);
                queryResult.release();
            }

            return null;
        }

        resp.setCode(ResponseCode.QUERY_NOT_FOUND);
        resp.setRemark("can not find message, maybe time range not correct");
        return resp;
    }

    /**
     * 通过消息id进行查询，
     * 我们知道消息id存储了存储主机的broker和其offset
     * @param ctx
     * @param req
     * @return
     * @throws RemotingCommandException
     */
    public RemotingCommand viewMessageById(ChannelHandlerContext ctx, RemotingCommand req)
        throws RemotingCommandException {
        final RemotingCommand resp = createResponseCommand(null);
        final ViewMessageRequestHeader reqHeader =
            (ViewMessageRequestHeader) req.decodeCommandCustomHeader(ViewMessageRequestHeader.class);

        resp.setOpaque(req.getOpaque());

        //直接使用offset找到对应的缓冲区
        final SelectMappedBufferResult selectMappedBufferResult = this.brokerController.getMessageStore().selectOneMessageByOffset(reqHeader.getOffset());
        if (selectMappedBufferResult != null) {
            resp.setCode(ResponseCode.SUCCESS);
            resp.setRemark(null);

            //直接使用FileRegion进行输出
            try {
                FileRegion fileRegion =
                    new OneMessageTransfer(resp.encodeHeader(selectMappedBufferResult.getSize()), selectMappedBufferResult);
                ctx.channel().writeAndFlush(fileRegion).addListener((ChannelFutureListener) future -> {
                    selectMappedBufferResult.release();
                    if (!future.isSuccess()) {
                        log.error("Transfer one message from page cache failed, ", future.cause());
                    }
                });
            } catch (Throwable e) {
                log.error("", e);
                selectMappedBufferResult.release();
            }

            return null;
        } else {
            resp.setCode(ResponseCode.SYSTEM_ERROR);
            resp.setRemark("can not find message by the offset, " + reqHeader.getOffset());
        }

        return resp;
    }
}
