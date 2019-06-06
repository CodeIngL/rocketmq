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
import org.apache.rocketmq.broker.transaction.OperationResult;
import org.apache.rocketmq.broker.transaction.TransactionalMessageService;
import org.apache.rocketmq.common.TopicFilterType;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.message.MessageAccessor;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.protocol.ResponseCode;
import org.apache.rocketmq.common.protocol.header.EndTransactionRequestHeader;
import org.apache.rocketmq.common.sysflag.MessageSysFlag;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;
import org.apache.rocketmq.remoting.netty.NettyRequestProcessor;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.store.MessageExtBrokerInner;
import org.apache.rocketmq.store.PutMessageResult;
import org.apache.rocketmq.store.config.BrokerRole;

import static org.apache.rocketmq.common.sysflag.MessageSysFlag.*;
import static org.apache.rocketmq.remoting.common.RemotingHelper.parseChannelRemoteAddr;
import static org.apache.rocketmq.remoting.protocol.RemotingSysResponseCode.SUCCESS;

/**
 * EndTransaction processor: process commit and rollback message
 * <p>
 *     EndTransaction处理器：负责处理提交和回滚消息
 * </p>
 */
public class EndTransactionProcessor implements NettyRequestProcessor {
    private static final InternalLogger LOGGER = InternalLoggerFactory.getLogger(LoggerName.TRANSACTION_LOGGER_NAME);
    private final BrokerController brokerController;

    public EndTransactionProcessor(final BrokerController brokerController) {
        this.brokerController = brokerController;
    }

    @Override
    public RemotingCommand processRequest(ChannelHandlerContext ctx, RemotingCommand req) throws
        RemotingCommandException {
        final RemotingCommand resp = RemotingCommand.createResponseCommand(null);
        final EndTransactionRequestHeader reqHeader = (EndTransactionRequestHeader)req.decodeCommandCustomHeader(EndTransactionRequestHeader.class);
        LOGGER.info("Transaction request:{}", reqHeader);
        //slave节点禁止处理事务消息
        if (BrokerRole.SLAVE == brokerController.getMessageStoreConfig().getBrokerRole()) {
            resp.setCode(ResponseCode.SLAVE_NOT_AVAILABLE);
            LOGGER.warn("Message store is slave mode, so end transaction is forbidden. ");
            return resp;
        }

        Integer transactionType = reqHeader.getCommitOrRollback();

        if (reqHeader.getFromTransactionCheck()) {
            switch (transactionType) {
                case TRANSACTION_NOT_TYPE: { //不是事务消息
                    LOGGER.warn("Check producer[{}] transaction state, but it's pending status.RequestHeader: {} Remark: {}",
                        parseChannelRemoteAddr(ctx.channel()), reqHeader.toString(), req.getRemark());
                    return null;
                }

                case TRANSACTION_COMMIT_TYPE: {
                    LOGGER.warn("Check producer[{}] transaction state, the producer commit the message.RequestHeader: {} Remark: {}",
                        parseChannelRemoteAddr(ctx.channel()), reqHeader.toString(), req.getRemark());
                    break;
                }

                case TRANSACTION_ROLLBACK_TYPE: {
                    LOGGER.warn("Check producer[{}] transaction state, the producer rollback the message.RequestHeader: {} Remark: {}",
                        parseChannelRemoteAddr(ctx.channel()), reqHeader.toString(), req.getRemark());
                    break;
                }
                default:
                    return null;
            }
        } else {
            switch (transactionType) {
                case TRANSACTION_NOT_TYPE: {
                    LOGGER.warn("The producer[{}] end transaction in sending message,  and it's pending status.RequestHeader: {} Remark: {}",
                        parseChannelRemoteAddr(ctx.channel()), reqHeader.toString(), req.getRemark());
                    return null;
                }
                case TRANSACTION_COMMIT_TYPE:
                    break;
                case TRANSACTION_ROLLBACK_TYPE: {
                    LOGGER.warn("The producer[{}] end transaction in sending message, rollback the message.RequestHeader: {} Remark: {}",
                        parseChannelRemoteAddr(ctx.channel()), reqHeader.toString(), req.getRemark());
                    break;
                }
                default:
                    return null;
            }
        }
        //操作结果
        OperationResult result = new OperationResult();
        TransactionalMessageService transactionalService = this.brokerController.getTransactionalMessageService();
        if (TRANSACTION_COMMIT_TYPE == transactionType) { //事务提交
            result = transactionalService.commitMessage(reqHeader);
            if (result.getResponseCode() == SUCCESS) {
                RemotingCommand res = checkPrepareMessage(result.getPrepareMessage(), reqHeader);
                if (res.getCode() == SUCCESS) {
                    MessageExtBrokerInner msgInner = endMessageTransaction(result.getPrepareMessage());
                    msgInner.setSysFlag(resetTransactionValue(msgInner.getSysFlag(), reqHeader.getCommitOrRollback()));
                    msgInner.setQueueOffset(reqHeader.getTranStateTableOffset());
                    msgInner.setPreparedTransactionOffset(reqHeader.getCommitLogOffset());
                    msgInner.setStoreTimestamp(result.getPrepareMessage().getStoreTimestamp());
                    RemotingCommand sendResult = sendFinalMessage(msgInner);
                    if (sendResult.getCode() == SUCCESS) {
                        transactionalService.deletePrepareMessage(result.getPrepareMessage());
                    }
                    return sendResult;
                }
                return res;
            }
        } else if (TRANSACTION_ROLLBACK_TYPE == transactionType) { //事务回滚
            result = transactionalService.rollbackMessage(reqHeader);
            if (result.getResponseCode() == SUCCESS) {
                RemotingCommand res = checkPrepareMessage(result.getPrepareMessage(), reqHeader);
                if (res.getCode() == SUCCESS) {
                    transactionalService.deletePrepareMessage(result.getPrepareMessage());
                }
                return res;
            }
        }
        resp.setCode(result.getResponseCode());
        resp.setRemark(result.getResponseRemark());
        return resp;
    }

    @Override
    public boolean rejectRequest() {
        return false;
    }

    /**
     * 校验prepare消息
     * @param msgExt
     * @param requestHeader
     * @return
     */
    private RemotingCommand checkPrepareMessage(MessageExt msgExt, EndTransactionRequestHeader requestHeader) {
        final RemotingCommand response = RemotingCommand.createResponseCommand(null);
        if (msgExt != null) {
            final String pgroupRead = msgExt.getProperty(MessageConst.PROPERTY_PRODUCER_GROUP);
            if (!pgroupRead.equals(requestHeader.getProducerGroup())) {
                response.setCode(ResponseCode.SYSTEM_ERROR);
                response.setRemark("The producer group wrong");
                return response;
            }

            if (msgExt.getQueueOffset() != requestHeader.getTranStateTableOffset()) {
                response.setCode(ResponseCode.SYSTEM_ERROR);
                response.setRemark("The transaction state table offset wrong");
                return response;
            }

            if (msgExt.getCommitLogOffset() != requestHeader.getCommitLogOffset()) {
                response.setCode(ResponseCode.SYSTEM_ERROR);
                response.setRemark("The commit log offset wrong");
                return response;
            }
        } else {
            response.setCode(ResponseCode.SYSTEM_ERROR);
            response.setRemark("Find prepared transaction message failed");
            return response;
        }
        response.setCode(SUCCESS);
        return response;
    }

    private MessageExtBrokerInner endMessageTransaction(MessageExt msgExt) {
        MessageExtBrokerInner msgInner = new MessageExtBrokerInner();
        msgInner.setTopic(msgExt.getUserProperty(MessageConst.PROPERTY_REAL_TOPIC));
        msgInner.setQueueId(Integer.parseInt(msgExt.getUserProperty(MessageConst.PROPERTY_REAL_QUEUE_ID)));
        msgInner.setBody(msgExt.getBody());
        msgInner.setFlag(msgExt.getFlag());
        msgInner.setBornTimestamp(msgExt.getBornTimestamp());
        msgInner.setBornHost(msgExt.getBornHost());
        msgInner.setStoreHost(msgExt.getStoreHost());
        msgInner.setReconsumeTimes(msgExt.getReconsumeTimes());
        msgInner.setWaitStoreMsgOK(false);
        msgInner.setTransactionId(msgExt.getUserProperty(MessageConst.PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX));
        msgInner.setSysFlag(msgExt.getSysFlag());
        TopicFilterType topicFilterType =
            (msgInner.getSysFlag() & MessageSysFlag.MULTI_TAGS_FLAG) == MessageSysFlag.MULTI_TAGS_FLAG ? TopicFilterType.MULTI_TAG
                : TopicFilterType.SINGLE_TAG;
        long tagsCodeValue = MessageExtBrokerInner.tagsString2tagsCode(topicFilterType, msgInner.getTags());
        msgInner.setTagsCode(tagsCodeValue);
        MessageAccessor.setProperties(msgInner, msgExt.getProperties());
        msgInner.setPropertiesString(MessageDecoder.messageProperties2String(msgExt.getProperties()));
        MessageAccessor.clearProperty(msgInner, MessageConst.PROPERTY_REAL_TOPIC);
        MessageAccessor.clearProperty(msgInner, MessageConst.PROPERTY_REAL_QUEUE_ID);
        return msgInner;
    }

    private RemotingCommand sendFinalMessage(MessageExtBrokerInner msgInner) {
        final RemotingCommand response = RemotingCommand.createResponseCommand(null);
        final PutMessageResult putMessageResult = this.brokerController.getMessageStore().putMessage(msgInner);
        if (putMessageResult != null) {
            switch (putMessageResult.getPutMessageStatus()) {
                // Success
                case PUT_OK:
                case FLUSH_DISK_TIMEOUT:
                case FLUSH_SLAVE_TIMEOUT:
                case SLAVE_NOT_AVAILABLE:
                    response.setCode(SUCCESS);
                    response.setRemark(null);
                    break;
                // Failed
                case CREATE_MAPEDFILE_FAILED:
                    response.setCode(ResponseCode.SYSTEM_ERROR);
                    response.setRemark("Create mapped file failed.");
                    break;
                case MESSAGE_ILLEGAL:
                case PROPERTIES_SIZE_EXCEEDED:
                    response.setCode(ResponseCode.MESSAGE_ILLEGAL);
                    response.setRemark("The message is illegal, maybe msg body or properties length not matched. msg body length limit 128k, msg properties length limit 32k.");
                    break;
                case SERVICE_NOT_AVAILABLE:
                    response.setCode(ResponseCode.SERVICE_NOT_AVAILABLE);
                    response.setRemark("Service not available now.");
                    break;
                case OS_PAGECACHE_BUSY:
                    response.setCode(ResponseCode.SYSTEM_ERROR);
                    response.setRemark("OS page cache busy, please try another machine");
                    break;
                case UNKNOWN_ERROR:
                    response.setCode(ResponseCode.SYSTEM_ERROR);
                    response.setRemark("UNKNOWN_ERROR");
                    break;
                default:
                    response.setCode(ResponseCode.SYSTEM_ERROR);
                    response.setRemark("UNKNOWN_ERROR DEFAULT");
                    break;
            }
            return response;
        } else {
            response.setCode(ResponseCode.SYSTEM_ERROR);
            response.setRemark("store putMessage return null");
        }
        return response;
    }
}
