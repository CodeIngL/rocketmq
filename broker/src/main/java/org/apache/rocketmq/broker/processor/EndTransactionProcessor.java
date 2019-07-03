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

import static org.apache.rocketmq.common.protocol.ResponseCode.SLAVE_NOT_AVAILABLE;
import static org.apache.rocketmq.common.sysflag.MessageSysFlag.*;
import static org.apache.rocketmq.remoting.common.RemotingHelper.parseChannelRemoteAddr;
import static org.apache.rocketmq.remoting.protocol.RemotingSysResponseCode.SUCCESS;

/**
 * EndTransaction processor: process commit and rollback message
 * <p>
 *     EndTransaction处理器：负责处理提交和回滚消息，并处理由回查引起的事务提交和回滚，
 *     两者本质上是一样，唯一的区别将在req中的fromTransactionCheck的体现
 *     仅仅处理一个码
 *     RequestCode.END_TRANSACTION
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
        //slave节点不支持处理事务消息
        if (BrokerRole.SLAVE == brokerController.getMessageStoreConfig().getBrokerRole()) {
            resp.setCode(SLAVE_NOT_AVAILABLE);
            LOGGER.warn("Message store is slave mode, so end transaction is forbidden. ");
            return resp;
        }

        //获得事务类型
        Integer transactionType = reqHeader.getCommitOrRollback();

        if (reqHeader.getFromTransactionCheck()) { //事务的结束是由事务回查得到的
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
                case TRANSACTION_NOT_TYPE: { //不是事务消息，返回
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
        //是事务消息的处理，可能是提交消息，可能是回滚消息

        //操作结果
        OperationResult result = new OperationResult();
        TransactionalMessageService transactionalService = this.brokerController.getTransactionalMessageService();
        switch (transactionType) {
            case TRANSACTION_COMMIT_TYPE://事务提交
                result = transactionalService.commitMessage(reqHeader); //提交事务
                if (result.getResponseCode() == SUCCESS) {
                    RemotingCommand res = checkPrepareMessage(result.getPrepareMessage(), reqHeader); //检查一致性
                    if (res.getCode() == SUCCESS) {
                        MessageExtBrokerInner msgInner = endMessageTransaction(result.getPrepareMessage()); //结束消息事务
                        msgInner.setSysFlag(resetTransactionValue(msgInner.getSysFlag(), reqHeader.getCommitOrRollback()));//设置消息标记，重置事务的状态
                        msgInner.setQueueOffset(reqHeader.getTranStateTableOffset());
                        msgInner.setPreparedTransactionOffset(reqHeader.getCommitLogOffset());
                        msgInner.setStoreTimestamp(result.getPrepareMessage().getStoreTimestamp());
                        RemotingCommand sendResult = sendFinalMessage(msgInner); //发送最终消息
                        if (sendResult.getCode() == SUCCESS) {
                            transactionalService.deletePrepareMessage(result.getPrepareMessage()); //删除perpare消息
                        }
                        return sendResult;
                    }
                    return res;
                }
            case TRANSACTION_ROLLBACK_TYPE://事务回滚
                result = transactionalService.rollbackMessage(reqHeader); //回滚事务
                if (result.getResponseCode() == SUCCESS) {
                    RemotingCommand res = checkPrepareMessage(result.getPrepareMessage(), reqHeader); //检查一致性
                    if (res.getCode() == SUCCESS) {
                        transactionalService.deletePrepareMessage(result.getPrepareMessage()); //删除perpare消息
                    }
                    return res;
                }
            default:
                    break;
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
     * 事务提交后，我们要校验prepare消息
     * @param msgExt
     * @param reqHeader
     * @return
     */
    private RemotingCommand checkPrepareMessage(MessageExt msgExt, EndTransactionRequestHeader reqHeader) {
        final RemotingCommand resp = RemotingCommand.createResponseCommand(null);
        if(msgExt == null){
            resp.setRemark("Find prepared transaction message failed");
            return resp;
        }
        final String pgroupRead = msgExt.getProperty(MessageConst.PROPERTY_PRODUCER_GROUP); //获得消息中的perpareGroup项
        if (!pgroupRead.equals(reqHeader.getProducerGroup())) {
            resp.setRemark("The producer group wrong");
            return resp;
        }

        if (msgExt.getQueueOffset() != reqHeader.getTranStateTableOffset()) { //queueOffset要一致
            resp.setRemark("The transaction state table offset wrong");
            return resp;
        }

        if (msgExt.getCommitLogOffset() != reqHeader.getCommitLogOffset()) { //offset要一致
            resp.setRemark("The commit log offset wrong");
            return resp;
        }
        resp.setCode(SUCCESS);
        return resp;
    }

    /**
     * 结束消息事务，从事务消息中恢复出原来的消息，也就是我们要即将投递到真实队列中
     * tip:这里仅仅是将事务消息恢复出部分真实的消息
     * @param msgExt
     * @return
     */
    private MessageExtBrokerInner endMessageTransaction(MessageExt msgExt) {
        MessageExtBrokerInner msgInner = new MessageExtBrokerInner();
        msgInner.setTopic(msgExt.getUserProperty(MessageConst.PROPERTY_REAL_TOPIC)); //真实的topic
        msgInner.setQueueId(Integer.parseInt(msgExt.getUserProperty(MessageConst.PROPERTY_REAL_QUEUE_ID))); //真实的queueId
        msgInner.setBody(msgExt.getBody());
        msgInner.setFlag(msgExt.getFlag());
        msgInner.setBornTimestamp(msgExt.getBornTimestamp());
        msgInner.setBornHost(msgExt.getBornHost());
        msgInner.setStoreHost(msgExt.getStoreHost());
        msgInner.setReconsumeTimes(msgExt.getReconsumeTimes());
        msgInner.setWaitStoreMsgOK(false);
        msgInner.setTransactionId(msgExt.getUserProperty(MessageConst.PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX));
        msgInner.setSysFlag(msgExt.getSysFlag());
        //tag类型
        TopicFilterType topicFilterType = (msgInner.getSysFlag() & MessageSysFlag.MULTI_TAGS_FLAG) == MessageSysFlag.MULTI_TAGS_FLAG ? TopicFilterType.MULTI_TAG : TopicFilterType.SINGLE_TAG;
        long tagsCodeValue = MessageExtBrokerInner.tagsString2tagsCode(topicFilterType, msgInner.getTags());
        msgInner.setTagsCode(tagsCodeValue);
        //
        MessageAccessor.setProperties(msgInner, msgExt.getProperties());
        msgInner.setPropertiesString(MessageDecoder.messageProperties2String(msgExt.getProperties()));
        MessageAccessor.clearProperty(msgInner, MessageConst.PROPERTY_REAL_TOPIC); //清除不要的属性
        MessageAccessor.clearProperty(msgInner, MessageConst.PROPERTY_REAL_QUEUE_ID); //清除不要的属性
        return msgInner;
    }

    /**
     * 发送最终的消息，事务提交，最终是将真正的消息进行投递真实队列中，投递成功后将标记对应op消息状态
     * @param msgInner 真实的待投递的已经提交的消息
     * @return
     */
    private RemotingCommand sendFinalMessage(MessageExtBrokerInner msgInner) {
        final RemotingCommand resp = RemotingCommand.createResponseCommand(null);
        final PutMessageResult putMessageResult = this.brokerController.getMessageStore().putMessage(msgInner);
        if (putMessageResult != null) {
            switch (putMessageResult.getPutMessageStatus()) {
                // Success
                case PUT_OK:
                case FLUSH_DISK_TIMEOUT:
                case FLUSH_SLAVE_TIMEOUT:
                case SLAVE_NOT_AVAILABLE:
                    resp.setCode(SUCCESS);
                    resp.setRemark(null);
                    break;
                // Failed
                case CREATE_MAPEDFILE_FAILED:
                    resp.setCode(ResponseCode.SYSTEM_ERROR);
                    resp.setRemark("Create mapped file failed.");
                    break;
                case MESSAGE_ILLEGAL:
                case PROPERTIES_SIZE_EXCEEDED:
                    resp.setCode(ResponseCode.MESSAGE_ILLEGAL);
                    resp.setRemark("The message is illegal, maybe msg body or properties length not matched. msg body length limit 128k, msg properties length limit 32k.");
                    break;
                case SERVICE_NOT_AVAILABLE:
                    resp.setCode(ResponseCode.SERVICE_NOT_AVAILABLE);
                    resp.setRemark("Service not available now.");
                    break;
                case OS_PAGECACHE_BUSY:
                    resp.setRemark("OS page cache busy, please try another machine");
                    break;
                case UNKNOWN_ERROR:
                    resp.setRemark("UNKNOWN_ERROR");
                    break;
                default:
                    resp.setRemark("UNKNOWN_ERROR DEFAULT");
                    break;
            }
            return resp;
        } else {
            resp.setRemark("store putMessage return null");
        }
        return resp;
    }
}
