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
package org.apache.rocketmq.broker.transaction.queue;

import org.apache.rocketmq.broker.transaction.AbstractTransactionalMessageCheckListener;
import org.apache.rocketmq.broker.transaction.OperationResult;
import org.apache.rocketmq.broker.transaction.TransactionalMessageService;
import org.apache.rocketmq.client.consumer.PullResult;
import org.apache.rocketmq.client.consumer.PullStatus;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.protocol.ResponseCode;
import org.apache.rocketmq.common.protocol.header.EndTransactionRequestHeader;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.store.AppendMessageResult;
import org.apache.rocketmq.store.MessageExtBrokerInner;
import org.apache.rocketmq.store.PutMessageResult;
import org.apache.rocketmq.store.PutMessageStatus;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import static org.apache.rocketmq.broker.transaction.queue.TransactionalMessageUtil.REMOVETAG;
import static org.apache.rocketmq.broker.transaction.queue.TransactionalMessageUtil.buildHalfTopic;
import static org.apache.rocketmq.client.consumer.PullStatus.NO_MATCHED_MSG;
import static org.apache.rocketmq.client.consumer.PullStatus.NO_NEW_MSG;
import static org.apache.rocketmq.client.consumer.PullStatus.OFFSET_ILLEGAL;
import static org.apache.rocketmq.common.message.MessageConst.PROPERTY_TRANSACTION_CHECK_TIMES;
import static org.apache.rocketmq.remoting.protocol.RemotingSysResponseCode.SUCCESS;
import static org.apache.rocketmq.remoting.protocol.RemotingSysResponseCode.SYSTEM_ERROR;

/**
 * 事务消息服务，用于处理事务相关的消息
 */
public class TransactionalMessageServiceImpl implements TransactionalMessageService {

    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.TRANSACTION_LOGGER_NAME);

    //核心处理事务的逻辑类
    private TransactionalMessageBridge transactionalMessageBridge;

    private static final int PULL_MSG_RETRY_NUMBER = 1;

    private static final int MAX_PROCESS_TIME_LIMIT = 60000;

    private static final int MAX_RETRY_COUNT_WHEN_HALF_NULL = 1;

    public TransactionalMessageServiceImpl(TransactionalMessageBridge transactionBridge) {
        this.transactionalMessageBridge = transactionBridge;
    }

    private ConcurrentHashMap<MessageQueue, MessageQueue> opQueueMap = new ConcurrentHashMap<>();

    /**
     * 事务prepare消息，也就是half消息
     * @param msgInner
     * @return
     */
    @Override
    public PutMessageResult prepareMessage(MessageExtBrokerInner msgInner) {
        return transactionalMessageBridge.putHalfMessage(msgInner);
    }

    /**
     * 消息是否需要丢弃，如果消息被重复的校验了多次，我们决定是应该丢弃这个消息的。
     * @param msgExt
     * @param checkMax
     * @return
     */
    private boolean needDiscard(MessageExt msgExt, int checkMax) {
        String checkTimes = msgExt.getProperty(PROPERTY_TRANSACTION_CHECK_TIMES); //最大校验次数
        int checkTime = 1;
        if (null != checkTimes) {
            checkTime = getInt(checkTimes);
            if (checkTime >= checkMax) {
                return true;
            } else {
                checkTime++;
            }
        }
        msgExt.putUserProperty(PROPERTY_TRANSACTION_CHECK_TIMES, String.valueOf(checkTime)); //更新，用户的次数
        return false;
    }

    /**
     * 是否忽略，如果消息存活时间，过长了，我们决定、将这个消息进行丢弃，默认是3天
     * @param msg
     * @return
     */
    private boolean needSkip(MessageExt msg) {
        long valueOfCurrentMinusBorn = System.currentTimeMillis() - msg.getBornTimestamp(); //消息已经生存的事件
        if (valueOfCurrentMinusBorn > transactionalMessageBridge.getBrokerController().getMessageStoreConfig().getFileReservedTime() * 3600L * 1000) { //大于3天了
            log.info("Half message exceed file reserved time ,so skip it.messageId {},bornTime {}", msg.getMsgId(), msg.getBornTimestamp());
            return true;
        }
        return false;
    }

    /**
     * 消息写回half队列
     * @param msgExt
     * @param offset
     * @return
     */
    private boolean putBackHalfMsgQueue(MessageExt msgExt, long offset) {
        PutMessageResult result = putBackToHalfQueueReturnResult(msgExt);
        if (result != null && result.getPutMessageStatus() == PutMessageStatus.PUT_OK) {
            AppendMessageResult appendResult = result.getAppendMessageResult();
            msgExt.setQueueOffset(appendResult.getLogicsOffset());
            msgExt.setCommitLogOffset(appendResult.getWroteOffset());
            msgExt.setMsgId(appendResult.getMsgId());
            log.debug("Send check message, the offset={} restored in queueOffset={} commitLogOffset={} newMsgId={} realMsgId={} topic={}",
                offset, msgExt.getQueueOffset(), msgExt.getCommitLogOffset(), msgExt.getMsgId(),
                msgExt.getUserProperty(MessageConst.PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX),
                msgExt.getTopic());
            return true;
        } else {
            log.error("PutBackToHalfQueueReturnResult write failed, topic: {}, queueId: {}, msgId: {}",
                msgExt.getTopic(), msgExt.getQueueId(), msgExt.getMsgId());
            return false;
        }
    }

    /**
     * 校验相关half消息，也就是从half这个特殊的topic中读取相关的信息，然后进行校验
     * @param timeout The minimum time of the transactional message to be checked firstly, one message only
     * exceed this time interval that can be checked.首先检查事务消息的最短时间，一条消息只超过可以检查的时间间隔。
     * @param checkMax The maximum number of times the message was checked, if exceed this value, this
     * message will be discarded. 检查消息的最大次数，如果超过此值，将丢弃消息。
     * @param listener When the message is considered to be checked or discarded, the relative method of this class will
     * be invoked. 当认为消息被检查或丢弃时，将调用此类的相对方法。
     */
    @Override
    public void check(long timeout, int checkMax, AbstractTransactionalMessageCheckListener listener) {
        try {
            //half消息是特殊的topic:RMQ_SYS_TRANS_HALF_TOPIC
            String topic = buildHalfTopic();
            //获得half的topic对应的消息队列
            Set<MessageQueue> mqs = transactionalMessageBridge.fetchMessageQueues(topic);
            //不存在，没有相关事务消息，我们忽略掉
            if (mqs == null || mqs.size() == 0) {
                log.warn("The queue of topic is empty :" + topic);
                return;
            }
            log.debug("Check topic={}, queues={}", topic, mqs);
            //遍历half的topic对应所有消息队列
            for (MessageQueue mq : mqs) {
                long startTime = System.currentTimeMillis();
                //获得half队列对应的op队列
                MessageQueue opQueue = getOpQueue(mq);

                //获取half消息偏移量
                long halfOffset = transactionalMessageBridge.fetchConsumeOffset(mq);
                //获取op消息偏移量
                long opOffset = transactionalMessageBridge.fetchConsumeOffset(opQueue);
                log.info("Before check, the queue={} msgOffset={} opOffset={}", mq, halfOffset, opOffset);
                if (halfOffset < 0 || opOffset < 0) { //非法的数据我们忽略他们
                    log.error("MessageQueue: {} illegal offset read: {}, op offset: {},skip this queue", mq, halfOffset, opOffset);
                    continue;
                }

                List<Long> doneOpOffset = new ArrayList<>(); //已经完成的opOffset列表
                HashMap<Long, Long> removeMap = new HashMap<>(); //需要remove的映射
                PullResult result = fillOpRemoveMap(removeMap, opQueue, opOffset, halfOffset, doneOpOffset);
                if (null == result) {
                    log.error("The queue={} check msgOffset={} with opOffset={} failed, pullResult is null", mq, halfOffset, opOffset);
                    continue;
                }

                // single thread
                int getMessageNullCount = 1;
                //循环中每轮新的offset，一开始时最小的offset
                long newOffset = halfOffset;
                //循环变量
                long i = halfOffset;
                while (true) {
                    if (System.currentTimeMillis() - startTime > MAX_PROCESS_TIME_LIMIT) { //超出最大处理时间
                        log.info("Queue={} process time reach max={}", mq, MAX_PROCESS_TIME_LIMIT);
                        break;
                    }
                    if (removeMap.containsKey(i)) { //该消息确定是committed或者rolled back
                        log.info("Half offset {} has been committed/rolled back", i);
                        removeMap.remove(i);
                    } else {
                        GetResult getResult = getHalfMsg(mq, i); //得到对应的half消息
                        MessageExt msg = getResult.getMsg(); //得到消息
                        if (msg == null) {
                            //空消息，通常是因边界的问题，比如消息读完了，或者非法的状态
                            if (getMessageNullCount++ > MAX_RETRY_COUNT_WHEN_HALF_NULL) {
                                //没有消息了，我们两次后，不再重复检测
                                break;
                            }
                            //没有新消息
                            if (getResult.getPullResult().getPullStatus() == NO_NEW_MSG) {
                                log.debug("No new msg, the miss offset={} in={}, continue check={}, pull result={}", i, mq, getMessageNullCount, getResult.getPullResult());
                                break;
                            } else {
                                //消息非法，调整到下一个开始
                                log.info("Illegal offset, the miss offset={} in={}, continue check={}, pull result={}", i, mq, getMessageNullCount, getResult.getPullResult());
                                i = getResult.getPullResult().getNextBeginOffset(); //下一个开始
                                newOffset = i;
                                continue;
                            }
                        }

                        //是否要丢弃，该消息校验次数已经很多了，这条消息太古老了
                        if (needDiscard(msg, checkMax) || needSkip(msg)) {
                            listener.resolveDiscardMsg(msg);
                            newOffset = i + 1; //推进下一个offset进行检查
                            i++;
                            continue;
                        }
                        if (msg.getStoreTimestamp() >= startTime) {
                            log.debug("Fresh stored. the miss offset={}, check it later, store={}", i, new Date(msg.getStoreTimestamp()));
                            break;
                        }

                        long valueOfCurrentMinusBorn = System.currentTimeMillis() - msg.getBornTimestamp(); //本消息已经生存的时间
                        long checkImmunityTime = timeout; //超时的时间
                        //用户设定可以超时的时间
                        String checkImmunityTimeStr = msg.getUserProperty(MessageConst.PROPERTY_CHECK_IMMUNITY_TIME_IN_SECONDS);
                        if (null != checkImmunityTimeStr) { //存在超时时间
                            checkImmunityTime = getImmunityTime(checkImmunityTimeStr, timeout); //确定真正使用的超时时间
                            if (valueOfCurrentMinusBorn < checkImmunityTime) { //已经生存的时间小于超时的事件，这个是正常的，
                                if (checkPrepareQueueOffset(removeMap, doneOpOffset, msg)) { //check一下，再次check一下，如果返回true，我们将忽略这条消息
                                    newOffset = i + 1;
                                    i++;
                                    continue;
                                }
                            }
                        } else {
                            //使用系统的超时时间，我们稍后近些check
                            if ((0 <= valueOfCurrentMinusBorn) && (valueOfCurrentMinusBorn < checkImmunityTime)) {
                                log.debug("New arrived, the miss offset={}, check it later checkImmunity={}, born={}", i,
                                    checkImmunityTime, new Date(msg.getBornTimestamp()));
                                break;
                            }
                        }

                        List<MessageExt> opMsg = result.getMsgFoundList(); //op消息
                        //没有op消息且本消息的时间，已经超过了事务的时间|op消息的最后一个消息开始时间已经大于超时时间|-1，应该是内部处理，这里是不可能成立的。
                        boolean isNeedCheck = (opMsg == null && valueOfCurrentMinusBorn > checkImmunityTime)
                            || (opMsg != null && (opMsg.get(opMsg.size() - 1).getBornTimestamp() - startTime > timeout))
                            || (valueOfCurrentMinusBorn <= -1); //是否需要再次check

                        if (isNeedCheck) { //需要check
                            if (!putBackHalfMsgQueue(msg, i)) { //重新投递一下消息,投递失败，我们忽略
                                continue;
                            }
                            //触发监听，回调客户端，来触发消息的监听
                            listener.resolveHalfMsg(msg);
                        } else {
                            result = fillOpRemoveMap(removeMap, opQueue, result.getNextBeginOffset(), halfOffset, doneOpOffset);
                            log.info("The miss offset:{} in messageQueue:{} need to get more opMsg, result is:{}", i,
                                mq, result);
                            continue;
                        }
                    }
                    newOffset = i + 1; //更新，再检查
                    i++;
                }
                if (newOffset != halfOffset) { //数据发生变化
                    transactionalMessageBridge.updateConsumeOffset(mq, newOffset); //更新一下
                }
                long newOpOffset = calculateOpOffset(doneOpOffset, opOffset); //计算一下opoffset
                if (newOpOffset != opOffset) {
                    transactionalMessageBridge.updateConsumeOffset(opQueue, newOpOffset); //更新
                }
            }
        } catch (Exception e) {
            log.error("Check error", e);
        }

    }

    /**
     * 获得真正的超时时间，
     * @param checkImmunityTimeStr 消息指定的超时时间
     * @param transactionTimeout 事务配置指定的超时时间
     * @return  以用户的消息指定时间为主。
     */
    private long getImmunityTime(String checkImmunityTimeStr, long transactionTimeout) {
        long checkImmunityTime;

        checkImmunityTime = getLong(checkImmunityTimeStr);
        if (-1 == checkImmunityTime) {
            checkImmunityTime = transactionTimeout;
        } else {
            checkImmunityTime *= 1000;
        }
        return checkImmunityTime;
    }

    /**
     * Read op message, parse op message, and fill removeMap
     * <p>
     *     读取操作消息，解析操作消息，并填充removeMap
     * </p>
     *
     * @param removeMap Half message to be remove, key:halfOffset, value: opOffset. 存储待删除映射halfoffset和opoffset的映射
     * @param opQueue Op message queue.
     * @param pullOffsetOfOp The begin offset of op message queue.
     * @param miniOffset The current minimum offset of half message queue.
     * @param doneOpOffset Stored op messages that have been processed.
     * @return Op message result.
     */
    private PullResult fillOpRemoveMap(HashMap<Long, Long> removeMap, MessageQueue opQueue, long pullOffsetOfOp, long miniOffset, List<Long> doneOpOffset) {
        //broker自己拉取一批op消息进行消费，op消息是带有一些特殊的状态
        PullResult result = pullOpMsg(opQueue, pullOffsetOfOp, 32);
        if (null == result) {
            return null;
        }
        //拉取结果
        PullStatus status = result.getPullStatus();
        if (status == OFFSET_ILLEGAL || status == NO_MATCHED_MSG) {
            //非法的offset和没有消息匹配
            log.warn("The miss op offset={} in queue={} is illegal, pullResult={}", pullOffsetOfOp, opQueue, result);
            transactionalMessageBridge.updateConsumeOffset(opQueue, result.getNextBeginOffset());
            return result;
        } else if (status == NO_NEW_MSG) {
            //没有新的消息
            log.warn("The miss op offset={} in queue={} is NO_NEW_MSG, pullResult={}", pullOffsetOfOp, opQueue, result);
            return result;
        }


        List<MessageExt> opMsgs = result.getMsgFoundList(); //拉取的消息
        if (opMsgs == null) {
            log.warn("The miss op offset={} in queue={} is empty, pullResult={}", pullOffsetOfOp, opQueue, result);
            return result;
        }
        for (MessageExt opMsg : opMsgs) { //遍历拉取的消息
            Long queueOffset = getLong(new String(opMsg.getBody(), TransactionalMessageUtil.charset)); //获得队列的offset（halfOffset）
            log.info("Topic: {} tags: {}, OpOffset: {}, HalfOffset: {}", opMsg.getTopic(), opMsg.getTags(), opMsg.getQueueOffset(), queueOffset);
            if (REMOVETAG.equals(opMsg.getTags())) { //tag是d，目前写入op消息，总是会带着一个特殊的标记
                if (queueOffset < miniOffset) { //offset已经小于最小offset，说明已经被处理，也就是op消息已经被处理掉了
                    doneOpOffset.add(opMsg.getQueueOffset());
                } else {
                    removeMap.put(queueOffset, opMsg.getQueueOffset()); //否则，这些op消息需要构成一个集合，因为我们后续需要通过他来确定我们的事务消息的状态，这里我们对他放入待删除映射
                }
            } else {
                log.error("Found a illegal tag in opMessageExt= {} ", opMsg);
            }
        }
        log.debug("Remove map: {}", removeMap);
        log.debug("Done op list: {}", doneOpOffset);
        return result;
    }

    /**
     * If return true, skip this msg
     *
     * @param removeMap Op message map to determine whether a half message was responded by producer.
     * @param doneOpOffset Op Message which has been checked.
     * @param msgExt Half message
     * @return Return true if put success, otherwise return false.
     */
    private boolean checkPrepareQueueOffset(HashMap<Long, Long> removeMap, List<Long> doneOpOffset, MessageExt msgExt) {
        //尝试获得用户设定一阶段的offset
        String prepareQueueOffsetStr = msgExt.getUserProperty(MessageConst.PROPERTY_TRANSACTION_PREPARED_QUEUE_OFFSET);
        if (null == prepareQueueOffsetStr) {
            //没有设定
            return putImmunityMsgBackToHalfQueue(msgExt); //重新投递到half队列中，
        } else {
            long prepareQueueOffset = getLong(prepareQueueOffsetStr); //获得指定的offset
            if (-1 == prepareQueueOffset) {
                //无效设定
                return false;
            } else {
                //已经确定的状态，不管是commit还是rollback，都是已经确定了的，
                if (removeMap.containsKey(prepareQueueOffset)) {
                    //删除，确定忽略这条消息
                    long tmpOpOffset = removeMap.remove(prepareQueueOffset);
                    doneOpOffset.add(tmpOpOffset);
                    return true;
                } else {
                    //重新投递到half消息队列中，投递成功，就不用校验这条消息，而是向前推进
                    return putImmunityMsgBackToHalfQueue(msgExt);
                }
            }
        }
    }

    /**
     * Write messageExt to Half topic again
     *
     * @param messageExt Message will be write back to queue
     * @return Put result can used to determine the specific results of storage.
     */
    private PutMessageResult putBackToHalfQueueReturnResult(MessageExt messageExt) {
        PutMessageResult putMessageResult = null;
        try {
            MessageExtBrokerInner msgInner = transactionalMessageBridge.renewHalfMessageInner(messageExt);
            putMessageResult = transactionalMessageBridge.putMessageReturnResult(msgInner);
        } catch (Exception e) {
            log.warn("PutBackToHalfQueueReturnResult error", e);
        }
        return putMessageResult;
    }

    /**
     * 重新投递消息，投递的half消息
     * @param messageExt
     * @return
     */
    private boolean putImmunityMsgBackToHalfQueue(MessageExt messageExt) {
        MessageExtBrokerInner msgInner = transactionalMessageBridge.renewImmunityHalfMessageInner(messageExt);
        return transactionalMessageBridge.putMessage(msgInner);
    }

    /**
     * Read half message from Half Topic
     *
     * @param mq Target message queue, in this method, it means the half message queue.
     * @param offset Offset in the message queue.
     * @param nums Pull message number.
     * @return Messages pulled from half message queue.
     */
    private PullResult pullHalfMsg(MessageQueue mq, long offset, int nums) {
        return transactionalMessageBridge.getHalfMessage(mq.getQueueId(), offset, nums);
    }

    /**
     * Read op message from Op Topic
     *
     * @param mq Target Message Queue
     * @param offset Offset in the message queue
     * @param nums Pull message number
     * @return Messages pulled from operate message queue.
     */
    private PullResult pullOpMsg(MessageQueue mq, long offset, int nums) {
        return transactionalMessageBridge.getOpMessage(mq.getQueueId(), offset, nums);
    }

    private Long getLong(String s) {
        long v = -1;
        try {
            v = Long.valueOf(s);
        } catch (Exception e) {
            log.error("GetLong error", e);
        }
        return v;

    }

    private Integer getInt(String s) {
        int v = -1;
        try {
            v = Integer.valueOf(s);
        } catch (Exception e) {
            log.error("GetInt error", e);
        }
        return v;

    }

    /**
     * 计算opoffset
     * @param doneOffset
     * @param oldOffset
     * @return
     */
    private long calculateOpOffset(List<Long> doneOffset, long oldOffset) {
        Collections.sort(doneOffset);
        long newOffset = oldOffset;
        for (int i = 0; i < doneOffset.size(); i++) {
            if (doneOffset.get(i) == newOffset) {
                newOffset++;
            } else {
                break;
            }
        }
        return newOffset;

    }

    /**
     * 获得消息队列对应op队列，一个half消息队列，对应一个相应的op操作队列。并存储在内存中，如果
     * 这个half消息队列是没有对应的op，我们总是会为他进行相应的创建
     * @param mq
     * @return
     */
    private MessageQueue getOpQueue(MessageQueue mq) {
        MessageQueue opQueue = opQueueMap.get(mq);
        if (opQueue != null){
            return opQueue;
        }
        opQueue = new MessageQueue(TransactionalMessageUtil.buildOpTopic(), mq.getBrokerName(), mq.getQueueId());
        opQueueMap.put(mq, opQueue);
        return opQueue;

    }

    /**
     * 获得half消息
     * @param messageQueue
     * @param offset
     * @return
     */
    private GetResult getHalfMsg(MessageQueue messageQueue, long offset) {
        GetResult getResult = new GetResult();

        PullResult result = pullHalfMsg(messageQueue, offset, PULL_MSG_RETRY_NUMBER);
        getResult.setPullResult(result);
        List<MessageExt> messageExts = result.getMsgFoundList();
        if (messageExts == null) {
            return getResult;
        }
        getResult.setMsg(messageExts.get(0));
        return getResult;
    }

    /**
     * 获得一阶段的消息，通过偏移量获得
     * @param commitLogOffset
     * @return
     */
    private OperationResult getHalfMessageByOffset(long commitLogOffset) {
        OperationResult resp = new OperationResult();
        MessageExt messageExt = this.transactionalMessageBridge.lookMessageByOffset(commitLogOffset);
        if (messageExt != null) {
            resp.setResponseCode(SUCCESS);
            resp.setPrepareMessage(messageExt);
        } else {
            resp.setResponseCode(SYSTEM_ERROR);
            resp.setResponseRemark("Find prepared transaction message failed");
        }
        return resp;
    }

    /**
     * 删除prepare消息,二阶段，需要删除一阶段的half消息，这里删除就是写入op消息是带有特殊标记的
     * 也就是说我们确定了一个事务消息已经被提交或者回滚的时候，我们要删除相关的一阶段的的消息也就是half消息
     * 删除我们是通过另一种逻辑形式来实现的我们尝试进行投递一个带有特殊标记的消息也就是op消息，来标识事务消息的状态。用op消息来表示一条消息已经确定
     * @param msgExt
     * @return
     */
    @Override
    public boolean deletePrepareMessage(MessageExt msgExt) {
        //删除消息，是有逻辑处理，就是写入一个特殊的消息，也就是op消息
        if (this.transactionalMessageBridge.putOpMessage(msgExt, REMOVETAG)) {
            log.info("Transaction op message write successfully. messageId={}, queueId={} msgExt:{}", msgExt.getMsgId(), msgExt.getQueueId(), msgExt);
            return true;
        } else {
            log.error("Transaction op message write failed. messageId is {}, queueId is {}", msgExt.getMsgId(), msgExt.getQueueId());
            return false;
        }
    }

    /**
     * 提交消息，实际上是获得一阶段消息，后续处理需要再次处理
     * @param requestHeader Commit message request header.
     * @return
     */
    @Override
    public OperationResult commitMessage(EndTransactionRequestHeader requestHeader) {
        return getHalfMessageByOffset(requestHeader.getCommitLogOffset());
    }

    /**
     * 回滚消息，实际上是获得一阶段消息，后续处理需要再次处理
     * @param requestHeader Prepare message request header.
     * @return
     */
    @Override
    public OperationResult rollbackMessage(EndTransactionRequestHeader requestHeader) {
        return getHalfMessageByOffset(requestHeader.getCommitLogOffset());
    }

    @Override
    public boolean open() {
        return true;
    }

    @Override
    public void close() {

    }

}
