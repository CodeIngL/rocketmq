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

import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.client.consumer.PullResult;
import org.apache.rocketmq.client.consumer.PullStatus;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.constant.PermName;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageAccessor;
import org.apache.rocketmq.common.message.MessageClientIDSetter;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.protocol.heartbeat.SubscriptionData;
import org.apache.rocketmq.common.sysflag.MessageSysFlag;
import org.apache.rocketmq.logging.InnerLoggerFactory;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.store.GetMessageResult;
import org.apache.rocketmq.store.MessageExtBrokerInner;
import org.apache.rocketmq.store.MessageStore;
import org.apache.rocketmq.store.PutMessageResult;
import org.apache.rocketmq.store.PutMessageStatus;
import org.apache.rocketmq.store.stats.BrokerStatsManager;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import static java.lang.String.valueOf;
import static java.lang.System.currentTimeMillis;
import static java.lang.Thread.currentThread;
import static org.apache.rocketmq.broker.transaction.queue.TransactionalMessageUtil.*;
import static org.apache.rocketmq.common.message.MessageAccessor.putProperty;
import static org.apache.rocketmq.common.message.MessageConst.PROPERTY_REAL_QUEUE_ID;
import static org.apache.rocketmq.common.message.MessageConst.PROPERTY_REAL_TOPIC;
import static org.apache.rocketmq.common.message.MessageConst.PROPERTY_TRANSACTION_PREPARED_QUEUE_OFFSET;
import static org.apache.rocketmq.common.message.MessageDecoder.messageProperties2String;
import static org.apache.rocketmq.common.sysflag.MessageSysFlag.TRANSACTION_NOT_TYPE;
import static org.apache.rocketmq.common.sysflag.MessageSysFlag.resetTransactionValue;
import static org.apache.rocketmq.remoting.common.RemotingHelper.parseSocketAddressAddr;
import static org.apache.rocketmq.store.MessageExtBrokerInner.tagsString2tagsCode;

/**
 * 事务消息处理的核心类
 */
public class TransactionalMessageBridge {

    private static final InternalLogger LOGGER = InnerLoggerFactory.getLogger(LoggerName.TRANSACTION_LOGGER_NAME);

    //op队列映射
    private final ConcurrentHashMap<MessageQueue, MessageQueue> opQueueMap = new ConcurrentHashMap<>();
    //broker
    private final BrokerController brokerController;
    //消息存储
    private final MessageStore store;
    private final SocketAddress storeHost;

    public TransactionalMessageBridge(BrokerController brokerController, MessageStore store) {
        try {
            this.brokerController = brokerController;
            this.store = store;
            this.storeHost = new InetSocketAddress(brokerController.getBrokerConfig().getBrokerIP1(),
                    brokerController.getNettyServerConfig().getListenPort());
        } catch (Exception e) {
            LOGGER.error("Init TransactionBridge error", e);
            throw new RuntimeException(e);
        }

    }

    /**
     * 找到该queue对应的最小的offset
     * @param mq
     * @return
     */
    public long fetchConsumeOffset(MessageQueue mq) {
        String topic = mq.getTopic();
        int queueId = mq.getQueueId();
        long offset = brokerController.getConsumerOffsetManager().queryOffset(buildConsumerGroup(), topic, queueId);
        if (offset == -1) {
            offset = store.getMinOffsetInQueue(topic, queueId); //最小的offset
        }
        return offset;
    }

    /**
     * 获得topic对应相关的的消息队列描述对象
     * 最多只有指定相关的支持可读的队列
     * @param topic
     * @return
     */
    public Set<MessageQueue> fetchMessageQueues(String topic) {
        Set<MessageQueue> mqSet = new HashSet<>();
        TopicConfig topicConfig = selectTopicConfig(topic);
        if (topicConfig != null && topicConfig.getReadQueueNums() > 0) {
            for (int i = 0; i < topicConfig.getReadQueueNums(); i++) {
                MessageQueue mq = new MessageQueue();
                mq.setTopic(topic);
                mq.setBrokerName(brokerController.getBrokerConfig().getBrokerName());
                mq.setQueueId(i);
                mqSet.add(mq);
            }
        }
        return mqSet;
    }

    /**
     * 更新consumer对应offset
     * @param mq
     * @param offset
     */
    public void updateConsumeOffset(MessageQueue mq, long offset) {
        this.brokerController.getConsumerOffsetManager().commitOffset(parseSocketAddressAddr(this.storeHost), buildConsumerGroup(), mq.getTopic(), mq.getQueueId(), offset);
    }

    /**
     * 拉取queueid对应的half消息，最多nums数量
     * @param queueId
     * @param offset
     * @param nums
     * @return
     */
    public PullResult getHalfMessage(int queueId, long offset, int nums) {
        String group = buildConsumerGroup();
        String topic = buildHalfTopic();
        SubscriptionData sub = new SubscriptionData(topic, "*");
        return getMessage(group, topic, queueId, offset, nums, sub);
    }

    public PullResult getOpMessage(int queueId, long offset, int nums) {
        String group = buildConsumerGroup();
        String topic = buildOpTopic();
        SubscriptionData sub = new SubscriptionData(topic, "*");
        return getMessage(group, topic, queueId, offset, nums, sub);
    }

    /**
     * 拉取消息
     * @param group
     * @param topic
     * @param queueId
     * @param offset
     * @param nums
     * @param sub
     * @return
     */
    private PullResult getMessage(String group, String topic, int queueId, long offset, int nums, SubscriptionData sub) {
        GetMessageResult getResult = store.getMessage(group, topic, queueId, offset, nums, null);
        if(getResult == null){
            LOGGER.error("Get message from store return null. topic={}, groupId={}, requestOffset={}", topic, group, offset);
            return null;
        }

        PullStatus status = PullStatus.NO_NEW_MSG;
        List<MessageExt> foundList = null;
        BrokerStatsManager statsManager = brokerController.getBrokerStatsManager();
        switch (getResult.getStatus()) {
            case FOUND:
                status = PullStatus.FOUND;
                foundList = decodeMsgList(getResult);
                statsManager.incGroupGetNums(group, topic, getResult.getMessageCount());
                statsManager.incGroupGetSize(group, topic, getResult.getBufferTotalSize());
                statsManager.incBrokerGetNums(getResult.getMessageCount());
                statsManager.recordDiskFallBehindTime(group, topic, queueId,
                        this.brokerController.getMessageStore().now() - foundList.get(foundList.size() - 1).getStoreTimestamp());
                break;
            case NO_MATCHED_MESSAGE:
                status = PullStatus.NO_MATCHED_MSG;
                LOGGER.warn("No matched message. GetMessageStatus={}, topic={}, groupId={}, requestOffset={}", getResult.getStatus(), topic, group, offset);
                break;
            case NO_MESSAGE_IN_QUEUE:
                status = PullStatus.NO_NEW_MSG;
                LOGGER.warn("No new message. GetMessageStatus={}, topic={}, groupId={}, requestOffset={}", getResult.getStatus(), topic, group, offset);
                break;
            case MESSAGE_WAS_REMOVING:
            case NO_MATCHED_LOGIC_QUEUE:
            case OFFSET_FOUND_NULL:
            case OFFSET_OVERFLOW_BADLY:
            case OFFSET_OVERFLOW_ONE:
            case OFFSET_TOO_SMALL:
                status = PullStatus.OFFSET_ILLEGAL;
                LOGGER.warn("Offset illegal. GetMessageStatus={}, topic={}, groupId={}, requestOffset={}",
                        getResult.getStatus(), topic, group, offset);
                break;
            default:
                assert false;
                break;
        }

        return new PullResult(status, getResult.getNextBeginOffset(), getResult.getMinOffset(), getResult.getMaxOffset(), foundList);

    }

    private List<MessageExt> decodeMsgList(GetMessageResult getMessageResult) {
        List<MessageExt> foundList = new ArrayList<>();
        try {
            List<ByteBuffer> messageBufferList = getMessageResult.getMessageBufferList();
            for (ByteBuffer bb : messageBufferList) {
                MessageExt msgExt = MessageDecoder.decode(bb);
                foundList.add(msgExt);
            }

        } finally {
            getMessageResult.release();
        }

        return foundList;
    }

    /**
     * 投递事务的half消息
     * @param msgInner
     * @return
     */
    public PutMessageResult putHalfMessage(MessageExtBrokerInner msgInner) {
        //构建half消息，然后由存储引擎进行处理
        return store.putMessage(parseHalfMessageInner(msgInner));
    }

    /**
     * 构建half消息一阶段消息
     * 核心就是将消息转换，通过topic和queue进行装换
     * @param msg
     * @return
     */
    private MessageExtBrokerInner parseHalfMessageInner(MessageExtBrokerInner msg) {
        //真实的topic
        putProperty(msg, PROPERTY_REAL_TOPIC, msg.getTopic());
        //真实的queueId
        putProperty(msg, PROPERTY_REAL_QUEUE_ID, valueOf(msg.getQueueId()));
        //重置消息的相关标记
        msg.setSysFlag(resetTransactionValue(msg.getSysFlag(), TRANSACTION_NOT_TYPE));
        //一阶段事务topic，写入一个特定的topic，并且总是写入其queueId，并且值是0。
        //这意味着所有的都是写入这个队列，而不管之前真实的topic是什么
        msg.setTopic(buildHalfTopic());
        msg.setQueueId(0);
        msg.setPropertiesString(messageProperties2String(msg.getProperties()));
        return msg;
    }

    /**
     * 放置op消息
     * @param msg
     * @param opType
     * @return
     */
    public boolean putOpMessage(MessageExt msg, String opType) {
        if (!REMOVETAG.equals(opType)){
            return true;
        }
        //添加消息的队列的OP的REMOVETAG
        return addRemoveTagInTransactionOp(msg, new MessageQueue(msg.getTopic(), this.brokerController.getBrokerConfig().getBrokerName(), msg.getQueueId()));
    }

    public PutMessageResult putMessageReturnResult(MessageExtBrokerInner messageInner) {
        LOGGER.debug("[BUG-TO-FIX] Thread:{} msgID:{}", currentThread().getName(), messageInner.getMsgId());
        return store.putMessage(messageInner);
    }

    /**
     * 投递消息
     * @param msgInner
     * @return
     */
    public boolean putMessage(MessageExtBrokerInner msgInner) {
        PutMessageResult result = store.putMessage(msgInner);
        if (result != null && result.getPutMessageStatus() == PutMessageStatus.PUT_OK) {
            return true;
        } else {
            LOGGER.error("Put message failed, topic: {}, queueId: {}, msgId: {}", msgInner.getTopic(), msgInner.getQueueId(), msgInner.getMsgId());
            return false;
        }
    }

    public MessageExtBrokerInner renewImmunityHalfMessageInner(MessageExt msgExt) {
        MessageExtBrokerInner msgInner = renewHalfMessageInner(msgExt);
        String queueOffsetFromPrepare = msgExt.getUserProperty(PROPERTY_TRANSACTION_PREPARED_QUEUE_OFFSET); //设置offset
        if (null != queueOffsetFromPrepare) {
            putProperty(msgInner, PROPERTY_TRANSACTION_PREPARED_QUEUE_OFFSET, valueOf(queueOffsetFromPrepare));
        } else {
            putProperty(msgInner, PROPERTY_TRANSACTION_PREPARED_QUEUE_OFFSET, valueOf(msgExt.getQueueOffset()));
        }

        msgInner.setPropertiesString(messageProperties2String(msgInner.getProperties()));

        return msgInner;
    }

    public MessageExtBrokerInner renewHalfMessageInner(MessageExt msgExt) {
        MessageExtBrokerInner msgInner = new MessageExtBrokerInner();
        msgInner.setTopic(msgExt.getTopic());
        msgInner.setBody(msgExt.getBody());
        msgInner.setQueueId(msgExt.getQueueId());
        msgInner.setMsgId(msgExt.getMsgId());
        msgInner.setSysFlag(msgExt.getSysFlag());
        msgInner.setTags(msgExt.getTags());
        msgInner.setTagsCode(tagsString2tagsCode(msgInner.getTags()));
        MessageAccessor.setProperties(msgInner, msgExt.getProperties());
        msgInner.setPropertiesString(messageProperties2String(msgExt.getProperties()));
        msgInner.setBornTimestamp(msgExt.getBornTimestamp());
        msgInner.setBornHost(msgExt.getBornHost());
        msgInner.setStoreHost(msgExt.getStoreHost());
        msgInner.setWaitStoreMsgOK(false);
        return msgInner;
    }

    /**
     * 构建内部的op消息
     * @param msg
     * @param mq
     * @return
     */
    private MessageExtBrokerInner makeOpMessageInner(Message msg, MessageQueue mq) {
        MessageExtBrokerInner msgInner = new MessageExtBrokerInner();
        msgInner.setTopic(msg.getTopic());
        msgInner.setBody(msg.getBody());
        msgInner.setQueueId(mq.getQueueId());
        msgInner.setTags(msg.getTags());
        msgInner.setTagsCode(tagsString2tagsCode(msgInner.getTags()));
        msgInner.setSysFlag(0);
        MessageAccessor.setProperties(msgInner, msg.getProperties());
        msgInner.setPropertiesString(messageProperties2String(msg.getProperties()));
        msgInner.setBornTimestamp(currentTimeMillis());
        msgInner.setBornHost(this.storeHost);
        msgInner.setStoreHost(this.storeHost);
        msgInner.setWaitStoreMsgOK(false);
        MessageClientIDSetter.setUniqID(msgInner);
        return msgInner;
    }

    private TopicConfig selectTopicConfig(String topic) {
        TopicConfig config = brokerController.getTopicConfigManager().selectTopicConfig(topic);
        if (config == null) {
            config = this.brokerController.getTopicConfigManager().createTopicInSendMessageBackMethod(
                topic, 1, PermName.PERM_WRITE | PermName.PERM_READ, 0);
        }
        return config;
    }

    /**
     * Use this function while transaction msg is committed or rollback write a flag 'd' to operation queue for the
     * msg's offset
     * <p>
     *     提交事务msg时使用此函数或回滚将操作队列中的标志“d”写入msg的偏移量
     * </p>
     *
     * @param messageExt Op message
     * @param messageQueue Op message queue
     * @return This method will always return true.
     */
    private boolean addRemoveTagInTransactionOp(MessageExt messageExt, MessageQueue messageQueue) {
        //构建消息，op消息也就是写入一个特殊标记，为op消息写入一个特殊的标记
        Message message = new Message(buildOpTopic(), REMOVETAG, valueOf(messageExt.getQueueOffset()).getBytes(TransactionalMessageUtil.charset));
        writeOp(message, messageQueue);
        return true;
    }

    /**
     * 写入op消息
     * @param message
     * @param mq
     */
    private void writeOp(Message message, MessageQueue mq) {
        //将消息写入由消息队列mq对应op队列，如果不存在我们将构建
        MessageQueue opQueue;
        if (opQueueMap.containsKey(mq)) {
            opQueue = opQueueMap.get(mq);
        } else {
            opQueue = getOpQueueByHalf(mq);
            MessageQueue oldQueue = opQueueMap.putIfAbsent(mq, opQueue);
            if (oldQueue != null) {
                opQueue = oldQueue;
            }
        }
        if (opQueue == null) {
            opQueue = new MessageQueue(buildOpTopic(), mq.getBrokerName(), mq.getQueueId());
        }
        putMessage(makeOpMessageInner(message, opQueue));
    }

    /**
     * 获得half消息对应的op消息队列
     * @param halfMQ
     * @return
     */
    private MessageQueue getOpQueueByHalf(MessageQueue halfMQ) {
        MessageQueue opQueue = new MessageQueue();
        opQueue.setTopic(buildOpTopic()); //特殊的队列
        opQueue.setBrokerName(halfMQ.getBrokerName());
        opQueue.setQueueId(halfMQ.getQueueId());
        return opQueue;
    }

    public MessageExt lookMessageByOffset(final long commitLogOffset) {
        return this.store.lookMessageByOffset(commitLogOffset);
    }

    public BrokerController getBrokerController() {
        return brokerController;
    }
}
