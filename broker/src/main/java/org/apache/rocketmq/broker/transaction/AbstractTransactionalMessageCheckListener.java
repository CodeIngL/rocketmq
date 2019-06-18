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
package org.apache.rocketmq.broker.transaction;

import io.netty.channel.Channel;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.protocol.header.CheckTransactionStateRequestHeader;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static org.apache.rocketmq.common.message.MessageConst.*;

public abstract class AbstractTransactionalMessageCheckListener {
    private static final InternalLogger LOGGER = InternalLoggerFactory.getLogger(LoggerName.TRANSACTION_LOGGER_NAME);

    private BrokerController brokerController;

    private static ExecutorService executorService = new ThreadPoolExecutor(2, 5, 100, TimeUnit.SECONDS, new ArrayBlockingQueue<Runnable>(2000), new ThreadFactory() {
        @Override
        public Thread newThread(Runnable r) {
            Thread thread = new Thread(r);
            thread.setName("Transaction-msg-check-thread");
            return thread;
        }
    });

    public AbstractTransactionalMessageCheckListener() {
    }

    public AbstractTransactionalMessageCheckListener(BrokerController brokerController) {
        this.brokerController = brokerController;
    }

    /**
     * 发送check消息返回客户端，也就是回查客户端的逻辑
     * @param msg
     * @throws Exception
     */
    public void sendCheckMessage(MessageExt msg) throws Exception {
        CheckTransactionStateRequestHeader reqHeader = new CheckTransactionStateRequestHeader();
        reqHeader.setCommitLogOffset(msg.getCommitLogOffset());
        reqHeader.setOffsetMsgId(msg.getMsgId());
        reqHeader.setMsgId(msg.getUserProperty(PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX));
        reqHeader.setTransactionId(reqHeader.getMsgId());
        reqHeader.setTranStateTableOffset(msg.getQueueOffset());
        msg.setTopic(msg.getUserProperty(PROPERTY_REAL_TOPIC));
        msg.setQueueId(Integer.parseInt(msg.getUserProperty(PROPERTY_REAL_QUEUE_ID)));
        msg.setStoreSize(0);
        String groupId = msg.getProperty(PROPERTY_PRODUCER_GROUP);
        Channel channel = brokerController.getProducerManager().getAvaliableChannel(groupId);
        if (channel != null) {
            brokerController.getBroker2Client().checkProducerTransactionState(groupId, channel, reqHeader, msg);
        } else {
            LOGGER.warn("Check transaction failed, channel is null. groupId={}", groupId);
        }
    }

    public void resolveHalfMsg(final MessageExt msgExt) {
        executorService.execute(() -> {
            try {
                sendCheckMessage(msgExt);
            } catch (Exception e) {
                LOGGER.error("Send check message error!", e);
            }
        });
    }

    public BrokerController getBrokerController() {
        return brokerController;
    }

    public void shutDown() {
        executorService.shutdown();
    }

    /**
     * Inject brokerController for this listener
     *
     * @param brokerController
     */
    public void setBrokerController(BrokerController brokerController) {
        this.brokerController = brokerController;
    }

    /**
     * In order to avoid check back unlimited, we will discard the message that have been checked more than a certain
     * number of times.
     * <p>
     *     为了避免无限制地检查，我们将丢弃已经检查超过一定次数的消息。
     * </p>
     * <p>
     *     我们进行通知，这个消息明确被我们要丢弃了
     * </p>
     *
     * @param msgExt Message to be discarded.
     */
    public abstract void resolveDiscardMsg(MessageExt msgExt);
}
