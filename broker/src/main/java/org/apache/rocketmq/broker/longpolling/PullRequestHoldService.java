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
package org.apache.rocketmq.broker.longpolling;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.common.ServiceThread;
import org.apache.rocketmq.common.SystemClock;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.store.ConsumeQueueExt;
import org.apache.rocketmq.store.MessageFilter;

/**
 * consumer 拉取消息如果还没有消息，则可以阻塞一定的时间直到有新的消息或超时。
 * pullRequestHoldService用于维护这些拉取请求。
 */
public class PullRequestHoldService extends ServiceThread {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.BROKER_LOGGER_NAME);
    private static final String TOPIC_QUEUEID_SEPARATOR = "@";
    private final BrokerController brokerController;
    private final SystemClock systemClock = new SystemClock();
    //请求内存表
    private ConcurrentMap<String/* topic@queueId */, ManyPullRequest> pullRequestTable = new ConcurrentHashMap<String, ManyPullRequest>(1024);

    public PullRequestHoldService(final BrokerController brokerController) {
        this.brokerController = brokerController;
    }

    /**
     * 暂停拉取请求，
     * @param topic
     * @param queueId
     * @param req
     */
    public void suspendPullRequest(final String topic, final int queueId, final PullRequest req) {
        String key = this.buildKey(topic, queueId);
        ManyPullRequest mpr = this.pullRequestTable.get(key); //key对应的请求
        if (null == mpr) {
            mpr = new ManyPullRequest();
            ManyPullRequest prev = this.pullRequestTable.putIfAbsent(key, mpr);
            if (prev != null) {
                mpr = prev;
            }
        }
        mpr.addPullRequest(req);
    }

    private String buildKey(final String topic, final int queueId) {
        return new StringBuilder()
                .append(topic)
                .append(TOPIC_QUEUEID_SEPARATOR)
                .append(queueId)
                .toString();
    }

    /**
     * 挂起客户端对broker的拉取消息的请求，超时的时候，尝试调用一下，然后将消息推送给客户端，
     */
    @Override
    public void run() {
        String name = getServiceName();
        log.info("{} service started", name);
        while (!this.isStopped()) {
            try {
                if (this.brokerController.getBrokerConfig().isLongPollingEnable()) { //支持长轮训
                    this.waitForRunning(5 * 1000);//5秒中一次
                } else {
                    //支持短轮训的方式，超时的时间由我们设置
                    this.waitForRunning(this.brokerController.getBrokerConfig().getShortPollingTimeMills());
                }

                long beginLockTimestamp = this.systemClock.now();
                //一旦轮训时间到，我们需要去检查响应的请求
                this.checkHoldRequest();
                long costTime = this.systemClock.now() - beginLockTimestamp;
                if (costTime > 5 * 1000) {
                    log.info("[NOTIFYME] check hold request cost {} ms.", costTime);
                }
            } catch (Throwable e) {
                log.warn(name + " service has exception. ", e);
            }
        }

        log.info("{} service end", name);
    }

    @Override
    public String getServiceName() {
        return PullRequestHoldService.class.getSimpleName();
    }

    /**
     * 校验持有的的request
     */
    private void checkHoldRequest() {
        for (String key : this.pullRequestTable.keySet()) {
            String[] kArray = key.split(TOPIC_QUEUEID_SEPARATOR);
            if (2 != kArray.length){
                return;
            }
            String topic = kArray[0];
            int queueId = Integer.parseInt(kArray[1]);
            final long offset = this.brokerController.getMessageStore().getMaxOffsetInQueue(topic, queueId);
            try {
                //通知消息到达
                this.notifyMessageArriving(topic, queueId, offset);
            } catch (Throwable e) {
                log.error("check hold request failed. topic={}, queueId={}", topic, queueId, e);
            }
        }
    }

    /**
     * 通知
     * @param topic
     * @param queueId
     * @param maxOffset
     */
    public void notifyMessageArriving(final String topic, final int queueId, final long maxOffset) {
        notifyMessageArriving(topic, queueId, maxOffset, null, 0, null, null);
    }

    /**
     * 通知客户端现在存在消息，消息到达
     * @param topic
     * @param queueId
     * @param maxOffset
     * @param tagsCode
     * @param msgStoreTime
     * @param filterBitMap
     * @param properties
     */
    public void notifyMessageArriving(final String topic, final int queueId, final long maxOffset, final Long tagsCode, long msgStoreTime, byte[] filterBitMap, Map<String, String> properties) {
        //获得内存中维持关于长轮训的请求
        ManyPullRequest mpr = this.pullRequestTable.get(buildKey(topic, queueId));
        if (mpr == null) {
            return;
        }
        List<PullRequest> reqs = mpr.cloneListAndClear();
        if (reqs == null){
            return;
        }
        List<PullRequest> replayList = new ArrayList<PullRequest>();

        //遍历拉取的请求
        for (PullRequest req : reqs) {
            long newestOffset = maxOffset; //等于当前队列中最大的offset，
            if (newestOffset <= req.getPullFromThisOffset()) {//如果小了，我们再次更新一下，可能是错了
                newestOffset = this.brokerController.getMessageStore().getMaxOffsetInQueue(topic, queueId);
            }

            if (newestOffset > req.getPullFromThisOffset()) { //最新的offset大于请求拉取的offset
                MessageFilter filter = req.getMessageFilter(); //获得filter
                boolean match = filter.isMatchedByConsumeQueue(tagsCode, new ConsumeQueueExt.CqExtUnit(tagsCode, msgStoreTime, filterBitMap)); //请求是否匹配消费队列
                if (match && properties != null) { // 按位图匹配，当属性不为null时需要再次使用eval。match by bit map, need eval again when properties is not null.
                    match = filter.isMatchedByCommitLog(null, properties); //匹配成功如果属性存在，我们需要匹配一下commitlog中的消息
                }

                //匹配，我们唤醒，来重新处理这个长轮训的请求，因为我们之前没能成功进行返回消息。
                if (match) {
                    try {
                        this.brokerController.getPullMessageProcessor().executeRequestWhenWakeup(req.getClientChannel(), req.getRequestCommand());
                    } catch (Throwable e) {
                        log.error("execute request when wakeup failed.", e);
                    }
                    continue;
                }
            }

            if (System.currentTimeMillis() >= (req.getSuspendTimestamp() + req.getTimeoutMillis())) { //超出时间了，让我们轮训一下，看一下存在的消息是否已经到达我们的消费队列中了。
                try {
                    this.brokerController.getPullMessageProcessor().executeRequestWhenWakeup(req.getClientChannel(), req.getRequestCommand());
                } catch (Throwable e) {
                    log.error("execute request when wakeup failed.", e);
                }
                continue;
            }

            replayList.add(req); //其他不符合的请求，我们等到下一轮
        }

        if (!replayList.isEmpty()) {
            mpr.addPullRequest(replayList);
        }
    }
}
