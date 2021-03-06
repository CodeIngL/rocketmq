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
package org.apache.rocketmq.broker.client.rebalance;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.common.message.MessageQueue;

/**
 * 负载均衡锁管理器,负责管理消费端的rebalance
 */
public class RebalanceLockManager {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.REBALANCE_LOCK_LOGGER_NAME);
    private final static long REBALANCE_LOCK_MAX_LIVE_TIME = Long.parseLong(System.getProperty("rocketmq.broker.rebalance.lockMaxLiveTime", "60000")); //锁定时间最多60s
    //锁定
    private final Lock lock = new ReentrantLock();
    //消费组和对应锁的信息
    private final ConcurrentMap<String/* group */, ConcurrentHashMap<MessageQueue, LockEntry>> mqLockTable = new ConcurrentHashMap<String, ConcurrentHashMap<MessageQueue, LockEntry>>(1024);

    //----------------------以下是一组锁操作--------------------//

    public boolean tryLock(final String group, final MessageQueue mq, final String clientId) {

        if (!this.isLocked(group, mq, clientId)) {
            try {
                this.lock.lockInterruptibly();
                try {
                    ConcurrentHashMap<MessageQueue, LockEntry> groupValue = this.mqLockTable.get(group);
                    if (null == groupValue) {
                        groupValue = new ConcurrentHashMap<>(32);
                        this.mqLockTable.put(group, groupValue);
                    }

                    LockEntry lockEntry = groupValue.get(mq);
                    if (null == lockEntry) {
                        lockEntry = new LockEntry();
                        lockEntry.setClientId(clientId);
                        groupValue.put(mq, lockEntry);
                        log.info("tryLock, message queue not locked, I got it. Group: {} NewClientId: {} {}",
                            group,
                            clientId,
                            mq);
                    }

                    if (lockEntry.isLocked(clientId)) {
                        lockEntry.setLastUpdateTimestamp(System.currentTimeMillis());
                        return true;
                    }

                    String oldClientId = lockEntry.getClientId();

                    if (lockEntry.isExpired()) {
                        lockEntry.setClientId(clientId);
                        lockEntry.setLastUpdateTimestamp(System.currentTimeMillis());
                        log.warn(
                            "tryLock, message queue lock expired, I got it. Group: {} OldClientId: {} NewClientId: {} {}",
                            group,
                            oldClientId,
                            clientId,
                            mq);
                        return true;
                    }

                    log.warn(
                        "tryLock, message queue locked by other client. Group: {} OtherClientId: {} NewClientId: {} {}",
                        group,
                        oldClientId,
                        clientId,
                        mq);
                    return false;
                } finally {
                    this.lock.unlock();
                }
            } catch (InterruptedException e) {
                log.error("putMessage exception", e);
            }
        } else {

        }

        return true;
    }

    /**
     * 是否已经被锁了
     * @param group 消息组
     * @param mq 消息队列
     * @param clientId 客户端id
     * @return
     */
    private boolean isLocked(final String group, final MessageQueue mq, final String clientId) {
        ConcurrentHashMap<MessageQueue, LockEntry> groupValue = this.mqLockTable.get(group); //获得消费组对应的消息队列和锁条目的映射
        if (groupValue != null) { //存在
            LockEntry lockEntry = groupValue.get(mq);//获得对应的锁条目
            if (lockEntry != null) {
                boolean locked = lockEntry.isLocked(clientId); //是否被该客户端锁定
                if (locked) {
                    //更新一下时间
                    lockEntry.setLastUpdateTimestamp(System.currentTimeMillis());
                }
                return locked;
            }
        }

        return false;
    }



    //----------------------以下是一组锁操作--------------------//
    /**
     * 尝试上锁
     * @param group 消费组
     * @param mqs 想要上锁消息队列
     * @param clientId 发起锁定的客户端
     * @return clientId客户端成功锁定的消息队列集合
     */
    public Set<MessageQueue> tryLockBatch(final String group, final Set<MessageQueue> mqs, final String clientId) {
        //存储锁定消息队列
        Set<MessageQueue> lockedMqs = new HashSet<>(mqs.size());
        //存储未锁定的消息队列
        Set<MessageQueue> notLockedMqs = new HashSet<>(mqs.size());

        //遍历进行分类
        for (MessageQueue mq : mqs) {
            if (this.isLocked(group, mq, clientId)) {
                lockedMqs.add(mq);
            } else {
                notLockedMqs.add(mq);
            }
        }

        if (!notLockedMqs.isEmpty()) {
            //存在未上锁的mq
            try {
                this.lock.lockInterruptibly();
                try {
                    //没有对应存储结构，使用结构
                    ConcurrentHashMap<MessageQueue, LockEntry> groupValue = this.mqLockTable.get(group);
                    if (null == groupValue) {
                        groupValue = new ConcurrentHashMap<>(32);
                        this.mqLockTable.put(group, groupValue);
                    }

                    //未没有上锁的进行上锁
                    for (MessageQueue mq : notLockedMqs) { //遍历未上锁的消息队列
                        LockEntry lockEntry = groupValue.get(mq); //构建锁项
                        if (null == lockEntry) {
                            lockEntry = new LockEntry();
                            lockEntry.setClientId(clientId);
                            groupValue.put(mq, lockEntry);
                            log.info("tryLockBatch, message queue not locked, I got it. Group: {} NewClientId: {} {}", group, clientId, mq);
                        }

                        //加入已锁定
                        if (lockEntry.isLocked(clientId)) {
                            lockEntry.setLastUpdateTimestamp(System.currentTimeMillis());
                            lockedMqs.add(mq);
                            continue;
                        }

                        //尝试获得上一个锁的拥有者
                        String oldClientId = lockEntry.getClientId();

                        if (lockEntry.isExpired()) {
                            //超时的话再更新一下现在的锁拥有者
                            lockEntry.setClientId(clientId);
                            lockEntry.setLastUpdateTimestamp(System.currentTimeMillis());
                            log.warn("tryLockBatch, message queue lock expired, I got it. Group: {} OldClientId: {} NewClientId: {} {}", group, oldClientId, clientId, mq);
                            lockedMqs.add(mq);
                            continue;
                        }

                        log.warn("tryLockBatch, message queue locked by other client. Group: {} OtherClientId: {} NewClientId: {} {}", group, oldClientId, clientId, mq);
                    }
                } finally {
                    this.lock.unlock();
                }
            } catch (InterruptedException e) {
                log.error("putMessage exception", e);
            }
        }

        return lockedMqs;
    }

    public void unlockBatch(final String group, final Set<MessageQueue> mqs, final String clientId) {
        try {
            this.lock.lockInterruptibly();
            try {
                ConcurrentHashMap<MessageQueue, LockEntry> groupValue = this.mqLockTable.get(group);
                if (null != groupValue) {
                    for (MessageQueue mq : mqs) {
                        LockEntry lockEntry = groupValue.get(mq);
                        if (null != lockEntry) {
                            if (lockEntry.getClientId().equals(clientId)) {
                                groupValue.remove(mq);
                                log.info("unlockBatch, Group: {} {} {}",
                                    group,
                                    mq,
                                    clientId);
                            } else {
                                log.warn("unlockBatch, but mq locked by other client: {}, Group: {} {} {}",
                                    lockEntry.getClientId(),
                                    group,
                                    mq,
                                    clientId);
                            }
                        } else {
                            log.warn("unlockBatch, but mq not locked, Group: {} {} {}",
                                group,
                                mq,
                                clientId);
                        }
                    }
                } else {
                    log.warn("unlockBatch, group not exist, Group: {} {}",
                        group,
                        clientId);
                }
            } finally {
                this.lock.unlock();
            }
        } catch (InterruptedException e) {
            log.error("putMessage exception", e);
        }
    }

    /**
     * 一个锁条目
     */
    static class LockEntry {
        //持有锁的的消费端
        private String clientId;
        //最后的更新时间
        private volatile long lastUpdateTimestamp = System.currentTimeMillis();

        public String getClientId() {
            return clientId;
        }

        public void setClientId(String clientId) {
            this.clientId = clientId;
        }

        public long getLastUpdateTimestamp() {
            return lastUpdateTimestamp;
        }

        public void setLastUpdateTimestamp(long lastUpdateTimestamp) {
            this.lastUpdateTimestamp = lastUpdateTimestamp;
        }

        /**
         * 是否被一个clientId锁定
         * @param clientId
         * @return
         */
        public boolean isLocked(final String clientId) {
            boolean eq = this.clientId.equals(clientId);
            return eq && !this.isExpired();
        }

        /**
         * 是否已经超时
         * @return
         */
        public boolean isExpired() {
            return (System.currentTimeMillis() - this.lastUpdateTimestamp) > REBALANCE_LOCK_MAX_LIVE_TIME; //6000
        }
    }
}
