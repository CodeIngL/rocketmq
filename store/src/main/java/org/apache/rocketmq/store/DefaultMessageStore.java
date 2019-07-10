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
package org.apache.rocketmq.store;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.FileLock;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.ServiceThread;
import org.apache.rocketmq.common.SystemClock;
import org.apache.rocketmq.common.ThreadFactoryImpl;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageExtBatch;
import org.apache.rocketmq.common.running.RunningStats;
import org.apache.rocketmq.common.sysflag.MessageSysFlag;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.store.config.BrokerRole;
import org.apache.rocketmq.store.config.MessageStoreConfig;
import org.apache.rocketmq.store.config.StorePathConfigHelper;
import org.apache.rocketmq.store.dledger.DLedgerCommitLog;
import org.apache.rocketmq.store.ha.HAService;
import org.apache.rocketmq.store.index.IndexService;
import org.apache.rocketmq.store.index.QueryOffsetResult;
import org.apache.rocketmq.store.schedule.ScheduleMessageService;
import org.apache.rocketmq.store.stats.BrokerStatsManager;

import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;
import static org.apache.rocketmq.common.sysflag.MessageSysFlag.*;
import static org.apache.rocketmq.store.GetMessageStatus.*;
import static org.apache.rocketmq.store.PutMessageStatus.*;
import static org.apache.rocketmq.store.config.BrokerRole.SLAVE;
import static org.apache.rocketmq.store.config.StorePathConfigHelper.getAbortFile;
import static org.apache.rocketmq.store.config.StorePathConfigHelper.getStorePathConsumeQueue;

/**
 * 默认的消息存储实现。存储消息作用
 */
public class DefaultMessageStore implements MessageStore {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);

    // 消息存储服务的配置项
    private final MessageStoreConfig messageStoreConfig;

    // 提交日志
    private final CommitLog commitLog;

    // topic,queueId(int类型),ConsumeQueue
    private final ConcurrentMap<String/* topic */, ConcurrentMap<Integer/* queueId */, ConsumeQueue>> consumeQueueTable;

    private final FlushConsumeQueueService flushConsumeQueueService;

    private final CleanCommitLogService cleanCommitLogService;

    private final CleanConsumeQueueService cleanConsumeQueueService;

    private final IndexService indexService;

    private final AllocateMappedFileService allocateMappedFileService;

    //重投递服务，负责将commitlog投递到consumerqueue中
    private final ReputMessageService reputMessageService;

    //高可用服务
    private final HAService haService;

    private final ScheduleMessageService scheduleMessageService;

    private final StoreStatsService storeStatsService;

    //是否支持瞬态，先写入该pool然后再写入批量写入page
    private final TransientStorePool transientStorePool;

    private final RunningFlags runningFlags = new RunningFlags();
    private final SystemClock systemClock = new SystemClock();

    private final ScheduledExecutorService scheduledExecutorService = newSingleThreadScheduledExecutor(new ThreadFactoryImpl("StoreScheduledThread"));
    private final BrokerStatsManager brokerStatsManager;
    private final MessageArrivingListener messageArrivingListener;
    private final BrokerConfig brokerConfig;

    private volatile boolean shutdown = true;

    private StoreCheckpoint storeCheckpoint;

    private AtomicLong printTimes = new AtomicLong(0);

    private final LinkedList<CommitLogDispatcher> dispatcherList;

    /**
     * 锁文件
     */
    private RandomAccessFile lockFile;

    /**
     * 文件锁
     */
    private FileLock lock;

    boolean shutDownNormal = false;

    /**
     * 构建默认的消息存储服务
     *
     * @param messageStoreConfig
     * @param brokerStatsManager
     * @param messageArrivingListener
     * @param brokerConfig
     * @throws IOException
     */
    public DefaultMessageStore(final MessageStoreConfig messageStoreConfig, final BrokerStatsManager brokerStatsManager,
                               final MessageArrivingListener messageArrivingListener, final BrokerConfig brokerConfig) throws IOException {
        //消息到达时的监听器
        this.messageArrivingListener = messageArrivingListener;
        //broker的配置项
        this.brokerConfig = brokerConfig;
        //存储服务相关的配置项
        this.messageStoreConfig = messageStoreConfig;
        //broker状态的管理器
        this.brokerStatsManager = brokerStatsManager;
        //是否允许文件映射服务
        this.allocateMappedFileService = new AllocateMappedFileService(this);
        //是否开启Dledger
        if (messageStoreConfig.isEnableDLegerCommitLog()) {
            this.commitLog = new DLedgerCommitLog(this);
        } else {
            this.commitLog = new CommitLog(this);
        }

        //消费者表
        this.consumeQueueTable = new ConcurrentHashMap<>(32);

        //刷新服务
        this.flushConsumeQueueService = new FlushConsumeQueueService();
        //清楚commitlog的服务
        this.cleanCommitLogService = new CleanCommitLogService();
        //清除消费者队列的服务
        this.cleanConsumeQueueService = new CleanConsumeQueueService();
        //存储的状态服务
        this.storeStatsService = new StoreStatsService();
        //索引服务
        this.indexService = new IndexService(this);

        //是否开启高可用的服务
        if (!messageStoreConfig.isEnableDLegerCommitLog()) {
            this.haService = new HAService(this);//使用Dledger来构建高可用，没有使用的话，我们需要开启单独的高可用服务来实现这个服务。。
        } else {
            this.haService = null;
        }

        //重置消息的服务
        this.reputMessageService = new ReputMessageService();

        //调度消息的服务
        this.scheduleMessageService = new ScheduleMessageService(this);

        //持久化存储
        this.transientStorePool = new TransientStorePool(messageStoreConfig);

        //是否开启
        if (messageStoreConfig.isTransientStorePoolEnable()) {
            this.transientStorePool.init();
        }

        //文件映射开启
        this.allocateMappedFileService.start();

        //索引服务开启
        this.indexService.start();

        //分发的列表
        this.dispatcherList = new LinkedList<>();
        //构建分发给consumeQueue的分发器
        this.dispatcherList.addLast(new CommitLogDispatcherBuildConsumeQueue());
        //构建分发给index的分发器
        this.dispatcherList.addLast(new CommitLogDispatcherBuildIndex());

        //构建锁
        File file = new File(StorePathConfigHelper.getLockFile(messageStoreConfig.getStorePathRootDir()));
        MappedFile.ensureDirOK(file.getParent());
        //锁的文件
        lockFile = new RandomAccessFile(file, "rw");
    }

    public void truncateDirtyLogicFiles(long phyOffset) {
        ConcurrentMap<String, ConcurrentMap<Integer, ConsumeQueue>> tables = DefaultMessageStore.this.consumeQueueTable;

        for (ConcurrentMap<Integer, ConsumeQueue> maps : tables.values()) {
            for (ConsumeQueue logic : maps.values()) {
                logic.truncateDirtyLogicFiles(phyOffset);
            }
        }
    }

    /**
     * 初始化，
     * 由commitlog开始加载，commitlog用于管理存储元数据
     *
     * @throws IOException
     */
    public boolean load() {
        boolean result = true;
        try {
            boolean lastExitOK = !this.isTempFileExist();
            log.info("last shutdown {}", lastExitOK ? "normally" : "abnormally");

            //调度消息的服务
            if (null != scheduleMessageService) {
                result = result && this.scheduleMessageService.load();
            }

            // load Commit Log
            // 加载消息的存储服务，所有消息都顺序存储在commitLog中
            result = result && this.commitLog.load();

            // load Consume Queue
            // 加载对应的消费队列。
            result = result && this.loadConsumeQueue();

            if (result) {
                //存储校验
                this.storeCheckpoint = new StoreCheckpoint(StorePathConfigHelper.getStoreCheckpoint(this.messageStoreConfig.getStorePathRootDir()));

                //根据上一次是否是正常退出来加载索引
                this.indexService.load(lastExitOK);

                //根据上一次是否是正常退出来进行恢复
                this.recover(lastExitOK);

                log.info("load over, and the max phy offset = {}", this.getMaxPhyOffset());
            }
        } catch (Exception e) {
            log.error("load exception", e);
            result = false;
        }

        if (!result) {
            this.allocateMappedFileService.shutdown();
        }

        return result;
    }

    /**
     * 消息存储启动
     *
     * @throws Exception
     */
    public void start() throws Exception {

        lock = lockFile.getChannel().tryLock(0, 1, false);
        if (lock == null || lock.isShared() || !lock.isValid()) {
            throw new RuntimeException("Lock failed,MQ already started");
        }

        lockFile.getChannel().write(ByteBuffer.wrap("lock".getBytes()));
        lockFile.getChannel().force(true);
        {
            /**
             * 1. Make sure the fast-forward messages to be truncated during the recovering according to the max physical offset of the commitlog;
             * 2. DLedger committedPos may be missing, so the maxPhysicalPosInLogicQueue maybe bigger that maxOffset returned by DLedgerCommitLog, just let it go;
             * 3. Calculate the reput offset according to the consume queue;
             * 4. Make sure the fall-behind messages to be dispatched before starting the commitlog, especially when the broker role are automatically changed.
             * 1. 根据commitlog的最大物理偏移量，确保在恢复过程中截断快进消息;
             * 2. DLedger committedPos可能丢失，因此maxPhysicalPosInLogicQueue可能比DLedgerCommitLog返回的maxOffset更大，只是让它去;
             * 3. 根据消耗队列计算输出偏移量;
             * 4. 确保在启动commitlog之前调度后备消息，尤其是在自动更改代理角色时。
             */
            long maxPhysicalPosInLogicQueue = commitLog.getMinOffset();
            for (ConcurrentMap<Integer, ConsumeQueue> maps : this.consumeQueueTable.values()) {
                for (ConsumeQueue logic : maps.values()) {
                    if (logic.getMaxPhysicOffset() > maxPhysicalPosInLogicQueue) {
                        maxPhysicalPosInLogicQueue = logic.getMaxPhysicOffset();
                    }
                }
            }
            if (maxPhysicalPosInLogicQueue < 0) {
                maxPhysicalPosInLogicQueue = 0;
            }
            if (maxPhysicalPosInLogicQueue < this.commitLog.getMinOffset()) {
                maxPhysicalPosInLogicQueue = this.commitLog.getMinOffset();
                /**
                 * This happens in following conditions:
                 * 1. If someone removes all the consumequeue files or the disk get damaged.
                 * 2. Launch a new broker, and copy the commitlog from other brokers.
                 *
                 * All the conditions has the same in common that the maxPhysicalPosInLogicQueue should be 0.
                 * If the maxPhysicalPosInLogicQueue is gt 0, there maybe something wrong.
                 *
                 * 这种情况发生在以下情况：
                 * 1.如果有人删除所有消耗队列文件或磁盘损坏。
                 * 2.启动新代理，并从其他代理复制commitlog。
                 *
                 * 所有条件具有相同的共同点，即maxPhysicalPosInLogicQueue应为0。
                 * 如果maxPhysicalPosInLogicQueue为gt 0，则可能出现问题。
                 */
                log.warn("[TooSmallCqOffset] maxPhysicalPosInLogicQueue={} clMinOffset={}", maxPhysicalPosInLogicQueue, this.commitLog.getMinOffset());
            }
            log.info("[SetReputOffset] maxPhysicalPosInLogicQueue={} clMinOffset={} clMaxOffset={} clConfirmedOffset={}",
                    maxPhysicalPosInLogicQueue, this.commitLog.getMinOffset(), this.commitLog.getMaxOffset(), this.commitLog.getConfirmOffset());
            this.reputMessageService.setReputFromOffset(maxPhysicalPosInLogicQueue);
            this.reputMessageService.start();

            /**
             *  1. Finish dispatching the messages fall behind, then to start other services.
             *  2. DLedger committedPos may be missing, so here just require dispatchBehindBytes <= 0
             *
             *  1.完成调度消息落后，然后启动其他服务。
             *  2. DLedger committedPos可能丢失，所以这里只需要dispatchBehindBytes <= 0
             */
            while (true) {
                if (dispatchBehindBytes() <= 0) {
                    break;
                }
                Thread.sleep(1000);
                log.info("Try to finish doing reput the messages fall behind during the starting, reputOffset={} maxOffset={} behind={}", this.reputMessageService.getReputFromOffset(), this.getMaxPhyOffset(), this.dispatchBehindBytes());
            }
            this.recoverTopicQueueTable();
        }

        if (!messageStoreConfig.isEnableDLegerCommitLog()) {
            this.haService.start();
            this.handleScheduleMessageService(messageStoreConfig.getBrokerRole());
        }

        this.flushConsumeQueueService.start();
        this.commitLog.start();
        this.storeStatsService.start();

        this.createTempFile();
        this.addScheduleTask();
        this.shutdown = false;
    }


    public void shutdown() {
        if (!this.shutdown) {
            this.shutdown = true;

            this.scheduledExecutorService.shutdown();

            try {

                Thread.sleep(1000);
            } catch (InterruptedException e) {
                log.error("shutdown Exception, ", e);
            }

            if (this.scheduleMessageService != null) {
                this.scheduleMessageService.shutdown();
            }
            if (this.haService != null) {
                this.haService.shutdown();
            }

            this.storeStatsService.shutdown();
            this.indexService.shutdown();
            this.commitLog.shutdown();
            this.reputMessageService.shutdown();
            this.flushConsumeQueueService.shutdown();
            this.allocateMappedFileService.shutdown();
            this.storeCheckpoint.flush();
            this.storeCheckpoint.shutdown();

            if (this.runningFlags.isWriteable() && dispatchBehindBytes() == 0) {
                this.deleteFile(getAbortFile(this.messageStoreConfig.getStorePathRootDir()));
                shutDownNormal = true;
            } else {
                log.warn("the store may be wrong, so shutdown abnormally, and keep abort file.");
            }
        }

        this.transientStorePool.destroy();

        if (lockFile != null && lock != null) {
            try {
                lock.release();
                lockFile.close();
            } catch (IOException e) {
            }
        }
    }

    /**
     * 销毁
     */
    public void destroy() {
        this.destroyLogics();
        this.commitLog.destroy();
        this.indexService.destroy();
        this.deleteFile(getAbortFile(this.messageStoreConfig.getStorePathRootDir()));
        this.deleteFile(StorePathConfigHelper.getStoreCheckpoint(this.messageStoreConfig.getStorePathRootDir()));
    }

    public void destroyLogics() {
        for (ConcurrentMap<Integer, ConsumeQueue> maps : this.consumeQueueTable.values()) {
            for (ConsumeQueue logic : maps.values()) {
                logic.destroy();
            }
        }
    }

    private PutMessageResult check(Message msg) {
        if (this.shutdown) { //；已经退出了，即服务现在不可用了
            log.warn("message store has shutdown, so putMessage is forbidden");
            return new PutMessageResult(SERVICE_NOT_AVAILABLE);//服务不可用
        }

        //broker角色是slave，无法投递producer的消息
        if (SLAVE == this.messageStoreConfig.getBrokerRole()) {
            long value = this.printTimes.getAndIncrement();
            if ((value % 50000) == 0) {
                log.warn("message store is slave mode, so putMessage is forbidden ");
            }
            return new PutMessageResult(SERVICE_NOT_AVAILABLE);//服务不可用
        }

        //broker的消息存储服务不可写
        if (!this.runningFlags.isWriteable()) {
            long value = this.printTimes.getAndIncrement();
            if ((value % 50000) == 0) {
                log.warn("message store is not writeable, so putMessage is forbidden " + this.runningFlags.getFlagBits());
            }
            return new PutMessageResult(SERVICE_NOT_AVAILABLE);//服务不可用
        } else {
            this.printTimes.set(0);
        }

        //producer投递消息的topic长度非法
        if (msg.getTopic().length() > Byte.MAX_VALUE) {
            log.warn("putMessage message topic length too long " + msg.getTopic().length());
            return new PutMessageResult(MESSAGE_ILLEGAL);
        }

        //消息体太长了
        if (msg.getBody().length > messageStoreConfig.getMaxMessageSize()) {
            log.warn("PutMessages body length too long " + msg.getBody().length);
            return new PutMessageResult(MESSAGE_ILLEGAL);
        }

        //os页缓存繁忙
        if (this.isOSPageCacheBusy()) {
            return new PutMessageResult(OS_PAGECACHE_BUSY);
        }

        return null;
    }

    /**
     * 存储引擎，处理投递的消息
     * 投递并处理来自producer的发送的消息。除了对消息本身特性的相关检查，不再检查消息的内部中相关标记
     * 如果是一阶段消息，那么消息已经被转换为half消息了
     *
     * @param msg Message instance to store 要存储消息的结果
     * @return 投递结果
     */
    public PutMessageResult putMessage(MessageExtBrokerInner msg) {
        PutMessageResult x = check(msg);
        if (x != null) return x;
        //producer投递消息的属性过长非法
        String propertiesStr = msg.getPropertiesString();
        if (propertiesStr != null && propertiesStr.length() > Short.MAX_VALUE) {
            log.warn("putMessage message properties length too long " + propertiesStr.length());
            return new PutMessageResult(PROPERTIES_SIZE_EXCEEDED);
        }
        //开始投递时间
        long beginTime = this.getSystemClock().now();
        //向commitLog投递消息，写入存储
        PutMessageResult result = this.commitLog.putMessage(msg);

        //消耗时间
        long eclipseTime = this.getSystemClock().now() - beginTime;
        if (eclipseTime > 500) {
            log.warn("putMessage not in lock eclipse time(ms)={}, bodyLength={}", eclipseTime, msg.getBody().length);
        }
        //设置相关状态进行统计
        this.storeStatsService.setPutMessageEntireTimeMax(eclipseTime);

        //投递失败，设置相关状态
        if (null == result || !result.isOk()) {
            this.storeStatsService.getPutMessageFailedTimes().incrementAndGet();
        }
        //返回投递结果
        return result;
    }

    /**
     * 投递批量消息
     *
     * @param messageExtBatch Message batch.
     * @return
     */
    public PutMessageResult putMessages(MessageExtBatch messageExtBatch) {
        PutMessageResult x = check(messageExtBatch);
        if (x != null) return x;

        //开始投递时间
        long beginTime = this.getSystemClock().now();
        //commitLog处理
        PutMessageResult result = this.commitLog.putMessages(messageExtBatch);

        long eclipseTime = this.getSystemClock().now() - beginTime;
        if (eclipseTime > 500) {
            log.warn("not in lock eclipse time(ms)={}, bodyLength={}", eclipseTime, messageExtBatch.getBody().length);
        }
        //设置相关状态
        this.storeStatsService.setPutMessageEntireTimeMax(eclipseTime);

        //投递失败，设置相关状态
        if (null == result || !result.isOk()) {
            this.storeStatsService.getPutMessageFailedTimes().incrementAndGet();
        }

        return result;
    }


    //oscachebusy=true意味着一条消息存储的时间超过了5s，这是有问题的，磁盘，网络磁盘，
    @Override
    public boolean isOSPageCacheBusy() {
        //开始时间
        long begin = this.getCommitLog().getBeginTimeInLock();
        //持续时间
        long diff = this.systemClock.now() - begin;
        //osPageCache繁忙
        return diff < 10000000 && diff > this.messageStoreConfig.getOsPageCacheBusyTimeOutMills();
    }

    @Override
    public long lockTimeMills() {
        return this.commitLog.lockTimeMills();
    }

    public SystemClock getSystemClock() {
        return systemClock;
    }

    public CommitLog getCommitLog() {
        return commitLog;
    }

    /**
     * 查询获得一批消息
     * @param group Consumer group that launches this query.
     * @param topic Topic to query.
     * @param queueId Queue ID to query.
     * @param offset Logical offset to start from.
     * @param maxMsgNums Maximum count of messages to query.
     * @param filter Message filter used to screen desired messages.
     * @return
     */
    public GetMessageResult getMessage(final String group, final String topic, final int queueId, final long offset, final int maxMsgNums, final MessageFilter filter) {
        if (this.shutdown) {
            log.warn("message store has shutdown, so getMessage is forbidden");
            return null;
        }

        if (!this.runningFlags.isReadable()) {
            log.warn("message store is not readable, so getMessage is forbidden " + this.runningFlags.getFlagBits());
            return null;
        }

        long beginTime = this.getSystemClock().now(); //开始时间

        GetMessageStatus status = NO_MESSAGE_IN_QUEUE;
        long nextBeginOffset = offset;
        long minOffset = 0;
        long maxOffset = 0;

        //消息存储结果
        GetMessageResult result = new GetMessageResult();

        //消息存储当前最大偏移量
        final long maxOffsetPy = this.commitLog.getMaxOffset();

        //找到匹配的消费队列
        ConsumeQueue cq = findConsumeQueue(topic, queueId);
        if (cq != null) {
            minOffset = cq.getMinOffsetInQueue();//消费队列维持的最小offset
            maxOffset = cq.getMaxOffsetInQueue();//消费队列维持的最大offset

            if (maxOffset == 0) { //消费队列没有消息
                status = NO_MESSAGE_IN_QUEUE;
                nextBeginOffset = nextOffsetCorrection(offset, 0); //调整下一个开始的offset，为0
            } else if (offset < minOffset) { //偏移量过小
                status = OFFSET_TOO_SMALL;
                nextBeginOffset = nextOffsetCorrection(offset, minOffset);//调整下一个开始为消息队列上最小的offset
            } else if (offset == maxOffset) { //偏移量超出1
                status = OFFSET_OVERFLOW_ONE;
                nextBeginOffset = nextOffsetCorrection(offset, offset);//不做任何处理，过大了，消费完了
            } else if (offset > maxOffset) { //太大了
                status = OFFSET_OVERFLOW_BADLY;
                if (0 == minOffset) {
                    nextBeginOffset = nextOffsetCorrection(offset, minOffset); //最小为0，使用0
                } else {
                    nextBeginOffset = nextOffsetCorrection(offset, maxOffset);//使用最大
                }
            } else {
                //获得offset对应的映射buffer
                SelectMappedBufferResult bufferConsumeQueue = cq.getIndexBuffer(offset);
                if (bufferConsumeQueue != null) { //索引存在相关
                    try {
                        status = NO_MATCHED_MESSAGE; //状态，初始没有任何匹配

                        long nextPhyFileStartOffset = Long.MIN_VALUE;//初始值
                        long maxPhyOffsetPulling = 0; //初始值

                        int i = 0;
                        //最大过滤量
                        final int maxFilterMessageCount = Math.max(16000, maxMsgNums * ConsumeQueue.CQ_STORE_UNIT_SIZE);
                        //磁盘满了记录标记
                        final boolean diskFallRecorded = this.messageStoreConfig.isDiskFallRecorded();

                        //扩展属性
                        ConsumeQueueExt.CqExtUnit cqExtUnit = new ConsumeQueueExt.CqExtUnit();
                        //进行遍历
                        for (; i < bufferConsumeQueue.getSize() && i < maxFilterMessageCount; i += ConsumeQueue.CQ_STORE_UNIT_SIZE) {
                            //物理的offset
                            long offsetPy = bufferConsumeQueue.getByteBuffer().getLong();
                            //物理的size
                            int sizePy = bufferConsumeQueue.getByteBuffer().getInt();
                            //物理的taghashcode，或者是扩展属性中的地址
                            long tagsCode = bufferConsumeQueue.getByteBuffer().getLong();

                            //更新
                            maxPhyOffsetPulling = offsetPy;

                            if (nextPhyFileStartOffset != Long.MIN_VALUE) {
                                if (offsetPy < nextPhyFileStartOffset)
                                    continue;
                            }

                            //是否在磁盘中
                            boolean isInDisk = checkInDiskByCommitOffset(offsetPy, maxOffsetPy);

                            if (this.isTheBatchFull(sizePy, maxMsgNums, result.getBufferTotalSize(), result.getMessageCount(), isInDisk)) {
                                break;
                            }

                            //可能的扩展属性操作

                            boolean extRet = false, isTagsCodeLegal = true;
                            if (cq.isExtAddr(tagsCode)) { //是的地址
                                extRet = cq.getExt(tagsCode, cqExtUnit);
                                if (extRet) { //是，使用扩展中保存的code
                                    tagsCode = cqExtUnit.getTagsCode();
                                } else {
                                    // can't find ext content.Client will filter messages by tag also.
                                    log.error("[BUG] can't find consume queue extend file content!addr={}, offsetPy={}, sizePy={}, topic={}, group={}",
                                            tagsCode, offsetPy, sizePy, topic, group);
                                    isTagsCodeLegal = false;
                                }
                            }

                            //过滤和匹配
                            if (filter != null && !filter.isMatchedByConsumeQueue(isTagsCodeLegal ? tagsCode : null, extRet ? cqExtUnit : null)) {
                                if (result.getBufferTotalSize() == 0) {
                                    status = NO_MATCHED_MESSAGE;
                                }
                                continue;
                            }

                            //获得消息
                            SelectMappedBufferResult selectResult = this.commitLog.getMessage(offsetPy, sizePy);
                            if (null == selectResult) {
                                if (result.getBufferTotalSize() == 0) {
                                    status = MESSAGE_WAS_REMOVING;
                                }
                                nextPhyFileStartOffset = this.commitLog.rollNextFile(offsetPy);
                                continue;
                            }

                            //匹配消息是否与存储的消息是否匹配
                            if (filter != null && !filter.isMatchedByCommitLog(selectResult.getByteBuffer().slice(), null)) {
                                if (result.getBufferTotalSize() == 0) {
                                    status = NO_MATCHED_MESSAGE;
                                }
                                // release...
                                selectResult.release();
                                continue;
                            }

                            this.storeStatsService.getGetMessageTransferedMsgCount().incrementAndGet();
                            result.addMessage(selectResult); //找到消息
                            status = FOUND;
                            nextPhyFileStartOffset = Long.MIN_VALUE;
                        }

                        if (diskFallRecorded) { //记录落后
                            long fallBehind = maxOffsetPy - maxPhyOffsetPulling; //落后长度
                            brokerStatsManager.recordDiskFallBehindSize(group, topic, queueId, fallBehind); //记录一下
                        }

                        nextBeginOffset = offset + (i / ConsumeQueue.CQ_STORE_UNIT_SIZE); //下一个开始

                        long diff = maxOffsetPy - maxPhyOffsetPulling; //最大内存-最大拉取offset
                        long memory = (long) (StoreUtil.TOTAL_PHYSICAL_MEMORY_SIZE * (this.messageStoreConfig.getAccessMessageInMemoryMaxRatio() / 100.0)); //内存大小
                        result.setSuggestPullingFromSlave(diff > memory); //是否建议从其他slave进行拉取，差距过大，可能master很麻烦，我们尝试从slave读取
                    } finally {
                        bufferConsumeQueue.release();
                    }
                } else {
                    status = OFFSET_FOUND_NULL;
                    nextBeginOffset = nextOffsetCorrection(offset, cq.rollNextFile(offset));
                    log.warn("consumer request topic: " + topic + "offset: " + offset + " minOffset: " + minOffset + " maxOffset: " + maxOffset + ", but access logic queue failed.");
                }
            }
        } else {
            status = NO_MATCHED_LOGIC_QUEUE;
            nextBeginOffset = nextOffsetCorrection(offset, 0);
        }

        if (FOUND == status) {
            this.storeStatsService.getGetMessageTimesTotalFound().incrementAndGet();
        } else {
            this.storeStatsService.getGetMessageTimesTotalMiss().incrementAndGet();
        }
        long eclipseTime = this.getSystemClock().now() - beginTime;
        this.storeStatsService.setGetMessageEntireTimeMax(eclipseTime);

        result.setStatus(status);
        result.setNextBeginOffset(nextBeginOffset);
        result.setMaxOffset(maxOffset);
        result.setMinOffset(minOffset);
        return result;
    }

    public long getMaxOffsetInQueue(String topic, int queueId) {
        ConsumeQueue logic = this.findConsumeQueue(topic, queueId);
        if (logic != null) {
            long offset = logic.getMaxOffsetInQueue();
            return offset;
        }

        return 0;
    }

    public long getMinOffsetInQueue(String topic, int queueId) {
        ConsumeQueue logic = this.findConsumeQueue(topic, queueId);
        if (logic != null) {
            return logic.getMinOffsetInQueue();
        }

        return -1;
    }

    @Override
    public long getCommitLogOffsetInQueue(String topic, int queueId, long consumeQueueOffset) {
        ConsumeQueue consumeQueue = findConsumeQueue(topic, queueId);
        if (consumeQueue != null) {
            SelectMappedBufferResult bufferConsumeQueue = consumeQueue.getIndexBuffer(consumeQueueOffset);
            if (bufferConsumeQueue != null) {
                try {
                    long offsetPy = bufferConsumeQueue.getByteBuffer().getLong();
                    return offsetPy;
                } finally {
                    bufferConsumeQueue.release();
                }
            }
        }

        return 0;
    }

    public long getOffsetInQueueByTime(String topic, int queueId, long timestamp) {
        ConsumeQueue logic = this.findConsumeQueue(topic, queueId);
        if (logic != null) {
            return logic.getOffsetInQueueByTime(timestamp);
        }

        return 0;
    }

    /**
     * 查找消息
     * @param commitLogOffset physical offset.
     * @return
     */
    public MessageExt lookMessageByOffset(long commitLogOffset) {
        //获得选择的消息
        SelectMappedBufferResult sbr = this.commitLog.getMessage(commitLogOffset, 4);
        if (null != sbr) {
            try {
                // 1 TOTALSIZE，获得消息的总长度
                int size = sbr.getByteBuffer().getInt();
                return lookMessageByOffset(commitLogOffset, size);
            } finally {
                sbr.release();
            }
        }

        return null;
    }

    @Override
    public SelectMappedBufferResult selectOneMessageByOffset(long commitLogOffset) {
        SelectMappedBufferResult sbr = this.commitLog.getMessage(commitLogOffset, 4);
        if (null != sbr) {
            try {
                // 1 TOTALSIZE
                int size = sbr.getByteBuffer().getInt();
                return this.commitLog.getMessage(commitLogOffset, size);
            } finally {
                sbr.release();
            }
        }

        return null;
    }

    @Override
    public SelectMappedBufferResult selectOneMessageByOffset(long commitLogOffset, int msgSize) {
        return this.commitLog.getMessage(commitLogOffset, msgSize);
    }

    public String getRunningDataInfo() {
        return this.storeStatsService.toString();
    }

    @Override
    public HashMap<String, String> getRuntimeInfo() {
        HashMap<String, String> result = this.storeStatsService.getRuntimeInfo();

        {
            String storePathPhysic = DefaultMessageStore.this.getMessageStoreConfig().getStorePathCommitLog();
            double physicRatio = UtilAll.getDiskPartitionSpaceUsedPercent(storePathPhysic);
            result.put(RunningStats.commitLogDiskRatio.name(), String.valueOf(physicRatio));

        }

        {

            String storePathLogics = getStorePathConsumeQueue(this.messageStoreConfig.getStorePathRootDir());
            double logicsRatio = UtilAll.getDiskPartitionSpaceUsedPercent(storePathLogics);
            result.put(RunningStats.consumeQueueDiskRatio.name(), String.valueOf(logicsRatio));
        }

        {
            if (this.scheduleMessageService != null) {
                this.scheduleMessageService.buildRunningStats(result);
            }
        }

        result.put(RunningStats.commitLogMinOffset.name(), String.valueOf(DefaultMessageStore.this.getMinPhyOffset()));
        result.put(RunningStats.commitLogMaxOffset.name(), String.valueOf(DefaultMessageStore.this.getMaxPhyOffset()));

        return result;
    }

    @Override
    public long getMaxPhyOffset() {
        return this.commitLog.getMaxOffset();
    }

    @Override
    public long getMinPhyOffset() {
        return this.commitLog.getMinOffset();
    }

    @Override
    public long getEarliestMessageTime(String topic, int queueId) {
        ConsumeQueue logicQueue = this.findConsumeQueue(topic, queueId);
        if (logicQueue != null) {
            long minLogicOffset = logicQueue.getMinLogicOffset();

            SelectMappedBufferResult result = logicQueue.getIndexBuffer(minLogicOffset / ConsumeQueue.CQ_STORE_UNIT_SIZE);
            return getStoreTime(result);
        }

        return -1;
    }

    private long getStoreTime(SelectMappedBufferResult result) {
        if (result != null) {
            try {
                final long phyOffset = result.getByteBuffer().getLong();
                final int size = result.getByteBuffer().getInt();
                long storeTime = this.getCommitLog().pickupStoreTimestamp(phyOffset, size);
                return storeTime;
            } catch (Exception e) {
            } finally {
                result.release();
            }
        }
        return -1;
    }

    @Override
    public long getEarliestMessageTime() {
        final long minPhyOffset = this.getMinPhyOffset();
        final int size = this.messageStoreConfig.getMaxMessageSize() * 2;
        return this.getCommitLog().pickupStoreTimestamp(minPhyOffset, size);
    }

    @Override
    public long getMessageStoreTimeStamp(String topic, int queueId, long consumeQueueOffset) {
        ConsumeQueue logicQueue = this.findConsumeQueue(topic, queueId);
        if (logicQueue != null) {
            SelectMappedBufferResult result = logicQueue.getIndexBuffer(consumeQueueOffset);
            return getStoreTime(result);
        }

        return -1;
    }

    @Override
    public long getMessageTotalInQueue(String topic, int queueId) {
        ConsumeQueue logicQueue = this.findConsumeQueue(topic, queueId);
        if (logicQueue != null) {
            return logicQueue.getMessageTotalInQueue();
        }

        return -1;
    }

    @Override
    public SelectMappedBufferResult getCommitLogData(final long offset) {
        if (this.shutdown) {
            log.warn("message store has shutdown, so getPhyQueueData is forbidden");
            return null;
        }

        return this.commitLog.getData(offset);
    }

    @Override
    public boolean appendToCommitLog(long startOffset, byte[] data) {
        if (this.shutdown) {
            log.warn("message store has shutdown, so appendToPhyQueue is forbidden");
            return false;
        }

        boolean result = this.commitLog.appendData(startOffset, data);
        if (result) {
            this.reputMessageService.wakeup();
        } else {
            log.error("appendToPhyQueue failed " + startOffset + " " + data.length);
        }

        return result;
    }

    @Override
    public void executeDeleteFilesManually() {
        this.cleanCommitLogService.excuteDeleteFilesManualy();
    }

    /**
     * 使用key方式进行查询消息
     * @param topic topic of the message.
     * @param key message key.
     * @param maxNum maximum number of the messages possible.
     * @param begin begin timestamp.
     * @param end end timestamp.
     * @return
     */
    @Override
    public QueryMessageResult queryMessage(String topic, String key, int maxNum, long begin, long end) {
        QueryMessageResult queryResult = new QueryMessageResult();

        long lastQueryMsgTime = end;

        for (int i = 0; i < 3; i++) {
            QueryOffsetResult queryOffsetResult = this.indexService.queryOffset(topic, key, maxNum, begin, lastQueryMsgTime); //查询index相关数据
            List<Long> phyOffsets = queryOffsetResult.getPhyOffsets();
            if (phyOffsets.isEmpty()) {
                break;
            }

            Collections.sort(phyOffsets);

            queryResult.setIndexLastUpdatePhyoffset(queryOffsetResult.getIndexLastUpdatePhyoffset());
            queryResult.setIndexLastUpdateTimestamp(queryOffsetResult.getIndexLastUpdateTimestamp());

            for (int m = 0; m < phyOffsets.size(); m++) {
                long offset = phyOffsets.get(m); //得到消息的offset

                try {

                    boolean match = true; //是否匹配
                    MessageExt msg = this.lookMessageByOffset(offset); //查找消息
                    if (0 == m) { //第一条
                        lastQueryMsgTime = msg.getStoreTimestamp();
                    }

//                    String[] keyArray = msg.getKeys().split(MessageConst.KEY_SEPARATOR);
//                    if (topic.equals(msg.getTopic())) {
//                        for (String k : keyArray) {
//                            if (k.equals(key)) {
//                                match = true;
//                                break;
//                            }
//                        }
//                    }

                    if (match) {
                        SelectMappedBufferResult result = this.commitLog.getData(offset, false); //获得数据
                        if (result != null) {
                            int size = result.getByteBuffer().getInt(0);
                            result.getByteBuffer().limit(size);
                            result.setSize(size);
                            queryResult.addMessage(result); //添加到结果中
                        }
                    } else {
                        log.warn("queryMessage hash duplicate, {} {}", topic, key);
                    }
                } catch (Exception e) {
                    log.error("queryMessage exception", e);
                }
            }

            if (queryResult.getBufferTotalSize() > 0) {
                break;
            }

            if (lastQueryMsgTime < begin) {
                break;
            }
        }

        return queryResult;
    }

    @Override
    public void updateHaMasterAddress(String newAddr) {
        this.haService.updateMasterAddress(newAddr);
    }

    @Override
    public long slaveFallBehindMuch() {
        return this.commitLog.getMaxOffset() - this.haService.getPush2SlaveMaxOffset().get();
    }

    @Override
    public long now() {
        return this.systemClock.now();
    }

    @Override
    public int cleanUnusedTopic(Set<String> topics) {
        Iterator<Entry<String, ConcurrentMap<Integer, ConsumeQueue>>> it = this.consumeQueueTable.entrySet().iterator();
        while (it.hasNext()) {
            Entry<String, ConcurrentMap<Integer, ConsumeQueue>> next = it.next();
            String topic = next.getKey();

            if (!topics.contains(topic) && !topic.equals(ScheduleMessageService.SCHEDULE_TOPIC)) {
                ConcurrentMap<Integer, ConsumeQueue> queueTable = next.getValue();
                for (ConsumeQueue cq : queueTable.values()) {
                    cq.destroy();
                    log.info("cleanUnusedTopic: {} {} ConsumeQueue cleaned",
                            cq.getTopic(),
                            cq.getQueueId()
                    );

                    this.commitLog.removeQueueFromTopicQueueTable(cq.getTopic(), cq.getQueueId());
                }
                it.remove();

                log.info("cleanUnusedTopic: {},topic destroyed", topic);
            }
        }

        return 0;
    }

    public void cleanExpiredConsumerQueue() {
        long minCommitLogOffset = this.commitLog.getMinOffset();

        Iterator<Entry<String, ConcurrentMap<Integer, ConsumeQueue>>> it = this.consumeQueueTable.entrySet().iterator();
        while (it.hasNext()) {
            Entry<String, ConcurrentMap<Integer, ConsumeQueue>> next = it.next();
            String topic = next.getKey();
            if (!topic.equals(ScheduleMessageService.SCHEDULE_TOPIC)) {
                ConcurrentMap<Integer, ConsumeQueue> queueTable = next.getValue();
                Iterator<Entry<Integer, ConsumeQueue>> itQT = queueTable.entrySet().iterator();
                while (itQT.hasNext()) {
                    Entry<Integer, ConsumeQueue> nextQT = itQT.next();
                    long maxCLOffsetInConsumeQueue = nextQT.getValue().getLastOffset();

                    if (maxCLOffsetInConsumeQueue == -1) {
                        log.warn("maybe ConsumeQueue was created just now. topic={} queueId={} maxPhysicOffset={} minLogicOffset={}.",
                                nextQT.getValue().getTopic(),
                                nextQT.getValue().getQueueId(),
                                nextQT.getValue().getMaxPhysicOffset(),
                                nextQT.getValue().getMinLogicOffset());
                    } else if (maxCLOffsetInConsumeQueue < minCommitLogOffset) {
                        log.info(
                                "cleanExpiredConsumerQueue: {} {} consumer queue destroyed, minCommitLogOffset: {} maxCLOffsetInConsumeQueue: {}",
                                topic,
                                nextQT.getKey(),
                                minCommitLogOffset,
                                maxCLOffsetInConsumeQueue);

                        DefaultMessageStore.this.commitLog.removeQueueFromTopicQueueTable(nextQT.getValue().getTopic(),
                                nextQT.getValue().getQueueId());

                        nextQT.getValue().destroy();
                        itQT.remove();
                    }
                }

                if (queueTable.isEmpty()) {
                    log.info("cleanExpiredConsumerQueue: {},topic destroyed", topic);
                    it.remove();
                }
            }
        }
    }

    public Map<String, Long> getMessageIds(final String topic, final int queueId, long minOffset, long maxOffset,
                                           SocketAddress storeHost) {
        Map<String, Long> messageIds = new HashMap<String, Long>();
        if (this.shutdown) {
            return messageIds;
        }

        ConsumeQueue consumeQueue = findConsumeQueue(topic, queueId);
        if (consumeQueue != null) {
            minOffset = Math.max(minOffset, consumeQueue.getMinOffsetInQueue());
            maxOffset = Math.min(maxOffset, consumeQueue.getMaxOffsetInQueue());

            if (maxOffset == 0) {
                return messageIds;
            }

            long nextOffset = minOffset;
            while (nextOffset < maxOffset) {
                SelectMappedBufferResult bufferConsumeQueue = consumeQueue.getIndexBuffer(nextOffset);
                if (bufferConsumeQueue != null) {
                    try {
                        int i = 0;
                        for (; i < bufferConsumeQueue.getSize(); i += ConsumeQueue.CQ_STORE_UNIT_SIZE) {
                            long offsetPy = bufferConsumeQueue.getByteBuffer().getLong();
                            final ByteBuffer msgIdMemory = ByteBuffer.allocate(MessageDecoder.MSG_ID_LENGTH);
                            String msgId =
                                    MessageDecoder.createMessageId(msgIdMemory, MessageExt.socketAddress2ByteBuffer(storeHost), offsetPy);
                            messageIds.put(msgId, nextOffset++);
                            if (nextOffset > maxOffset) {
                                return messageIds;
                            }
                        }
                    } finally {

                        bufferConsumeQueue.release();
                    }
                } else {
                    return messageIds;
                }
            }
        }
        return messageIds;
    }

    @Override
    public boolean checkInDiskByConsumeOffset(final String topic, final int queueId, long consumeOffset) {

        final long maxOffsetPy = this.commitLog.getMaxOffset();

        ConsumeQueue consumeQueue = findConsumeQueue(topic, queueId);
        if (consumeQueue != null) {
            SelectMappedBufferResult bufferConsumeQueue = consumeQueue.getIndexBuffer(consumeOffset);
            if (bufferConsumeQueue != null) {
                try {
                    for (int i = 0; i < bufferConsumeQueue.getSize(); ) {
                        i += ConsumeQueue.CQ_STORE_UNIT_SIZE;
                        long offsetPy = bufferConsumeQueue.getByteBuffer().getLong();
                        return checkInDiskByCommitOffset(offsetPy, maxOffsetPy);
                    }
                } finally {

                    bufferConsumeQueue.release();
                }
            } else {
                return false;
            }
        }
        return false;
    }

    @Override
    public long dispatchBehindBytes() {
        return this.reputMessageService.behind();
    }

    @Override
    public long flush() {
        return this.commitLog.flush();
    }

    @Override
    public boolean resetWriteOffset(long phyOffset) {
        return this.commitLog.resetOffset(phyOffset);
    }

    @Override
    public long getConfirmOffset() {
        return this.commitLog.getConfirmOffset();
    }

    @Override
    public void setConfirmOffset(long phyOffset) {
        this.commitLog.setConfirmOffset(phyOffset);
    }

    /**
     * 获得消息，消息在commitlog的偏移量加上消息的总长度
     * @param commitLogOffset
     * @param size
     * @return
     */
    public MessageExt lookMessageByOffset(long commitLogOffset, int size) {
        SelectMappedBufferResult sbr = this.commitLog.getMessage(commitLogOffset, size);
        if (null != sbr) {
            try {
                return MessageDecoder.decode(sbr.getByteBuffer(), true, false);
            } finally {
                sbr.release();
            }
        }

        return null;
    }

    /**
     * 找到topic-queueId对应消费队列，不存在我讲构建一个，当我们支持扩展属性，我们将同时使用扩展的enableConsumeQueueExt
     * @param topic
     * @param queueId
     * @return
     */
    public ConsumeQueue findConsumeQueue(String topic, int queueId) {
        ConcurrentMap<Integer, ConsumeQueue> map = consumeQueueTable.get(topic);
        if (null == map) {
            ConcurrentMap<Integer, ConsumeQueue> newMap = new ConcurrentHashMap<Integer, ConsumeQueue>(128);
            ConcurrentMap<Integer, ConsumeQueue> oldMap = consumeQueueTable.putIfAbsent(topic, newMap);
            if (oldMap != null) {
                map = oldMap;
            } else {
                map = newMap;
            }
        }

        //获得对应的消费队列，不存在，我构建相应的消费队列进行操作
        ConsumeQueue logic = map.get(queueId);
        if (null == logic) {
            ConsumeQueue newLogic = new ConsumeQueue(topic, queueId,
                    getStorePathConsumeQueue(this.messageStoreConfig.getStorePathRootDir()),
                    this.getMessageStoreConfig().getMapedFileSizeConsumeQueue(),
                    this);
            ConsumeQueue oldLogic = map.putIfAbsent(queueId, newLogic);
            if (oldLogic != null) {
                logic = oldLogic;
            } else {
                logic = newLogic;
            }
        }

        return logic;
    }

    /**
     * 更正下一个offset
     * @param oldOffset
     * @param newOffset
     * @return
     */
    private long nextOffsetCorrection(long oldOffset, long newOffset) {
        long nextOffset = oldOffset;
        if (this.getMessageStoreConfig().getBrokerRole() != SLAVE || this.getMessageStoreConfig().isOffsetCheckInSlave()) {
            nextOffset = newOffset;
        }
        return nextOffset;
    }

    /**
     * 是否可能在磁盘上，只要超过
     * @param offsetPy
     * @param maxOffsetPy
     * @return
     */
    private boolean checkInDiskByCommitOffset(long offsetPy, long maxOffsetPy) {
        //最大内存
        long memory = (long) (StoreUtil.TOTAL_PHYSICAL_MEMORY_SIZE * (this.messageStoreConfig.getAccessMessageInMemoryMaxRatio() / 100.0));
        //是否大，大在磁盘中
        return (maxOffsetPy - offsetPy) > memory;
    }

    /**
     * 是否批处理满？？
     * @param sizePy 物理大小
     * @param maxMsgNums 最大消息量
     * @param bufferTotal 总共大小
     * @param messageTotal 消息总共大小
     * @param isInDisk 是否在磁盘中
     * @return
     */
    private boolean isTheBatchFull(int sizePy, int maxMsgNums, int bufferTotal, int messageTotal, boolean isInDisk) {

        if (0 == bufferTotal || 0 == messageTotal) { //数量还是0直接返回，没满
            return false;
        }

        if (maxMsgNums <= messageTotal) { //最大消息量少于总共消息量，足够，返回true
            return true;
        }

        if (isInDisk) { //磁盘
            if ((bufferTotal + sizePy) > this.messageStoreConfig.getMaxTransferBytesOnMessageInDisk()) {//容量大于阈值，
                return true;
            }

            if (messageTotal > this.messageStoreConfig.getMaxTransferCountOnMessageInDisk() - 1) { //数量大于阈值
                return true;
            }
        } else {
            if ((bufferTotal + sizePy) > this.messageStoreConfig.getMaxTransferBytesOnMessageInMemory()) { //容量大于内存阈值
                return true;
            }

            if (messageTotal > this.messageStoreConfig.getMaxTransferCountOnMessageInMemory() - 1) { //数量大于内存阈值
                return true;
            }
        }

        return false;
    }

    private void deleteFile(final String fileName) {
        File file = new File(fileName);
        boolean result = file.delete();
        log.info(fileName + (result ? " delete OK" : " delete Failed"));
    }

    /**
     * @throws IOException
     */
    private void createTempFile() throws IOException {
        String fileName = getAbortFile(this.messageStoreConfig.getStorePathRootDir());
        File file = new File(fileName);
        MappedFile.ensureDirOK(file.getParent());
        boolean result = file.createNewFile();
        log.info(fileName + (result ? " create OK" : " already exists"));
    }

    /**
     * 添加定时调度的任务
     *
     */
    private void addScheduleTask() {

        this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                DefaultMessageStore.this.cleanFilesPeriodically();
            }
        }, 1000 * 60, this.messageStoreConfig.getCleanResourceInterval(), TimeUnit.MILLISECONDS);

        this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                DefaultMessageStore.this.checkSelf();
            }
        }, 1, 10, TimeUnit.MINUTES);

        this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                if (DefaultMessageStore.this.getMessageStoreConfig().isDebugLockEnable()) {
                    try {
                        if (DefaultMessageStore.this.commitLog.getBeginTimeInLock() != 0) {
                            long lockTime = System.currentTimeMillis() - DefaultMessageStore.this.commitLog.getBeginTimeInLock();
                            if (lockTime > 1000 && lockTime < 10000000) {

                                String stack = UtilAll.jstack();
                                final String fileName = System.getProperty("user.home") + File.separator + "debug/lock/stack-"
                                        + DefaultMessageStore.this.commitLog.getBeginTimeInLock() + "-" + lockTime;
                                MixAll.string2FileNotSafe(stack, fileName);
                            }
                        }
                    } catch (Exception e) {
                    }
                }
            }
        }, 1, 1, TimeUnit.SECONDS);

        // this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
        // @Override
        // public void run() {
        // DefaultMessageStore.this.cleanExpiredConsumerQueue();
        // }
        // }, 1, 1, TimeUnit.HOURS);
    }

    private void cleanFilesPeriodically() {
        this.cleanCommitLogService.run();
        this.cleanConsumeQueueService.run();
    }

    private void checkSelf() {
        this.commitLog.checkSelf();

        Iterator<Entry<String, ConcurrentMap<Integer, ConsumeQueue>>> it = this.consumeQueueTable.entrySet().iterator();
        while (it.hasNext()) {
            Entry<String, ConcurrentMap<Integer, ConsumeQueue>> next = it.next();
            Iterator<Entry<Integer, ConsumeQueue>> itNext = next.getValue().entrySet().iterator();
            while (itNext.hasNext()) {
                Entry<Integer, ConsumeQueue> cq = itNext.next();
                cq.getValue().checkSelf();
            }
        }
    }

    /**
     * abortFile文件存在，说明是无故宕机的
     *
     * @return
     */
    private boolean isTempFileExist() {
        return new File(getAbortFile(this.messageStoreConfig.getStorePathRootDir())).exists();
    }

    /**
     * 加载消费队列
     *
     * @return
     */
    private boolean loadConsumeQueue() {
        //加载路径
        String consumeQueueStorePath = getStorePathConsumeQueue(this.messageStoreConfig.getStorePathRootDir());
        File dirLogic = new File(consumeQueueStorePath);
        //
        File[] topics = dirLogic.listFiles();
        if (topics != null) {
            //topic
            for (File fileTopic : topics) {
                String topic = fileTopic.getName();

                //queue
                File[] QueueIds = fileTopic.listFiles();
                if (QueueIds != null) {
                    //文件名就是queueId
                    for (File fileQueueId : QueueIds) {
                        int queueId;
                        try {
                            queueId = Integer.parseInt(fileQueueId.getName());
                        } catch (NumberFormatException e) {
                            continue;
                        }
                        //构建消费队列
                        ConsumeQueue logic = new ConsumeQueue(topic, queueId, consumeQueueStorePath, this.getMessageStoreConfig().getMapedFileSizeConsumeQueue(), this);
                        //构建map
                        this.putConsumeQueue(topic, queueId, logic);
                        if (!logic.load()) {
                            return false;
                        }
                    }
                }
            }
        }

        log.info("load logics queue all over, OK");

        return true;
    }

    /**
     * 恢复
     *
     * @param lastExitOK false 代表宕机了
     */
    private void recover(final boolean lastExitOK) {
        //获得最大的物理偏移量
        long maxPhyOffsetOfConsumeQueue = this.recoverConsumeQueue();

        if (lastExitOK) {
            //正常的恢复
            this.commitLog.recoverNormally(maxPhyOffsetOfConsumeQueue);
        } else {
            //非正常的恢复
            this.commitLog.recoverAbnormally(maxPhyOffsetOfConsumeQueue);
        }

        //恢复topicQueue表
        this.recoverTopicQueueTable();
    }

    public MessageStoreConfig getMessageStoreConfig() {
        return messageStoreConfig;
    }

    public TransientStorePool getTransientStorePool() {
        return transientStorePool;
    }

    private void putConsumeQueue(final String topic, final int queueId, final ConsumeQueue consumeQueue) {
        ConcurrentMap<Integer/* queueId */, ConsumeQueue> map = this.consumeQueueTable.get(topic);
        if (null == map) {
            map = new ConcurrentHashMap<Integer/* queueId */, ConsumeQueue>();
            map.put(queueId, consumeQueue);
            this.consumeQueueTable.put(topic, map);
        } else {
            map.put(queueId, consumeQueue);
        }
    }

    /**
     * 恢复ConsumeQueue，并获得其中最大的物理消息偏移量
     *
     * @return
     */
    private long recoverConsumeQueue() {
        long maxPhysicOffset = -1;
        for (ConcurrentMap<Integer, ConsumeQueue> maps : this.consumeQueueTable.values()) {
            for (ConsumeQueue logic : maps.values()) {
                logic.recover();
                if (logic.getMaxPhysicOffset() > maxPhysicOffset) {
                    maxPhysicOffset = logic.getMaxPhysicOffset();
                }
            }
        }

        return maxPhysicOffset;
    }

    public void recoverTopicQueueTable() {
        HashMap<String/* topic-queueid */, Long/* offset */> table = new HashMap<String, Long>(1024);
        long minPhyOffset = this.commitLog.getMinOffset();
        for (ConcurrentMap<Integer, ConsumeQueue> maps : this.consumeQueueTable.values()) {
            for (ConsumeQueue logic : maps.values()) {
                String key = logic.getTopic() + "-" + logic.getQueueId();
                table.put(key, logic.getMaxOffsetInQueue());
                logic.correctMinOffset(minPhyOffset);
            }
        }

        this.commitLog.setTopicQueueTable(table);
    }

    public AllocateMappedFileService getAllocateMappedFileService() {
        return allocateMappedFileService;
    }

    public StoreStatsService getStoreStatsService() {
        return storeStatsService;
    }

    public RunningFlags getAccessRights() {
        return runningFlags;
    }

    public ConcurrentMap<String, ConcurrentMap<Integer, ConsumeQueue>> getConsumeQueueTable() {
        return consumeQueueTable;
    }

    public StoreCheckpoint getStoreCheckpoint() {
        return storeCheckpoint;
    }

    public HAService getHaService() {
        return haService;
    }

    public ScheduleMessageService getScheduleMessageService() {
        return scheduleMessageService;
    }

    public RunningFlags getRunningFlags() {
        return runningFlags;
    }

    /**
     * 分发给下游进行处理
     * @param req
     */
    public void doDispatch(DispatchRequest req) {
        for (CommitLogDispatcher dispatcher : this.dispatcherList) {
            dispatcher.dispatch(req);
        }
    }

    /**
     * 投递消息到相关的comsumeQueue中
     * @param req 分发的请求
     */
    public void putMessagePositionInfo(DispatchRequest req) {
        //定位特定队列
        ConsumeQueue cq = this.findConsumeQueue(req.getTopic(), req.getQueueId());
        //由对应的消费队列进行操作
        cq.putMessagePositionInfoWrapper(req);
    }

    @Override
    public BrokerStatsManager getBrokerStatsManager() {
        return brokerStatsManager;
    }

    @Override
    public void handleScheduleMessageService(final BrokerRole brokerRole) {
        if (this.scheduleMessageService != null) {
            if (brokerRole == SLAVE) {
                this.scheduleMessageService.shutdown();
            } else {
                this.scheduleMessageService.start();
            }
        }

    }

    public int remainTransientStoreBufferNumbs() {
        return this.transientStorePool.remainBufferNumbs();
    }

    @Override
    public boolean isTransientStorePoolDeficient() {
        return remainTransientStoreBufferNumbs() == 0;
    }

    @Override
    public LinkedList<CommitLogDispatcher> getDispatcherList() {
        return this.dispatcherList;
    }

    @Override
    public ConsumeQueue getConsumeQueue(String topic, int queueId) {
        ConcurrentMap<Integer, ConsumeQueue> map = consumeQueueTable.get(topic);
        if (map == null) {
            return null;
        }
        return map.get(queueId);
    }

    public void unlockMappedFile(final MappedFile mappedFile) {
        this.scheduledExecutorService.schedule(new Runnable() {
            @Override
            public void run() {
                mappedFile.munlock();
            }
        }, 6, TimeUnit.SECONDS);
    }

    /**
     * 构建ConsumeQueue
     */
    class CommitLogDispatcherBuildConsumeQueue implements CommitLogDispatcher {

        @Override
        public void dispatch(DispatchRequest request) {
            final int tranType = MessageSysFlag.getTransactionValue(request.getSysFlag()); //获得事务类型
            switch (tranType) {
                case TRANSACTION_NOT_TYPE:
                case TRANSACTION_COMMIT_TYPE:
                    //进行投递
                    DefaultMessageStore.this.putMessagePositionInfo(request);
                    break;
                case TRANSACTION_PREPARED_TYPE:
                case TRANSACTION_ROLLBACK_TYPE: //准备消息,回滚消息
                    //两者不投递到消费队列中
                    break;
            }
        }
    }

    /**
     * 构建Index
     */
    class CommitLogDispatcherBuildIndex implements CommitLogDispatcher {

        @Override
        public void dispatch(DispatchRequest request) {
            if (DefaultMessageStore.this.messageStoreConfig.isMessageIndexEnable()) { //支持构建index
                DefaultMessageStore.this.indexService.buildIndex(request); //构建index索引
            }
        }
    }

    /**
     * 负责清理commitLog的过期文件，如果满足时间(每天凌晨4点)/磁盘空间不足/有人手动删除过中的一项，即可删去3天内没有修改记录的commitlog。
     */
    class CleanCommitLogService {

        private final static int MAX_MANUAL_DELETE_FILE_TIMES = 20;

        private final double diskSpaceWarningLevelRatio = Double.parseDouble(System.getProperty("rocketmq.broker.diskSpaceWarningLevelRatio", "0.90"));

        private final double diskSpaceCleanForciblyRatio = Double.parseDouble(System.getProperty("rocketmq.broker.diskSpaceCleanForciblyRatio", "0.85"));
        private long lastRedeleteTimestamp = 0;

        private volatile int manualDeleteFileSeveralTimes = 0;

        private volatile boolean cleanImmediately = false;

        public void excuteDeleteFilesManualy() {
            this.manualDeleteFileSeveralTimes = MAX_MANUAL_DELETE_FILE_TIMES;
            DefaultMessageStore.log.info("executeDeleteFilesManually was invoked");
        }

        public void run() {
            try {
                //删除过期的文件
                this.deleteExpiredFiles();

                //删除挂起的文件
                this.redeleteHangedFile();
            } catch (Throwable e) {
                DefaultMessageStore.log.warn(this.getServiceName() + " service has exception. ", e);
            }
        }

        /**
         * 删除过期的文件
         */
        private void deleteExpiredFiles() {
            int deleteCount = 0;
            //文件保留时间
            long fileReservedTime = DefaultMessageStore.this.getMessageStoreConfig().getFileReservedTime();
            //删除物理文件的间隔
            int deletePhysicFilesInterval = DefaultMessageStore.this.getMessageStoreConfig().getDeleteCommitLogFilesInterval();
            //销毁MMP
            int destroyMapedFileIntervalForcibly = DefaultMessageStore.this.getMessageStoreConfig().getDestroyMapedFileIntervalForcibly();

            //时间策略删除
            boolean timeup = this.isTimeToDelete();
            //空间策略删除
            boolean spacefull = this.isSpaceToDelete();
            //是否手动删除
            boolean manualDelete = this.manualDeleteFileSeveralTimes > 0;

            if (timeup || spacefull || manualDelete) {

                //每手动删除一次，次数减减
                if (manualDelete)
                    this.manualDeleteFileSeveralTimes--;

                boolean cleanAtOnce = DefaultMessageStore.this.getMessageStoreConfig().isCleanFileForciblyEnable() && this.cleanImmediately;

                log.info("begin to delete before {} hours file. timeup: {} spacefull: {} manualDeleteFileSeveralTimes: {} cleanAtOnce: {}",
                        fileReservedTime,
                        timeup,
                        spacefull,
                        manualDeleteFileSeveralTimes,
                        cleanAtOnce);

                fileReservedTime *= 60 * 60 * 1000;


                deleteCount = DefaultMessageStore.this.commitLog.deleteExpiredFile(fileReservedTime, deletePhysicFilesInterval,
                        destroyMapedFileIntervalForcibly, cleanAtOnce);
                if (deleteCount > 0) {
                } else if (spacefull) {
                    log.warn("disk space will be full soon, but delete file failed.");
                }
            }
        }

        /**
         * 重新删除挂起的文件
         */
        private void redeleteHangedFile() {
            //间隔
            int interval = DefaultMessageStore.this.getMessageStoreConfig().getRedeleteHangedFileInterval();
            long currentTimestamp = System.currentTimeMillis();
            if ((currentTimestamp - this.lastRedeleteTimestamp) > interval) {
                this.lastRedeleteTimestamp = currentTimestamp;
                int destroyMapedFileIntervalForcibly =
                        DefaultMessageStore.this.getMessageStoreConfig().getDestroyMapedFileIntervalForcibly();
                if (DefaultMessageStore.this.commitLog.retryDeleteFirstFile(destroyMapedFileIntervalForcibly)) {
                }
            }
        }

        public String getServiceName() {
            return CleanCommitLogService.class.getSimpleName();
        }

        /**
         * 是时间去删除
         * @return
         */
        private boolean isTimeToDelete() {
            String when = DefaultMessageStore.this.getMessageStoreConfig().getDeleteWhen();
            if (UtilAll.isItTimeToDo(when)) {
                DefaultMessageStore.log.info("it's time to reclaim disk space, " + when);
                return true;
            }

            return false;
        }

        /**
         * 空间不足删除
         * @return
         */
        private boolean isSpaceToDelete() {
            double ratio = DefaultMessageStore.this.getMessageStoreConfig().getDiskMaxUsedSpaceRatio() / 100.0;

            cleanImmediately = false;

            {
                String storePathPhysic = DefaultMessageStore.this.getMessageStoreConfig().getStorePathCommitLog();
                double physicRatio = UtilAll.getDiskPartitionSpaceUsedPercent(storePathPhysic);
                if (physicRatio > diskSpaceWarningLevelRatio) {
                    boolean diskok = DefaultMessageStore.this.runningFlags.getAndMakeDiskFull();
                    if (diskok) {
                        DefaultMessageStore.log.error("physic disk maybe full soon " + physicRatio + ", so mark disk full");
                    }

                    cleanImmediately = true;
                } else if (physicRatio > diskSpaceCleanForciblyRatio) {
                    cleanImmediately = true;
                } else {
                    boolean diskok = DefaultMessageStore.this.runningFlags.getAndMakeDiskOK();
                    if (!diskok) {
                        DefaultMessageStore.log.info("physic disk space OK " + physicRatio + ", so mark disk ok");
                    }
                }

                if (physicRatio < 0 || physicRatio > ratio) {
                    DefaultMessageStore.log.info("physic disk maybe full soon, so reclaim space, " + physicRatio);
                    return true;
                }
            }

            {
                String storePathLogics = getStorePathConsumeQueue(DefaultMessageStore.this.getMessageStoreConfig().getStorePathRootDir());
                double logicsRatio = UtilAll.getDiskPartitionSpaceUsedPercent(storePathLogics);
                if (logicsRatio > diskSpaceWarningLevelRatio) {
                    boolean diskok = DefaultMessageStore.this.runningFlags.getAndMakeDiskFull();
                    if (diskok) {
                        DefaultMessageStore.log.error("logics disk maybe full soon " + logicsRatio + ", so mark disk full");
                    }

                    cleanImmediately = true;
                } else if (logicsRatio > diskSpaceCleanForciblyRatio) {
                    cleanImmediately = true;
                } else {
                    boolean diskok = DefaultMessageStore.this.runningFlags.getAndMakeDiskOK();
                    if (!diskok) {
                        DefaultMessageStore.log.info("logics disk space OK " + logicsRatio + ", so mark disk ok");
                    }
                }

                if (logicsRatio < 0 || logicsRatio > ratio) {
                    DefaultMessageStore.log.info("logics disk maybe full soon, so reclaim space, " + logicsRatio);
                    return true;
                }
            }

            return false;
        }

        public int getManualDeleteFileSeveralTimes() {
            return manualDeleteFileSeveralTimes;
        }

        public void setManualDeleteFileSeveralTimes(int manualDeleteFileSeveralTimes) {
            this.manualDeleteFileSeveralTimes = manualDeleteFileSeveralTimes;
        }
    }

    /**
     * 清理consumequeue的过期文件，触发条件比较苛刻，一个consumequeue文件最多可以存储30w个消息位置信息，检查最后一个消息的offset(即这个文件中最新的消息)是否小于commitlog的最小offset，如果是则删除。即如果某个文件的消息不满30w，肯定不会被删除。
     */
    class CleanConsumeQueueService {
        private long lastPhysicalMinOffset = 0;

        public void run() {
            try {
                this.deleteExpiredFiles();
            } catch (Throwable e) {
                DefaultMessageStore.log.warn("CleanConsumeQueueService service has exception. ", e);
            }
        }

        /**
         * 清理过期的文件
         */
        private void deleteExpiredFiles() {
            //删除逻辑文件的间隔
            int deleteLogicsFilesInterval = DefaultMessageStore.this.getMessageStoreConfig().getDeleteConsumeQueueFilesInterval();

            //最小的offset
            long minOffset = DefaultMessageStore.this.commitLog.getMinOffset();
            //最小的offset已经大于这个最近的物理的offset
            if (minOffset > this.lastPhysicalMinOffset) {
                this.lastPhysicalMinOffset = minOffset;

                //获得consumeQueueTable
                ConcurrentMap<String, ConcurrentMap<Integer, ConsumeQueue>> tables = DefaultMessageStore.this.consumeQueueTable;

                //遍历
                for (ConcurrentMap<Integer, ConsumeQueue> maps : tables.values()) {
                    for (ConsumeQueue logic : maps.values()) {
                        //删除文件
                        int deleteCount = logic.deleteExpiredFile(minOffset);

                        //
                        if (deleteCount > 0 && deleteLogicsFilesInterval > 0) {
                            try {
                                Thread.sleep(deleteLogicsFilesInterval);
                            } catch (InterruptedException ignored) {
                            }
                        }
                    }
                }

                //删除索引文件
                DefaultMessageStore.this.indexService.deleteExpiredFile(minOffset);
            }
        }

        public String getServiceName() {
            return CleanConsumeQueueService.class.getSimpleName();
        }
    }

    /**
     * 定期将consumeQueue刷盘
     */
    class FlushConsumeQueueService extends ServiceThread {
        private static final int RETRY_TIMES_OVER = 3;
        private long lastFlushTimestamp = 0;

        private void doFlush(int retryTimes) {
            //刷页的次数
            int flushConsumeQueueLeastPages = DefaultMessageStore.this.getMessageStoreConfig().getFlushConsumeQueueLeastPages();

            if (retryTimes == RETRY_TIMES_OVER) {
                flushConsumeQueueLeastPages = 0;
            }

            long logicsMsgTimestamp = 0;

            int flushConsumeQueueThoroughInterval = DefaultMessageStore.this.getMessageStoreConfig().getFlushConsumeQueueThoroughInterval();
            long currentTimeMillis = System.currentTimeMillis();
            if (currentTimeMillis >= (this.lastFlushTimestamp + flushConsumeQueueThoroughInterval)) {
                this.lastFlushTimestamp = currentTimeMillis;
                flushConsumeQueueLeastPages = 0;
                logicsMsgTimestamp = DefaultMessageStore.this.getStoreCheckpoint().getLogicsMsgTimestamp();
            }

            ConcurrentMap<String, ConcurrentMap<Integer, ConsumeQueue>> tables = DefaultMessageStore.this.consumeQueueTable;

            for (ConcurrentMap<Integer, ConsumeQueue> maps : tables.values()) {
                for (ConsumeQueue cq : maps.values()) {
                    boolean result = false;
                    for (int i = 0; i < retryTimes && !result; i++) {
                        result = cq.flush(flushConsumeQueueLeastPages);
                    }
                }
            }

            if (0 == flushConsumeQueueLeastPages) {
                if (logicsMsgTimestamp > 0) {
                    DefaultMessageStore.this.getStoreCheckpoint().setLogicsMsgTimestamp(logicsMsgTimestamp);
                }
                DefaultMessageStore.this.getStoreCheckpoint().flush();
            }
        }

        /**
         * 线程执行，FlushConsumeQueueService服务运行
         */
        public void run() {
            DefaultMessageStore.log.info("FlushConsumeQueueService service started");

            while (!this.isStopped()) {
                try {
                    int interval = DefaultMessageStore.this.getMessageStoreConfig().getFlushIntervalConsumeQueue();
                    this.waitForRunning(interval);
                    this.doFlush(1);
                } catch (Exception e) {
                    DefaultMessageStore.log.warn("FlushConsumeQueueService service has exception. ", e);
                }
            }

            this.doFlush(RETRY_TIMES_OVER);

            DefaultMessageStore.log.info("FlushConsumeQueueService service end");
        }

        @Override
        public String getServiceName() {
            return FlushConsumeQueueService.class.getSimpleName();
        }

        @Override
        public long getJointime() {
            return 1000 * 60;
        }
    }

    /**
     * 每1ms将commitlog中的变化量写入consumerqueue
     * 重投递，将producer投递给commitlog的消息投递写入对应的consumequeue中
     */
    class ReputMessageService extends ServiceThread {

        private volatile long reputFromOffset = 0;

        public long getReputFromOffset() {
            return reputFromOffset;
        }

        public void setReputFromOffset(long reputFromOffset) {
            this.reputFromOffset = reputFromOffset;
        }

        @Override
        public void shutdown() {
            for (int i = 0; i < 50 && this.isCommitLogAvailable(); i++) {
                try {
                    Thread.sleep(100);
                } catch (InterruptedException ignored) {
                }
            }

            if (this.isCommitLogAvailable()) {
                log.warn("shutdown ReputMessageService, but commitlog have not finish to be dispatched, CL: {} reputFromOffset: {}",
                        DefaultMessageStore.this.commitLog.getMaxOffset(), this.reputFromOffset);
            }

            super.shutdown();
        }

        /**
         * 重投递消息落后多少偏移量
         * @return
         */
        public long behind() {
            return DefaultMessageStore.this.commitLog.getMaxOffset() - this.reputFromOffset;
        }

        /**
         * 可用，只要当前的重投递的偏移量落后commitlog就认为是可用的
         * @return
         */
        private boolean isCommitLogAvailable() {
            return this.reputFromOffset < DefaultMessageStore.this.commitLog.getMaxOffset();
        }

        /**
         * 投递
         */
        private void doReput() {
            CommitLog commitLog = DefaultMessageStore.this.commitLog;
            //最小的偏移量
            long minOffset = commitLog.getMinOffset();
            // 检查状态，投递的偏移量小于存储引擎的最小的偏移量，这通常表明调度过多而且commitlog已经过期
            if (this.reputFromOffset < minOffset) {
                log.warn("The reputFromOffset={} is smaller than minPyOffset={}, this usually indicate that the dispatch behind too much and the commitlog has expired.", this.reputFromOffset, minOffset);
                this.reputFromOffset = minOffset;
            }

            //
            for (boolean doNext = true; this.isCommitLogAvailable() && doNext; ) {

                if (DefaultMessageStore.this.getMessageStoreConfig().isDuplicationEnable() && this.reputFromOffset >= DefaultMessageStore.this.getConfirmOffset()) {
                    break;
                }
                /**
                 * 获得映射
                 */
                SelectMappedBufferResult result = commitLog.getData(reputFromOffset);
                if (result == null) {
                    break;
                }
                try {
                    //更新
                    this.reputFromOffset = result.getStartOffset();

                    for (int readSize = 0; readSize < result.getSize() && doNext; ) {
                        //分发的请求
                        DispatchRequest req = commitLog.checkMessageAndReturnSize(result.getByteBuffer(), false, false);
                        String topic = req.getTopic();
                        int size = req.getBufferSize() == -1 ? req.getMsgSize() : req.getBufferSize();

                        if (req.isSuccess()) {
                            if (size > 0) {
                                //分发请求
                                DefaultMessageStore.this.doDispatch(req);

                                //不是slave节点，并支持长pool
                                if (SLAVE != DefaultMessageStore.this.getMessageStoreConfig().getBrokerRole() && DefaultMessageStore.this.brokerConfig.isLongPollingEnable()) {
                                    //触发监听到达的消息
                                    DefaultMessageStore.this.messageArrivingListener.arriving(
                                            topic,
                                            req.getQueueId(),
                                            req.getConsumeQueueOffset() + 1,
                                            req.getTagsCode(),
                                            req.getStoreTimestamp(),
                                            req.getBitMap(),
                                            req.getPropertiesMap());
                                }
                                //更新
                                this.reputFromOffset += size;
                                readSize += size;
                                //状态
                                if (DefaultMessageStore.this.getMessageStoreConfig().getBrokerRole() == SLAVE) {
                                    DefaultMessageStore.this.storeStatsService.getSinglePutMessageTopicTimesTotal(topic).incrementAndGet();
                                    DefaultMessageStore.this.storeStatsService.getSinglePutMessageTopicSizeTotal(topic).addAndGet(req.getMsgSize());
                                }
                            } else if (size == 0) {
                                this.reputFromOffset = commitLog.rollNextFile(this.reputFromOffset);
                                readSize = result.getSize();
                            }
                        } else if (!req.isSuccess()) {
                            if (size > 0) {
                                log.error("[BUG]read total count not equals msg total size. reputFromOffset={}", reputFromOffset);
                                this.reputFromOffset += size;
                            } else {
                                doNext = false;
                                log.error("[BUG]dispatch message to consume queue error, COMMITLOG OFFSET: {}",
                                        this.reputFromOffset);
                                this.reputFromOffset += result.getSize() - readSize;
                            }
                        }
                    }
                } finally {
                    result.release();
                }
            }
        }

        /**
         * 一直刷新分发相关的信息
         */
        @Override
        public void run() {
            String name = getServiceName();
            DefaultMessageStore.log.info(name + " service started");

            while (!this.isStopped()) {
                try {
                    Thread.sleep(1);
                    this.doReput();
                } catch (Exception e) {
                    DefaultMessageStore.log.warn(name + " service has exception. ", e);
                }
            }

            DefaultMessageStore.log.info(name + " service end");
        }

        @Override
        public String getServiceName() {
            return ReputMessageService.class.getSimpleName();
        }

    }
}
