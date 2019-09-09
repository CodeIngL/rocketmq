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
package org.apache.rocketmq.store.index;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.sysflag.MessageSysFlag;
import org.apache.rocketmq.store.DefaultMessageStore;
import org.apache.rocketmq.store.DispatchRequest;
import org.apache.rocketmq.store.config.StorePathConfigHelper;

import static org.apache.rocketmq.common.UtilAll.timeMillisToHumanString;
import static org.apache.rocketmq.common.sysflag.MessageSysFlag.*;

/**
 * 索引服务，提供了使用key或者时间区段来查询消息的方法
 */
public class IndexService {

    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);
    /**
     * Maximum times to attempt index file creation.
     * <p>
     *     尝试创建索引文件的最长时间。
     * </p>
     */
    private static final int MAX_TRY_IDX_CREATE = 3;
    private final DefaultMessageStore defaultMessageStore;
    //hash槽数量
    private final int hashSlotNum;
    //位置数量，2千万
    private final int indexNum;
    private final String storePath;
    //相关的索引文件列表
    private final ArrayList<IndexFile> indexFileList = new ArrayList<IndexFile>();
    private final ReadWriteLock readWriteLock = new ReentrantReadWriteLock();

    public IndexService(final DefaultMessageStore store) {
        this.defaultMessageStore = store;
        this.hashSlotNum = store.getMessageStoreConfig().getMaxHashSlotNum();
        this.indexNum = store.getMessageStoreConfig().getMaxIndexNum();
        this.storePath = StorePathConfigHelper.getStorePathIndex(store.getMessageStoreConfig().getStorePathRootDir());
    }

    /**
     * 根据上一次是否正常的退出来加载索引文件，提供索引的服务
     * @param lastExitOK false 代表宕机这种方式
     * @return
     */
    public boolean load(final boolean lastExitOK) {
        //索引目录
        File dir = new File(this.storePath);
        //
        File[] files = dir.listFiles();
        if (files != null) {
            // ascending order
            Arrays.sort(files);
            for (File file : files) {
                try {
                    //构建文件对应
                    IndexFile f = new IndexFile(file.getPath(), this.hashSlotNum, this.indexNum, 0, 0);
                    f.load();

                    if (!lastExitOK) {
                        //结束时间大于存储校验点的时间，销毁这个文件
                        if (f.getEndTimestamp() > this.defaultMessageStore.getStoreCheckpoint().getIndexMsgTimestamp()) {
                            f.destroy(0);
                            continue;
                        }
                    }

                    log.info("load index file OK, " + f.getFileName());
                    this.indexFileList.add(f);
                } catch (IOException e) {
                    log.error("load file {} error", file, e);
                    return false;
                } catch (NumberFormatException e) {
                    log.error("load file {} error", file, e);
                }
            }
        }

        return true;
    }

    public void deleteExpiredFile(long offset) {
        Object[] files = null;
        try {
            this.readWriteLock.readLock().lock();
            if (this.indexFileList.isEmpty()) {
                return;
            }

            //如果第一个最后
            long endPhyOffset = this.indexFileList.get(0).getEndPhyOffset();
            if (endPhyOffset < offset) {
                files = this.indexFileList.toArray();
            }
        } catch (Exception e) {
            log.error("destroy exception", e);
        } finally {
            this.readWriteLock.readLock().unlock();
        }

        if (files != null) { //存在
            List<IndexFile> fileList = new ArrayList<IndexFile>();
            for (int i = 0; i < (files.length - 1); i++) {
                IndexFile f = (IndexFile) files[i];
                if (f.getEndPhyOffset() < offset) {
                    fileList.add(f);
                } else {
                    break;
                }
            }

            this.deleteExpiredFile(fileList);
        }
    }

    /**
     * 删除相关的索引文件
     * @param files
     */
    private void deleteExpiredFile(List<IndexFile> files) {
        if (!files.isEmpty()) {
            try {
                this.readWriteLock.writeLock().lock();
                for (IndexFile file : files) {
                    boolean destroyed = file.destroy(3000);
                    destroyed = destroyed && this.indexFileList.remove(file);
                    if (!destroyed) {
                        log.error("deleteExpiredFile remove failed.");
                        break;
                    }
                }
            } catch (Exception e) {
                log.error("deleteExpiredFile has exception.", e);
            } finally {
                this.readWriteLock.writeLock().unlock();
            }
        }
    }

    public void destroy() {
        try {
            this.readWriteLock.writeLock().lock();
            for (IndexFile f : this.indexFileList) {
                f.destroy(1000 * 3);
            }
            this.indexFileList.clear();
        } catch (Exception e) {
            log.error("destroy exception", e);
        } finally {
            this.readWriteLock.writeLock().unlock();
        }
    }

    /**
     * 使用key进行查询
     * @param topic
     * @param key
     * @param maxNum
     * @param begin
     * @param end
     * @return
     */
    public QueryOffsetResult queryOffset(String topic, String key, int maxNum, long begin, long end) {
        List<Long> phyOffsets = new ArrayList<>(maxNum);

        long indexLastUpdateTimestamp = 0;
        long indexLastUpdatePhyoffset = 0;
        maxNum = Math.min(maxNum, this.defaultMessageStore.getMessageStoreConfig().getMaxMsgsNumBatch()); //一次查询最大量
        try {
            this.readWriteLock.readLock().lock();
            if (!this.indexFileList.isEmpty()) {
                for (int i = this.indexFileList.size(); i > 0; i--) {
                    IndexFile f = this.indexFileList.get(i - 1);
                    boolean lastFile = i == this.indexFileList.size();
                    if (lastFile) {
                        indexLastUpdateTimestamp = f.getEndTimestamp();
                        indexLastUpdatePhyoffset = f.getEndPhyOffset();
                    }

                    if (f.isTimeMatched(begin, end)) { //消息时间匹配
                        f.selectPhyOffset(phyOffsets, buildKey(topic, key), maxNum, begin, end, lastFile);
                    }

                    if (f.getBeginTimestamp() < begin) {
                        break;
                    }

                    if (phyOffsets.size() >= maxNum) {
                        break;
                    }
                }
            }
        } catch (Exception e) {
            log.error("queryMsg exception", e);
        } finally {
            this.readWriteLock.readLock().unlock();
        }

        return new QueryOffsetResult(phyOffsets, indexLastUpdateTimestamp, indexLastUpdatePhyoffset);
    }

    private String buildKey(final String topic, final String key) {
        return topic + "#" + key;
    }

    /**
     * 为消息构建index索引
     * @param req
     */
    public void buildIndex(DispatchRequest req) {
        //获得对应indexFile映射文件
        IndexFile indexFile = retryGetAndCreateIndexFile();
        if (indexFile == null){
            log.error("build index error, stop building index");
            return;
        }
        //结束的物理位置
        long endPhyOffset = indexFile.getEndPhyOffset();
        //分发的请求
        DispatchRequest msg = req;
        String topic = msg.getTopic();//topic
        String keys = msg.getKeys(); //keys
        if (msg.getCommitLogOffset() < endPhyOffset) { //非法的状态，我们直接返回
            return;
        }

        //事务类型
        final int tranType = MessageSysFlag.getTransactionValue(msg.getSysFlag());
        switch (tranType) {
            case TRANSACTION_NOT_TYPE:
            case TRANSACTION_PREPARED_TYPE:
            case TRANSACTION_COMMIT_TYPE:
                break;
            case TRANSACTION_ROLLBACK_TYPE: //rollback消息直接返回，不构建index索引，其他都会构建
                return;
        }

        //存在唯一键，key
        if (req.getUniqKey() != null) {
            //放置key
            indexFile = putKey(indexFile, msg, buildKey(topic, req.getUniqKey()));
            if (indexFile == null) {
                log.error("putKey error commitlog {} uniqkey {}", req.getCommitLogOffset(), req.getUniqKey());
                return;
            }
        }
        if (keys == null || keys.length() == 0){
            return;
        }

        //很多其他key，每一个key进行放置
        for (String key : keys.split(MessageConst.KEY_SEPARATOR)) {
            if (key.length() == 0){
                continue;
            }
            indexFile = putKey(indexFile, msg, buildKey(topic, key));
            if (indexFile == null) {
                log.error("putKey error commitlog {} uniqkey {} normal keys {} error key {}", req.getCommitLogOffset(), req.getUniqKey(), keys, key);
                return;
            }
        }
    }

    /**
     * 提供了key放置，将索引信息写入index文件中，提供基于key的查找
     * @param indexFile
     * @param msg
     * @param idxKey
     * @return
     */
    private IndexFile putKey(IndexFile indexFile, DispatchRequest msg, String idxKey) {
        long offset = msg.getCommitLogOffset(); //消息offset
        long timestamp = msg.getStoreTimestamp(); //消息存储时间
        for (boolean ok = indexFile.putKey(idxKey, offset, timestamp); !ok; ) {
            log.warn("Index file [" + indexFile.getFileName() + "] is full, trying to create another one");
            indexFile = retryGetAndCreateIndexFile();
            if (null == indexFile) {
                return null;
            }
            ok = indexFile.putKey(idxKey, offset, timestamp);
        }

        return indexFile;
    }

    /**
     * Retries to get or create index file.
     * <p>
     *     重试获取或创建索引文件。
     * </p>
     *
     * @return {@link IndexFile} or null on failure.
     */
    public IndexFile retryGetAndCreateIndexFile() {
        IndexFile indexFile = null;

        for (int times = 0; null == indexFile && times < MAX_TRY_IDX_CREATE; times++) {
            indexFile = this.getAndCreateLastIndexFile();
            if (null != indexFile)
                break;

            try {
                log.info("Tried to create index file " + times + " times");
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                log.error("Interrupted", e);
            }
        }

        if (null == indexFile) {
            this.defaultMessageStore.getAccessRights().makeIndexFileError();
            log.error("Mark index file cannot build flag");
        }

        return indexFile;
    }

    /**
     * 获得或者新创建索引
     * @return
     */
    public IndexFile getAndCreateLastIndexFile() {
        IndexFile indexFile = null;
        IndexFile prevIndexFile = null;
        long lastUpdateEndPhyOffset = 0;
        long lastUpdateIndexTimestamp = 0;

        {
            this.readWriteLock.readLock().lock();
            //获得最后一个索引文件，如果他是满的，我们尝试创建新文件
            if (!this.indexFileList.isEmpty()) {
                IndexFile tmp = this.indexFileList.get(this.indexFileList.size() - 1);
                if (!tmp.isWriteFull()) {
                    indexFile = tmp;
                } else {
                    lastUpdateEndPhyOffset = tmp.getEndPhyOffset();
                    lastUpdateIndexTimestamp = tmp.getEndTimestamp();
                    prevIndexFile = tmp;
                }
            }

            this.readWriteLock.readLock().unlock();
        }

        //满的，我们尝试创造新文件
        if (indexFile == null) {
            try {
                String fileName = this.storePath + File.separator + timeMillisToHumanString(System.currentTimeMillis());
                indexFile = new IndexFile(fileName, this.hashSlotNum, this.indexNum, lastUpdateEndPhyOffset, lastUpdateIndexTimestamp);
                this.readWriteLock.writeLock().lock();
                this.indexFileList.add(indexFile);
            } catch (Exception e) {
                log.error("getLastIndexFile exception ", e);
            } finally {
                this.readWriteLock.writeLock().unlock();
            }

            if (indexFile != null) {
                final IndexFile flushThisFile = prevIndexFile;
                Thread flushThread = new Thread(() -> IndexService.this.flush(flushThisFile), "FlushIndexFileThread");
                flushThread.setDaemon(true);
                flushThread.start();
            }
        }

        return indexFile;
    }

    /**
     * 刷新磁盘服务
     * @param f
     */
    public void flush(final IndexFile f) {
        if (null == f)
            return;

        long indexMsgTimestamp = 0;

        if (f.isWriteFull()) {
            indexMsgTimestamp = f.getEndTimestamp();
        }

        f.flush();

        if (indexMsgTimestamp > 0) {
            this.defaultMessageStore.getStoreCheckpoint().setIndexMsgTimestamp(indexMsgTimestamp);
            this.defaultMessageStore.getStoreCheckpoint().flush();
        }
    }

    public void start() {

    }

    public void shutdown() {

    }
}
