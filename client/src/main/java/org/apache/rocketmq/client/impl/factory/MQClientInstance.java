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
package org.apache.rocketmq.client.impl.factory;

import java.io.UnsupportedEncodingException;
import java.net.DatagramSocket;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.rocketmq.client.ClientConfig;
import org.apache.rocketmq.client.admin.MQAdminExtInner;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.impl.ClientRemotingProcessor;
import org.apache.rocketmq.client.impl.FindBrokerResult;
import org.apache.rocketmq.client.impl.MQAdminImpl;
import org.apache.rocketmq.client.impl.MQClientAPIImpl;
import org.apache.rocketmq.client.impl.MQClientManager;
import org.apache.rocketmq.client.impl.consumer.DefaultMQPullConsumerImpl;
import org.apache.rocketmq.client.impl.consumer.DefaultMQPushConsumerImpl;
import org.apache.rocketmq.client.impl.consumer.MQConsumerInner;
import org.apache.rocketmq.client.impl.consumer.ProcessQueue;
import org.apache.rocketmq.client.impl.consumer.PullMessageService;
import org.apache.rocketmq.client.impl.consumer.RebalanceService;
import org.apache.rocketmq.client.impl.producer.DefaultMQProducerImpl;
import org.apache.rocketmq.client.impl.producer.MQProducerInner;
import org.apache.rocketmq.client.impl.producer.TopicPublishInfo;
import org.apache.rocketmq.client.log.ClientLogger;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.stat.ConsumerStatsManager;
import org.apache.rocketmq.common.MQVersion;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.ServiceState;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.constant.PermName;
import org.apache.rocketmq.common.filter.ExpressionType;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.protocol.body.ConsumeMessageDirectlyResult;
import org.apache.rocketmq.common.protocol.body.ConsumerRunningInfo;
import org.apache.rocketmq.common.protocol.heartbeat.ConsumerData;
import org.apache.rocketmq.common.protocol.heartbeat.HeartbeatData;
import org.apache.rocketmq.common.protocol.heartbeat.ProducerData;
import org.apache.rocketmq.common.protocol.heartbeat.SubscriptionData;
import org.apache.rocketmq.common.protocol.route.BrokerData;
import org.apache.rocketmq.common.protocol.route.QueueData;
import org.apache.rocketmq.common.protocol.route.TopicRouteData;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.apache.rocketmq.remoting.netty.NettyClientConfig;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;

import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;
import static org.apache.rocketmq.common.protocol.heartbeat.ConsumeType.CONSUME_PASSIVELY;
import static org.apache.rocketmq.remoting.common.RemotingHelper.exceptionSimpleDesc;

/**
 * 代表了网络端点中的mqbroker对应客户端，因此这个广泛存在于mq消费端，mq生产端
 * 因此这里主要包括了producer，consumer，还有一个我们以管理者的身份操作，也就是admin
 */
public class MQClientInstance {

    //超时锁3000
    private final static long LOCK_TIMEOUT_MILLIS = 3000;

    private final InternalLogger log = ClientLogger.getLog();
    //客户端的配置
    private final ClientConfig clientConfig;
    //实例内存索引
    private final int instanceIndex;
    //实例指定的id，客户端Id,发送心跳的时候，这些数据标识了一个客户端
    private final String clientId;
    //启动时间
    private final long bootTimestamp = System.currentTimeMillis();
    /**
     * 组名和生产者
     */
    private final ConcurrentMap<String/* group */, MQProducerInner> producerTable = new ConcurrentHashMap<>();
    /**
     * 组名和消费端
     */
    private final ConcurrentMap<String/* group */, MQConsumerInner> consumerTable = new ConcurrentHashMap<>();
    /**
     * 组名和管理端，admin使用特殊组名也就是admin_ext_group
     */
    private final ConcurrentMap<String/* group */, MQAdminExtInner> adminExtTable = new ConcurrentHashMap<>();

    private final NettyClientConfig nettyClientConfig;


    private final MQClientAPIImpl mQClientAPIImpl;
    private final MQAdminImpl mQAdminImpl;
    //topic和相关topic路由信息的映射
    private final ConcurrentMap<String/* Topic */, TopicRouteData> topicRouteTable = new ConcurrentHashMap<String, TopicRouteData>();
    //nameServer锁，当多次访问nameServer，我们使用同步
    private final Lock lockNamesrv = new ReentrantLock();
    //心跳锁，当多个心跳时候，我们使用同步
    private final Lock lockHeartbeat = new ReentrantLock();


    //brokerName和其的映射
    private final ConcurrentMap<String/* Broker Name */, HashMap<Long/* brokerId */, String/* address */>> brokerAddrTable = new ConcurrentHashMap<String, HashMap<Long, String>>();
    private final ConcurrentMap<String/* Broker Name */, HashMap<String/* address */, Integer>> brokerVersionTable = new ConcurrentHashMap<String, HashMap<String, Integer>>();

    private final ScheduledExecutorService scheduledExecutorService = newSingleThreadScheduledExecutor(r -> new Thread(r, "MQClientFactoryScheduledThread"));

    //处理broker对客户端的请求
    private final ClientRemotingProcessor clientRemotingProcessor;
    //拉取消息服务
    private final PullMessageService pullMessageService;
    //重新负载均衡作用
    private final RebalanceService rebalanceService;
    //发送者
    private final DefaultMQProducer defaultMQProducer;
    //消费状态管理器
    private final ConsumerStatsManager consumerStatsManager;
    //总共发生心跳的次数
    private final AtomicLong sendHeartbeatTimesTotal = new AtomicLong(0);
    //当前整个服务的状态
    private ServiceState serviceState = ServiceState.CREATE_JUST;
    //随机数
    private Random random = new Random();

    //构造器
    public MQClientInstance(ClientConfig clientConfig, int instanceIndex, String clientId) {
        this(clientConfig, instanceIndex, clientId, null);
    }

    //构造器
    public MQClientInstance(ClientConfig clientConfig, int instanceIndex, String clientId, RPCHook rpcHook) {
        this.clientConfig = clientConfig;
        this.instanceIndex = instanceIndex;
        this.nettyClientConfig = new NettyClientConfig();
        this.nettyClientConfig.setClientCallbackExecutorThreads(clientConfig.getClientCallbackExecutorThreads());
        this.nettyClientConfig.setUseTLS(clientConfig.isUseTLS());
        this.clientRemotingProcessor = new ClientRemotingProcessor(this);
        this.mQClientAPIImpl = new MQClientAPIImpl(this.nettyClientConfig, this.clientRemotingProcessor, rpcHook, clientConfig);

        if (this.clientConfig.getNamesrvAddr() != null) {
            //指定使用的nameServer地址，我们使用nameServer地址
            this.mQClientAPIImpl.updateNameServerAddressList(this.clientConfig.getNamesrvAddr());
            log.info("user specified name server address: {}", this.clientConfig.getNamesrvAddr());
        }

        this.clientId = clientId;

        this.mQAdminImpl = new MQAdminImpl(this);

        this.pullMessageService = new PullMessageService(this);

        this.rebalanceService = new RebalanceService(this);

        this.defaultMQProducer = new DefaultMQProducer(MixAll.CLIENT_INNER_PRODUCER_GROUP);
        this.defaultMQProducer.resetClientConfig(clientConfig);

        this.consumerStatsManager = new ConsumerStatsManager(this.scheduledExecutorService);

        log.info("Created a new client Instance, InstanceIndex:{}, ClientID:{}, ClientConfig:{}, ClientVersion:{}, SerializerType:{}",
                this.instanceIndex, this.clientId, this.clientConfig, MQVersion.getVersionDesc(MQVersion.CURRENT_VERSION), RemotingCommand.getSerializeTypeConfigInThisServer());
    }

    /**
     * 信息转换，topic路由信息转换为topic发布信息
     *
     * @param topic
     * @param route
     * @return
     */
    public static TopicPublishInfo topicRouteData2TopicPublishInfo(final String topic, final TopicRouteData route) {
        TopicPublishInfo info = new TopicPublishInfo();
        info.setTopicRouteData(route);  //设置元数据

        String orderTopicConf = route.getOrderTopicConf();
        if (orderTopicConf != null && orderTopicConf.length() > 0) {
            //解析顺序配置，当且存在顺序相关的配置的时候
            //xxx:9;xxxxx:10
            String[] brokers = orderTopicConf.split(";"); //相关的broker
            for (String broker : brokers) {
                String[] item = broker.split(":"); //该broker上的项
                int nums = Integer.parseInt(item[1]);
                for (int i = 0; i < nums; i++) {
                    //有序发送
                    MessageQueue mq = new MessageQueue(topic, item[0], i); //构建项
                    info.getMessageQueueList().add(mq);
                }
            }
            info.setOrderTopic(true); //设置这是个有序的topic
        } else {
            //直接使用queueData,当且没有使用顺序配置
            List<QueueData> qds = route.getQueueDatas();
            Collections.sort(qds); //排序
            for (QueueData qd : qds) {
                if (!PermName.isWriteable(qd.getPerm())) {
                    continue;
                }
                //该队列可写，可以接受消息发送
                BrokerData brokerData = null; //查找含有这个队列的broker
                for (BrokerData bd : route.getBrokerDatas()) {
                    if (bd.getBrokerName().equals(qd.getBrokerName())) {
                        brokerData = bd;
                        break;
                    }
                }

                if (null == brokerData) {
                    continue;
                }

                if (!brokerData.getBrokerAddrs().containsKey(MixAll.MASTER_ID)) { //不是master是非法的
                    continue;
                }

                for (int i = 0; i < qd.getWriteQueueNums(); i++) { //构建消息队列
                    MessageQueue mq = new MessageQueue(topic, qd.getBrokerName(), i);
                    info.getMessageQueueList().add(mq);
                }
            }
            info.setOrderTopic(false);
        }
        return info;
    }

    /**
     * 信息转换，topic路由信息转换topic订阅信息
     *
     * @param topic
     * @param route
     * @return
     */
    public static Set<MessageQueue> topicRouteData2TopicSubscribeInfo(final String topic, final TopicRouteData route) {
        Set<MessageQueue> mqList = new HashSet<>();
        List<QueueData> qds = route.getQueueDatas();
        for (QueueData qd : qds) {
            if (PermName.isReadable(qd.getPerm())) {
                for (int i = 0; i < qd.getReadQueueNums(); i++) {
                    MessageQueue mq = new MessageQueue(topic, qd.getBrokerName(), i);
                    mqList.add(mq);
                }
            }
        } //构建消息队列
        return mqList;
    }

    /**
     * 启动网络客户端
     *
     * @throws MQClientException
     */
    public void start() throws MQClientException {

        synchronized (this) {
            switch (this.serviceState) {
                case CREATE_JUST:
                    this.serviceState = ServiceState.START_FAILED;
                    // If not specified,looking address from name server
                    if (null == this.clientConfig.getNamesrvAddr()) {
                        //没有指定地址我们尝试使用中间服务抓取相关的地址
                        this.mQClientAPIImpl.fetchNameServerAddr();
                    }
                    // Start request-response channel
                    // 启动请求 - 响应通道
                    this.mQClientAPIImpl.start();
                    // Start various schedule tasks
                    // 启动各种计划任务
                    this.startScheduledTask();
                    // Start pull service
                    // 开始拉服务
                    this.pullMessageService.start();
                    // Start rebalance service
                    // 启动重新平衡服务
                    this.rebalanceService.start();
                    // Start push service
                    // 开始推送服务
                    this.defaultMQProducer.getDefaultMQProducerImpl().start(false);
                    log.info("the client factory [{}] start OK", this.clientId);
                    this.serviceState = ServiceState.RUNNING;
                    break;
                case RUNNING:
                    break;
                case SHUTDOWN_ALREADY:
                    break;
                case START_FAILED:
                    throw new MQClientException("The Factory object[" + this.getClientId() + "] has been created before, and failed.", null);
                default:
                    break;
            }
        }
    }

    /**
     * 执行调度任务
     */
    private void startScheduledTask() {
        if (null == this.clientConfig.getNamesrvAddr()) { //如果没有配置，我们尝试获取相关name服务地址
            this.scheduledExecutorService.scheduleAtFixedRate(() -> {
                try {
                    MQClientInstance.this.mQClientAPIImpl.fetchNameServerAddr();
                } catch (Exception e) {
                    log.error("ScheduledTask fetchNameServerAddr exception", e);
                }
            }, 1000 * 10, 1000 * 60 * 2, TimeUnit.MILLISECONDS); //2分钟执行一次
        }

        /**
         * 调度服务，用于调度更新相关的路由表，信息来自nameServer，默认30s进行拉取一次,更新相关的变动
         * notice非常重要的一个类，提供了重新拉取核心的路由信息的调度服务
         */
        this.scheduledExecutorService.scheduleAtFixedRate(() -> {
            try {
                MQClientInstance.this.updateTopicRouteInfoFromNameServer();
            } catch (Exception e) {
                log.error("ScheduledTask updateTopicRouteInfoFromNameServer exception", e);
            }
        }, 10, this.clientConfig.getPollNameServerInterval(), TimeUnit.MILLISECONDS); //30000

        /**
         * 调度服务，心跳服务，默认30s发送一次
         * 非常核心的服务，清理宕机掉的broker节点，并发送相关的数据到所有存活的broker上，
         * 宕机的broker通过感知路由来发现，也就是通过nameServer上的信息来发现，所以部分依赖上面的一个调度服务
         */
        this.scheduledExecutorService.scheduleAtFixedRate(() -> {
            try {
                //清理已经下线的broker
                MQClientInstance.this.cleanOfflineBroker();
                //发送心跳的信息
                MQClientInstance.this.sendHeartbeatToAllBrokerWithLock();
            } catch (Exception e) {
                log.error("ScheduledTask sendHeartbeatToAllBroker exception", e);
            }
        }, 1000, this.clientConfig.getHeartbeatBrokerInterval(), TimeUnit.MILLISECONDS); //30000

        /**
         * 调度服务，定时将消费消息游标持久化到本地
         */
        this.scheduledExecutorService.scheduleAtFixedRate(() -> {
            try {
                //持久化所有的offset
                MQClientInstance.this.persistAllConsumerOffset();
            } catch (Exception e) {
                log.error("ScheduledTask persistAllConsumerOffset exception", e);
            }
        }, 1000 * 10, this.clientConfig.getPersistConsumerOffsetInterval(), TimeUnit.MILLISECONDS);

        /**
         * 调度服务
         */
        this.scheduledExecutorService.scheduleAtFixedRate(() -> {
            try {
                //调整线程池大小
                MQClientInstance.this.adjustThreadPool();
            } catch (Exception e) {
                log.error("ScheduledTask adjustThreadPool exception", e);
            }
        }, 1, 1, TimeUnit.MINUTES);
    }


    public String getClientId() {
        return clientId;
    }


    /**
     * 访问nameServer来进行我们的路由表更新
     */
    public void updateTopicRouteInfoFromNameServer() {
        Set<String> topicList = new HashSet<>();

        // Consumer
        // 遍历本地存在相关consumer，提取所有的topic
        {
            Iterator<Entry<String, MQConsumerInner>> it = this.consumerTable.entrySet().iterator();
            while (it.hasNext()) {
                Entry<String, MQConsumerInner> entry = it.next();
                MQConsumerInner impl = entry.getValue();
                if (impl != null) {
                    Set<SubscriptionData> subList = impl.subscriptions();
                    if (subList != null) {
                        for (SubscriptionData subData : subList) {
                            topicList.add(subData.getTopic());
                        }
                    }
                }
            }
        }

        // Producer
        //遍历本地存在的相关producer，提取所有的topic
        {
            Iterator<Entry<String, MQProducerInner>> it = this.producerTable.entrySet().iterator();
            while (it.hasNext()) {
                Entry<String, MQProducerInner> entry = it.next();
                MQProducerInner impl = entry.getValue();
                if (impl != null) {
                    Set<String> lst = impl.getPublishTopicList();
                    topicList.addAll(lst);
                }
            }
        }

        //根据topic进行路由表的更新
        for (String topic : topicList) {
            this.updateTopicRouteInfoFromNameServer(topic);
        }
    }

    /**
     * Remove offline broker
     * 清理已经下线的broker
     */
    private void cleanOfflineBroker() {
        try {
            if (this.lockNamesrv.tryLock(LOCK_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS))
                try {
                    ConcurrentHashMap<String, HashMap<Long, String>> updatedTable = new ConcurrentHashMap<String, HashMap<Long, String>>();

                    Iterator<Entry<String, HashMap<Long, String>>> itBrokerTable = this.brokerAddrTable.entrySet().iterator();
                    while (itBrokerTable.hasNext()) {
                        Entry<String, HashMap<Long, String>> entry = itBrokerTable.next();
                        String brokerName = entry.getKey();
                        HashMap<Long, String> oneTable = entry.getValue();

                        HashMap<Long, String> cloneAddrTable = new HashMap<Long, String>();
                        cloneAddrTable.putAll(oneTable);

                        Iterator<Entry<Long, String>> it = cloneAddrTable.entrySet().iterator();
                        while (it.hasNext()) {
                            Entry<Long, String> ee = it.next();
                            String addr = ee.getValue();
                            if (!this.isBrokerAddrExistInTopicRouteTable(addr)) {
                                //不存在，我们删除相关标识符
                                it.remove();
                                log.info("the broker addr[{} {}] is offline, remove it", brokerName, addr);
                            }
                        }

                        if (cloneAddrTable.isEmpty()) {
                            itBrokerTable.remove();
                            log.info("the broker[{}] name's host is offline, remove it", brokerName);
                        } else {
                            updatedTable.put(brokerName, cloneAddrTable);
                        }
                    }

                    if (!updatedTable.isEmpty()) {
                        this.brokerAddrTable.putAll(updatedTable);
                    }
                } finally {
                    this.lockNamesrv.unlock();
                }
        } catch (InterruptedException e) {
            log.warn("cleanOfflineBroker Exception", e);
        }
    }

    /**
     * 检查客户端
     *
     * @throws MQClientException
     */
    public void checkClientInBroker() throws MQClientException {
        //消费端，group和各自的消费接口
        for (Entry<String, MQConsumerInner> entry : this.consumerTable.entrySet()) {
            //订阅的信息
            Set<SubscriptionData> subscriptionInner = entry.getValue().subscriptions();
            if (subscriptionInner == null || subscriptionInner.isEmpty()) {
                return;
            }

            for (SubscriptionData subscriptionData : subscriptionInner) {
                //基于tag更新，忽略
                if (ExpressionType.isTagType(subscriptionData.getExpressionType())) {
                    continue;
                }
                // may need to check one broker every cluster...
                // assume that the configs of every broker in cluster are the the same.
                // 可能需要检查一个代理每个集群假设集群中每个代理的配置是相同的。
                String addr = findBrokerAddrByTopic(subscriptionData.getTopic());
                if (addr == null) {
                    continue;
                }
                try {
                    getMQClientAPIImpl().checkClientInBroker(addr, entry.getKey(), this.clientId, subscriptionData, 3 * 1000);
                } catch (Exception e) {
                    if (e instanceof MQClientException) {
                        throw (MQClientException) e;
                    } else {
                        throw new MQClientException("Check client in broker error, maybe because you use "
                                + subscriptionData.getExpressionType() + " to filter message, but server has not been upgraded to support!"
                                + "This error would not affect the launch of consumer, but may has impact on message receiving if you " +
                                "have use the new features which are not supported by server, please check the log!", e);
                    }
                }
            }
        }
    }

    /**
     * 发送心跳信息，并上传相关过滤类文件
     */
    public void sendHeartbeatToAllBrokerWithLock() {
        if (this.lockHeartbeat.tryLock()) {
            try {
                //发送心跳信息给Broker
                this.sendHeartbeatToAllBroker();
                //上传相关的filterClass源
                this.uploadFilterClassSource();
            } catch (final Exception e) {
                log.error("sendHeartbeatToAllBroker exception", e);
            } finally {
                this.lockHeartbeat.unlock();
            }
        } else {
            log.warn("lock heartBeat, but failed.");
        }
    }

    /**
     * 持久化消费
     */
    private void persistAllConsumerOffset() {
        Iterator<Entry<String, MQConsumerInner>> it = this.consumerTable.entrySet().iterator();
        while (it.hasNext()) {
            Entry<String, MQConsumerInner> entry = it.next();
            MQConsumerInner impl = entry.getValue();
            impl.persistConsumerOffset();
        }
    }

    /**
     * 调整线程池
     */
    public void adjustThreadPool() {
        Iterator<Entry<String, MQConsumerInner>> it = this.consumerTable.entrySet().iterator();
        while (it.hasNext()) {
            Entry<String, MQConsumerInner> entry = it.next();
            MQConsumerInner impl = entry.getValue();
            if (impl == null) {
                continue;
            }
            try {
                if (impl instanceof DefaultMQPushConsumerImpl) {
                    ((DefaultMQPushConsumerImpl) impl).adjustThreadPool();
                }
            } catch (Exception e) {
            }
        }
    }

    /***
     * 从nameServer中跟新相关的路由
     * 网络客户端通过此来更新当前的信息
     * 路由表总是和topic是紧密相关的
     * @param topic 需要更新topic
     * @return 更新成功or失败
     */
    public boolean updateTopicRouteInfoFromNameServer(final String topic) {
        return updateTopicRouteInfoFromNameServer(topic, false, null);
    }

    /**
     * 是否broker存在于Topic路由中
     *
     * @param addr broker地址
     * @return true 存在，false 不存在
     */
    private boolean isBrokerAddrExistInTopicRouteTable(final String addr) {
        Iterator<Entry<String, TopicRouteData>> it = this.topicRouteTable.entrySet().iterator();
        while (it.hasNext()) {
            Entry<String, TopicRouteData> entry = it.next();
            TopicRouteData topicRouteData = entry.getValue();
            List<BrokerData> bds = topicRouteData.getBrokerDatas();
            for (BrokerData bd : bds) {
                if (bd.getBrokerAddrs() != null) {
                    boolean exist = bd.getBrokerAddrs().containsValue(addr);
                    if (exist)
                        return true;
                }
            }
        }
        return false;
    }

    /**
     * 向所有的broker发送心跳信息
     */
    private void sendHeartbeatToAllBroker() {
        //准备心跳数据，向所有的broker发送心跳信息
        final HeartbeatData heartbeatData = this.prepareHeartbeatData();
        final boolean producerEmpty = heartbeatData.getProducerDataSet().isEmpty();
        final boolean consumerEmpty = heartbeatData.getConsumerDataSet().isEmpty();
        if (producerEmpty && consumerEmpty) {
            log.warn("sending heartbeat, but no consumer and no producer");
            return;
        }
        if (this.brokerAddrTable.isEmpty()) {
            return;
        }
        long times = this.sendHeartbeatTimesTotal.getAndIncrement(); //统计次数++
        for (Entry<String, HashMap<Long, String>> entry : this.brokerAddrTable.entrySet()) {
            String brokerName = entry.getKey();
            HashMap<Long, String> oneTable = entry.getValue();
            if (oneTable == null) {
                continue;
            }
            for (Entry<Long, String> entry1 : oneTable.entrySet()) {
                Long id = entry1.getKey();
                String addr = entry1.getValue();
                if (addr == null) {
                    continue;
                }
                if (consumerEmpty) {
                    //对于发送者，我们只需要和master有信息维护就可以了
                    if (id != MixAll.MASTER_ID)
                        continue;
                }

                try {
                    //发送消息返回版本
                    int version = this.mQClientAPIImpl.sendHearbeat(addr, heartbeatData, 3000);
                    if (!this.brokerVersionTable.containsKey(brokerName)) {
                        this.brokerVersionTable.put(brokerName, new HashMap<String, Integer>(4));
                    }
                    this.brokerVersionTable.get(brokerName).put(addr, version);
                    //20次心跳打印一次，默认400s中打印一次相关心跳
                    if (times % 20 == 0) {
                        log.info("send heart beat to broker[{} {} {}] success", brokerName, id, addr);
                        log.info(heartbeatData.toString());
                    }
                } catch (Exception e) {
                    if (this.isBrokerInNameServer(addr)) {
                        log.info("send heart beat to broker[{} {} {}] failed", brokerName, id, addr, e);
                    } else {
                        log.info("send heart beat to broker[{} {} {}] exception, because the broker not up, forget it", brokerName, id, addr, e);
                    }
                }
            }
        }
    }

    /**
     * 上传基于ClassFilter的操作,已经被废弃
     */
    private void uploadFilterClassSource() {
        for (Entry<String, MQConsumerInner> next : this.consumerTable.entrySet()) {
            MQConsumerInner consumer = next.getValue();
            if (CONSUME_PASSIVELY == consumer.consumeType()) { //push才支持
                Set<SubscriptionData> subscriptions = consumer.subscriptions(); //订阅信息
                for (SubscriptionData sub : subscriptions) {
                    if (sub.isClassFilterMode() && sub.getFilterClassSource() != null) { //支持classFilter并且存在类
                        final String consumerGroup = consumer.groupName();
                        final String className = sub.getSubString();
                        final String topic = sub.getTopic();
                        final String filterClassSource = sub.getFilterClassSource();
                        try {
                            this.uploadFilterClassToAllFilterServer(consumerGroup, className, topic, filterClassSource);
                        } catch (Exception e) {
                            log.error("uploadFilterClassToAllFilterServer Exception", e);
                        }
                    }
                }
            }
        }
    }

    /**
     * 从nameServer中获得相关的路由信息，
     * 从nameServer中获得topicRoute,从中构建出相关发布或者订阅的信息
     * 发布信息TopicPublishInfo和订阅信息Set<MessageQueue>和Broker的信息BrokerData
     *
     * @param topic
     * @param isDefault 是否使用默认的topic
     * @param defaultMQProducer 是否是有默认的topic
     * @return
     */
    public boolean updateTopicRouteInfoFromNameServer(final String topic, boolean isDefault, DefaultMQProducer defaultMQProducer) {
        try {
            //尝试加锁，如果超时会出现相关的问题,超时的时间为3000
            if (this.lockNamesrv.tryLock(LOCK_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)) {
                try {
                    TopicRouteData topicRouteData;
                    if (isDefault && defaultMQProducer != null) {
                        // 存在后面的两个选项，使用默认的topickey，note这里是使用了特殊的topic也就是这里不再有是有传递进来的topic，
                        // 最终默认的topic作为我们的路由信息 TBW102
                        topicRouteData = this.mQClientAPIImpl.getDefaultTopicRouteInfoFromNameServer(defaultMQProducer.getCreateTopicKey(), 1000 * 3);

                        if (topicRouteData != null) {
                            //存在相关的信息
                            for (QueueData data : topicRouteData.getQueueDatas()) {
                                //使用本地默认数量和远程数据的数量进行比较，选取其中小的
                                //这是使用默认路由的时候
                                int queueNums = Math.min(defaultMQProducer.getDefaultTopicQueueNums(), data.getReadQueueNums()); //4和设定值中选取小的
                                data.setReadQueueNums(queueNums);
                                data.setWriteQueueNums(queueNums);
                            }
                        }
                    } else {
                        topicRouteData = this.mQClientAPIImpl.getTopicRouteInfoFromNameServer(topic, 1000 * 3);
                    }

                    if (topicRouteData != null) {
                        //远程的topicInfo存在
                        TopicRouteData old = this.topicRouteTable.get(topic); //内存中旧的topicInfo
                        boolean changed = topicRouteDataIsChange(old, topicRouteData); //是否存在变更
                        if (!changed) {
                            //发生了变更
                            changed = this.isNeedUpdateTopicRouteInfo(topic); //是否需要变更
                        } else {
                            log.info("the topic[{}] route info changed, old[{}] ,new[{}]", topic, old, topicRouteData);
                        }

                        if (changed) { //需要变更
                            TopicRouteData cloneTopicRouteData = topicRouteData.cloneTopicRouteData();

                            for (BrokerData bd : topicRouteData.getBrokerDatas()) {
                                //更新topicinfo携带的broker相关的信息
                                this.brokerAddrTable.put(bd.getBrokerName(), bd.getBrokerAddrs());
                            }

                            // Update Pub info
                            // 更新发布的信息，让提供方自己更新
                            {
                                TopicPublishInfo publishInfo = topicRouteData2TopicPublishInfo(topic, topicRouteData);
                                publishInfo.setHaveTopicRouterInfo(true); //已经存在了topicInfo信息，而不仅仅是一个空壳子
                                for (Entry<String, MQProducerInner> entry : this.producerTable.entrySet()) {
                                    MQProducerInner impl = entry.getValue();
                                    if (impl != null) {
                                        impl.updateTopicPublishInfo(topic, publishInfo); //发送者进行更改相关的topic和相应的topicInfo
                                    }
                                }
                            }

                            // Update sub info
                            //更新相关的订阅的相关的信息，让消费方自己更新
                            {
                                Set<MessageQueue> subscribeInfo = topicRouteData2TopicSubscribeInfo(topic, topicRouteData);
                                for (Entry<String, MQConsumerInner> entry : this.consumerTable.entrySet()) {
                                    MQConsumerInner impl = entry.getValue();
                                    if (impl != null) {
                                        impl.updateTopicSubscribeInfo(topic, subscribeInfo); //消费者进行更改相关的topic和相应的订阅info
                                    }
                                }
                            }
                            log.info("topicRouteTable.put. Topic = {}, TopicRouteData[{}]", topic, cloneTopicRouteData);
                            this.topicRouteTable.put(topic, cloneTopicRouteData); //放置相关的topic和对应的data信息
                            return true;
                        }
                    } else {
                        log.warn("updateTopicRouteInfoFromNameServer, getTopicRouteInfoFromNameServer return null, Topic: {}", topic);
                    }
                } catch (Exception e) {
                    if (!topic.startsWith(MixAll.RETRY_GROUP_TOPIC_PREFIX) && !topic.equals(MixAll.AUTO_CREATE_TOPIC_KEY_TOPIC)) {
                        log.warn("updateTopicRouteInfoFromNameServer Exception", e);
                    }
                } finally {
                    this.lockNamesrv.unlock();
                }
            } else {
                log.warn("updateTopicRouteInfoFromNameServer tryLock timeout {}ms", LOCK_TIMEOUT_MILLIS);
            }
        } catch (InterruptedException e) {
            log.warn("updateTopicRouteInfoFromNameServer Exception", e);
        }

        return false;
    }

    /**
     * 客户端构建自己的心跳信息
     *
     * @return 心跳数据
     */
    private HeartbeatData prepareHeartbeatData() {
        HeartbeatData heartbeatData = new HeartbeatData();

        // clientID，设置客户端标识
        heartbeatData.setClientID(this.clientId);

        // 该客户端下的所有consumer时构建data进入，主要是构建consumerData，从inner中提起
        for (Map.Entry<String, MQConsumerInner> entry : this.consumerTable.entrySet()) {
            MQConsumerInner impl = entry.getValue();
            if (impl == null) {
                continue;
            }
            //消费描述数据，从消费者中提取
            ConsumerData consumerData = new ConsumerData();
            consumerData.setGroupName(impl.groupName());
            consumerData.setConsumeType(impl.consumeType());
            consumerData.setMessageModel(impl.messageModel());
            consumerData.setConsumeFromWhere(impl.consumeFromWhere());
            consumerData.getSubscriptionDataSet().addAll(impl.subscriptions());
            consumerData.setUnitMode(impl.isUnitMode());

            heartbeatData.getConsumerDataSet().add(consumerData);
        }

        // 该客户端下的遍历相关的producer时构建data进入
        for (Map.Entry<String/* group */, MQProducerInner> entry : this.producerTable.entrySet()) {
            MQProducerInner impl = entry.getValue();
            if (impl == null) {
                continue;
            }
            ProducerData producerData = new ProducerData();
            producerData.setGroupName(entry.getKey());
            heartbeatData.getProducerDataSet().add(producerData);
        }
        return heartbeatData;
    }

    /**
     * broker在nameserver的信息中
     * 这个信息仅仅是当前的快照。
     * @param brokerAddr
     * @return
     */
    private boolean isBrokerInNameServer(final String brokerAddr) {
        Iterator<Entry<String, TopicRouteData>> it = this.topicRouteTable.entrySet().iterator();
        while (it.hasNext()) {
            Entry<String, TopicRouteData> itNext = it.next();
            List<BrokerData> brokerDatas = itNext.getValue().getBrokerDatas();
            for (BrokerData bd : brokerDatas) {
                boolean contain = bd.getBrokerAddrs().containsValue(brokerAddr);
                if (contain)
                    return true;
            }
        }
        return false;
    }

    /**
     * This method will be removed in the version 5.0.0,because filterServer was removed,and method <code>subscribe(final String topic, final MessageSelector messageSelector)</code>
     * is recommended.
     * <p>
     * 此方法将在5.0.0版中删除，因为删除了filterServer，建议使用方法subscribe（final String topic，final MessageSelector messageSelector）
     * </p>
     */
    @Deprecated
    private void uploadFilterClassToAllFilterServer(final String consumerGroup, final String fullClassName, final String topic, final String filterClassSource)
            throws UnsupportedEncodingException {
        byte[] classBody = null;
        int classCRC = 0;
        try {
            classBody = filterClassSource.getBytes(MixAll.DEFAULT_CHARSET);
            classCRC = UtilAll.crc32(classBody);
        } catch (Exception e1) {
            log.warn("uploadFilterClassToAllFilterServer Exception, ClassName: {} {}", fullClassName, exceptionSimpleDesc(e1));
        }

        TopicRouteData topicRouteData = this.topicRouteTable.get(topic); //获得topic的路由信息
        if (topicRouteData != null && topicRouteData.getFilterServerTable() != null && !topicRouteData.getFilterServerTable().isEmpty()) {
            Iterator<Entry<String, List<String>>> it = topicRouteData.getFilterServerTable().entrySet().iterator();
            while (it.hasNext()) {
                Entry<String, List<String>> next = it.next();
                List<String> value = next.getValue();
                for (final String fsAddr : value) {
                    try {
                        //注册filter过滤类
                        this.mQClientAPIImpl.registerMessageFilterClass(fsAddr, consumerGroup, topic, fullClassName, classCRC, classBody, 5000);

                        log.info("register message class filter to {} OK, ConsumerGroup: {} Topic: {} ClassName: {}", fsAddr, consumerGroup,
                                topic, fullClassName);

                    } catch (Exception e) {
                        log.error("uploadFilterClassToAllFilterServer Exception", e);
                    }
                }
            }
        } else {
            log.warn("register message class filter failed, because no filter server, ConsumerGroup: {} Topic: {} ClassName: {}",
                    consumerGroup, topic, fullClassName);
        }
    }

    /**
     * topic路由信息发生变更，
     * 我们比较老的和新的data进行比较来确定是否需要进行改变
     *
     * @param olddata
     * @param nowdata
     * @return
     */
    private boolean topicRouteDataIsChange(TopicRouteData olddata, TopicRouteData nowdata) {
        if (olddata == null || nowdata == null)
            return true;
        TopicRouteData old = olddata.cloneTopicRouteData();
        TopicRouteData now = nowdata.cloneTopicRouteData();
        Collections.sort(old.getQueueDatas());
        Collections.sort(old.getBrokerDatas());
        Collections.sort(now.getQueueDatas());
        Collections.sort(now.getBrokerDatas());
        return !old.equals(now);

    }

    /**
     * 是否需要进行更新相关的topic路由信息
     *
     * @param topic
     * @return
     */
    private boolean isNeedUpdateTopicRouteInfo(final String topic) {
        boolean result = false;
        //提供方的变更
        {
            Iterator<Entry<String, MQProducerInner>> it = this.producerTable.entrySet().iterator();
            while (it.hasNext() && !result) {
                Entry<String, MQProducerInner> entry = it.next();
                MQProducerInner impl = entry.getValue();
                if (impl != null) {
                    //让实现端来进行来决定是否要进行更新
                    result = impl.isPublishTopicNeedUpdate(topic);
                }
            }
        }
        //消费端的变更

        {
            Iterator<Entry<String, MQConsumerInner>> it = this.consumerTable.entrySet().iterator();
            while (it.hasNext() && !result) {
                Entry<String, MQConsumerInner> entry = it.next();
                MQConsumerInner impl = entry.getValue();
                if (impl != null) {
                    //让实现端来进行决定是否要进行更新
                    result = impl.isSubscribeTopicNeedUpdate(topic);
                }
            }
        }

        return result;
    }

    public void shutdown() {
        // Consumer
        if (!this.consumerTable.isEmpty())
            return;

        // AdminExt
        if (!this.adminExtTable.isEmpty())
            return;

        // Producer
        if (this.producerTable.size() > 1)
            return;

        synchronized (this) {
            switch (this.serviceState) {
                case CREATE_JUST:
                    break;
                case RUNNING:
                    this.defaultMQProducer.getDefaultMQProducerImpl().shutdown(false);

                    this.serviceState = ServiceState.SHUTDOWN_ALREADY;
                    this.pullMessageService.shutdown(true);
                    this.scheduledExecutorService.shutdown();
                    this.mQClientAPIImpl.shutdown();
                    this.rebalanceService.shutdown();
                    MQClientManager.getInstance().removeClientFactory(this.clientId);
                    log.info("the client factory [{}] shutdown OK", this.clientId);
                    break;
                case SHUTDOWN_ALREADY:
                    break;
                default:
                    break;
            }
        }
    }

    /**
     * 注册consumer,注册表构建在内存中
     *
     * @param group
     * @param consumer
     * @return
     */
    public boolean registerConsumer(final String group, final MQConsumerInner consumer) {
        if (null == group || null == consumer) {
            return false;
        }

        MQConsumerInner prev = this.consumerTable.putIfAbsent(group, consumer);
        if (prev != null) {
            log.warn("the consumer group[" + group + "] exist already.");
            return false;
        }

        return true;
    }

    public void unregisterConsumer(final String group) {
        this.consumerTable.remove(group);
        this.unregisterClientWithLock(null, group);
    }

    private void unregisterClientWithLock(final String producerGroup, final String consumerGroup) {
        try {
            if (this.lockHeartbeat.tryLock(LOCK_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)) {
                try {
                    this.unregisterClient(producerGroup, consumerGroup);
                } catch (Exception e) {
                    log.error("unregisterClient exception", e);
                } finally {
                    this.lockHeartbeat.unlock();
                }
            } else {
                log.warn("lock heartBeat, but failed.");
            }
        } catch (InterruptedException e) {
            log.warn("unregisterClientWithLock exception", e);
        }
    }

    private void unregisterClient(final String producerGroup, final String consumerGroup) {
        Iterator<Entry<String, HashMap<Long, String>>> it = this.brokerAddrTable.entrySet().iterator();
        while (it.hasNext()) {
            Entry<String, HashMap<Long, String>> entry = it.next();
            String brokerName = entry.getKey();
            HashMap<Long, String> oneTable = entry.getValue();

            if (oneTable != null) {
                for (Map.Entry<Long, String> entry1 : oneTable.entrySet()) {
                    String addr = entry1.getValue();
                    if (addr != null) {
                        try {
                            this.mQClientAPIImpl.unregisterClient(addr, this.clientId, producerGroup, consumerGroup, 3000);
                            log.info("unregister client[Producer: {} Consumer: {}] from broker[{} {} {}] success", producerGroup, consumerGroup, brokerName, entry1.getKey(), addr);
                        } catch (RemotingException e) {
                            log.error("unregister client exception from broker: " + addr, e);
                        } catch (InterruptedException e) {
                            log.error("unregister client exception from broker: " + addr, e);
                        } catch (MQBrokerException e) {
                            log.error("unregister client exception from broker: " + addr, e);
                        }
                    }
                }
            }
        }
    }

    /**
     * 注册发送端，构建在内存中
     *
     * @param group
     * @param producer
     * @return
     */
    public boolean registerProducer(final String group, final DefaultMQProducerImpl producer) {
        if (null == group || null == producer) {
            return false;
        }

        MQProducerInner prev = this.producerTable.putIfAbsent(group, producer);
        if (prev != null) {
            log.warn("the producer group[{}] exist already.", group);
            return false;
        }

        return true;
    }

    public void unregisterProducer(final String group) {
        this.producerTable.remove(group);
        this.unregisterClientWithLock(group, null);
    }

    /**
     * 注册管理端，构建在内存中
     *
     * @param group
     * @param admin
     * @return
     */
    public boolean registerAdminExt(final String group, final MQAdminExtInner admin) {
        if (null == group || null == admin) {
            return false;
        }

        MQAdminExtInner prev = this.adminExtTable.putIfAbsent(group, admin);
        if (prev != null) {
            log.warn("the admin group[{}] exist already.", group);
            return false;
        }

        return true;
    }

    public void unregisterAdminExt(final String group) {
        this.adminExtTable.remove(group);
    }

    /**
     * 唤醒正处于睡眠的负载均衡服务，执行相关的平衡事件
     * 只有在服务一开始时，或者broker进行通知时产生
     */
    public void rebalanceImmediately() {
        this.rebalanceService.wakeup();
    }

    /**
     * 客户端使用的rebalance，网络客户端支持
     */
    public void doRebalance() {
        //内部使用表，对每一个内部的Inner进行rebalance，也就对每一个消息者本身进行负载均衡操作
        for (Map.Entry<String, MQConsumerInner> entry : this.consumerTable.entrySet()) {
            MQConsumerInner impl = entry.getValue();
            if (impl == null) {
                continue;
            }
            try {
                impl.doRebalance(); //消费端自己负载均衡
            } catch (Throwable e) {
                log.error("doRebalance exception", e);
            }
        }
    }

    /**
     * 获得group对应的生产者
     *
     * @param group
     * @return
     */
    public MQProducerInner selectProducer(final String group) {
        return this.producerTable.get(group);
    }

    /**
     * 获得消费组group对应的消费端
     *
     * @param group
     * @return
     */
    public MQConsumerInner selectConsumer(final String group) {
        return this.consumerTable.get(group);
    }

    public FindBrokerResult findBrokerAddressInAdmin(final String brokerName) {
        String brokerAddr = null;
        boolean slave = false;
        boolean found = false;

        HashMap<Long/* brokerId */, String/* address */> map = this.brokerAddrTable.get(brokerName);
        if (map != null && !map.isEmpty()) {
            for (Map.Entry<Long, String> entry : map.entrySet()) {
                Long id = entry.getKey();
                brokerAddr = entry.getValue();
                if (brokerAddr != null) {
                    found = true;
                    if (MixAll.MASTER_ID == id) {
                        slave = false;
                    } else {
                        slave = true;
                    }
                    break;

                }
            } // end of for
        }

        if (found) {
            return new FindBrokerResult(brokerAddr, slave, findBrokerVersion(brokerName, brokerAddr));
        }

        return null;
    }

    /**
     * 在内存中找到对应的broker
     * 首先根据name找到对应集合，从中获取主节点，masterId为0是主节点，非0是非主节点
     *
     * @param brokerName
     * @return
     */
    public String findBrokerAddressInPublish(final String brokerName) {
        //获得brokerName下的主从映射关系，key为角色标记，value为地址
        HashMap<Long/* brokerId */, String/* address */> map = this.brokerAddrTable.get(brokerName);
        if (map != null && !map.isEmpty()) {
            //总是优先获得主节点
            return map.get(MixAll.MASTER_ID);
        }

        return null;
    }


    /**
     * 查找broker结果
     *
     * @param brokerName
     * @param brokerId
     * @param onlyThisBroker 是否支持找不到进行转移 false支持，true不支持
     * @return 查找broker的结果
     */
    public FindBrokerResult findBrokerAddressInSubscribe(final String brokerName, final long brokerId, final boolean onlyThisBroker) {
        String brokerAddr = null;
        boolean slave = false;
        boolean found = false;

        //获得同一brokerName下的相关的节点，一般是主从节点
        HashMap<Long/* brokerId */, String/* address */> map = this.brokerAddrTable.get(brokerName);

        if (map != null && !map.isEmpty()) {
            //使用broker标识获得地址
            brokerAddr = map.get(brokerId);
            //是否是salve
            slave = brokerId != MixAll.MASTER_ID;
            //是否找到
            found = brokerAddr != null;

            if (!found && !onlyThisBroker) {
                //找不到，且没有强制指出必须获得这个指定的Broker

                //遍历获得一个可以找到的Broker
                Entry<Long, String> entry = map.entrySet().iterator().next();
                brokerAddr = entry.getValue();
                //确定是否是slave
                slave = entry.getKey() != MixAll.MASTER_ID;
                //找到节点
                found = true;
            }
        }

        if (found) {
            return new FindBrokerResult(brokerAddr, slave, findBrokerVersion(brokerName, brokerAddr));
        }

        return null;
    }

    public int findBrokerVersion(String brokerName, String brokerAddr) {
        if (this.brokerVersionTable.containsKey(brokerName)) {
            if (this.brokerVersionTable.get(brokerName).containsKey(brokerAddr)) {
                return this.brokerVersionTable.get(brokerName).get(brokerAddr);
            }
        }
        //To do need to fresh the version
        return 0;
    }

    /**
     * 获得对应consumerId
     *
     * @param topic
     * @param group
     * @return
     */
    public List<String> findConsumerIdList(final String topic, final String group) {
        String brokerAddr = this.findBrokerAddrByTopic(topic); //查找topic对应的broker
        if (null == brokerAddr) {
            //地址为空，
            //从nameServer中更新一下获得相关的信息
            this.updateTopicRouteInfoFromNameServer(topic);
            //再次调用找到相关的数据，从NameServer中抓取消息后，我们有了相应的内存结构
            brokerAddr = this.findBrokerAddrByTopic(topic);
        }
        if (null == brokerAddr) {
            return null;
        }
        try {
            //获得同一个消费组下的相关信息
            return this.mQClientAPIImpl.getConsumerIdListByGroup(brokerAddr, group, 3000); //找到group对应的队列ids
        } catch (Exception e) {
            log.warn("getConsumerIdListByGroup exception, " + brokerAddr + " " + group, e);
        }
        return null;
    }

    /**
     * 根据topic查找相关的broker
     *
     * @param topic
     * @return
     */
    public String findBrokerAddrByTopic(final String topic) {
        TopicRouteData topicRouteData = this.topicRouteTable.get(topic); //获得内存保留的基于topicroute的数据
        if (topicRouteData == null) {
            return null;
        }
        List<BrokerData> brokers = topicRouteData.getBrokerDatas(); //从路由数据中找到相关的broker信息
        if (brokers.isEmpty()) {
            return null;
        }
        //随机的选择一个，我们使用该broker的地址
        int index = random.nextInt(brokers.size());
        BrokerData bd = brokers.get(index % brokers.size());
        return bd.selectBrokerAddr();
    }

    /**
     * 重置客户端的offset
     *
     * @param topic       主题
     * @param group       消费组
     * @param offsetTable 重置的内容
     */
    public void resetOffset(String topic, String group, Map<MessageQueue, Long> offsetTable) {
        DefaultMQPushConsumerImpl consumer = null;
        try {
            MQConsumerInner impl = this.consumerTable.get(group);
            if (impl != null && impl instanceof DefaultMQPushConsumerImpl) {
                consumer = (DefaultMQPushConsumerImpl) impl;
            } else {
                log.info("[reset-offset] consumer dose not exist. group={}", group);
                return;
            }
            consumer.suspend();

            //获得消费表
            ConcurrentMap<MessageQueue, ProcessQueue> processQueueTable = consumer.getRebalanceImpl().getProcessQueueTable();
            //遍历处理
            for (Map.Entry<MessageQueue, ProcessQueue> entry : processQueueTable.entrySet()) {
                MessageQueue mq = entry.getKey();
                //存在
                if (topic.equals(mq.getTopic()) && offsetTable.containsKey(mq)) {
                    //标记删除
                    ProcessQueue pq = entry.getValue();
                    pq.setDropped(true);
                    pq.clear();
                }
            }

            try {
                TimeUnit.SECONDS.sleep(10);
            } catch (InterruptedException e) {
            }

            //遍历
            Iterator<MessageQueue> iterator = processQueueTable.keySet().iterator();
            while (iterator.hasNext()) {
                MessageQueue mq = iterator.next();
                Long offset = offsetTable.get(mq);
                if (topic.equals(mq.getTopic()) && offset != null) {
                    try {
                        //更新offset
                        consumer.updateConsumeOffset(mq, offset);
                        //尝试删除
                        consumer.getRebalanceImpl().removeUnnecessaryMessageQueue(mq, processQueueTable.get(mq));
                        //删除
                        iterator.remove();
                    } catch (Exception e) {
                        log.warn("reset offset failed. group={}, {}", group, mq, e);
                    }
                }
            }
        } finally {
            if (consumer != null) {
                consumer.resume();
            }
        }
    }

    public Map<MessageQueue, Long> getConsumerStatus(String topic, String group) {
        MQConsumerInner impl = this.consumerTable.get(group);
        if (impl != null && impl instanceof DefaultMQPushConsumerImpl) {
            DefaultMQPushConsumerImpl consumer = (DefaultMQPushConsumerImpl) impl;
            return consumer.getOffsetStore().cloneOffsetTable(topic);
        } else if (impl != null && impl instanceof DefaultMQPullConsumerImpl) {
            DefaultMQPullConsumerImpl consumer = (DefaultMQPullConsumerImpl) impl;
            return consumer.getOffsetStore().cloneOffsetTable(topic);
        } else {
            return Collections.EMPTY_MAP;
        }
    }

    public TopicRouteData getAnExistTopicRouteData(final String topic) {
        return this.topicRouteTable.get(topic);
    }

    public MQClientAPIImpl getMQClientAPIImpl() {
        return mQClientAPIImpl;
    }

    public MQAdminImpl getMQAdminImpl() {
        return mQAdminImpl;
    }

    public long getBootTimestamp() {
        return bootTimestamp;
    }

    public ScheduledExecutorService getScheduledExecutorService() {
        return scheduledExecutorService;
    }

    public PullMessageService getPullMessageService() {
        return pullMessageService;
    }

    public DefaultMQProducer getDefaultMQProducer() {
        return defaultMQProducer;
    }

    public ConcurrentMap<String, TopicRouteData> getTopicRouteTable() {
        return topicRouteTable;
    }

    /**
     * 直接消费消息
     *
     * @param msg
     * @param consumerGroup
     * @param brokerName
     * @return
     */
    public ConsumeMessageDirectlyResult consumeMessageDirectly(final MessageExt msg, final String consumerGroup, final String brokerName) {
        MQConsumerInner mqConsumerInner = this.consumerTable.get(consumerGroup); //获得消费端
        if (null != mqConsumerInner) {
            DefaultMQPushConsumerImpl consumer = (DefaultMQPushConsumerImpl) mqConsumerInner;
            //直接使用消费服务进行直接的消费
            ConsumeMessageDirectlyResult result = consumer.getConsumeMessageService().consumeMessageDirectly(msg, brokerName);
            return result;
        }
        return null;
    }

    public ConsumerRunningInfo consumerRunningInfo(final String consumerGroup) {
        MQConsumerInner mqConsumerInner = this.consumerTable.get(consumerGroup);

        ConsumerRunningInfo consumerRunningInfo = mqConsumerInner.consumerRunningInfo();

        List<String> nsList = this.mQClientAPIImpl.getRemotingClient().getNameServerAddressList();

        StringBuilder strBuilder = new StringBuilder();
        if (nsList != null) {
            for (String addr : nsList) {
                strBuilder.append(addr).append(";");
            }
        }

        String nsAddr = strBuilder.toString();
        consumerRunningInfo.getProperties().put(ConsumerRunningInfo.PROP_NAMESERVER_ADDR, nsAddr);
        consumerRunningInfo.getProperties().put(ConsumerRunningInfo.PROP_CONSUME_TYPE, mqConsumerInner.consumeType().name());
        consumerRunningInfo.getProperties().put(ConsumerRunningInfo.PROP_CLIENT_VERSION,
                MQVersion.getVersionDesc(MQVersion.CURRENT_VERSION));

        return consumerRunningInfo;
    }

    public ConsumerStatsManager getConsumerStatsManager() {
        return consumerStatsManager;
    }

    public NettyClientConfig getNettyClientConfig() {
        return nettyClientConfig;
    }
}
