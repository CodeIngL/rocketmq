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
package org.apache.rocketmq.broker.out;

import com.google.common.collect.Lists;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.rocketmq.broker.latency.BrokerFixedThreadPoolExecutor;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.common.DataVersion;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.ThreadFactoryImpl;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.common.namesrv.RegisterBrokerResult;
import org.apache.rocketmq.common.namesrv.TopAddressing;
import org.apache.rocketmq.common.protocol.RequestCode;
import org.apache.rocketmq.common.protocol.ResponseCode;
import org.apache.rocketmq.common.protocol.body.ConsumerOffsetSerializeWrapper;
import org.apache.rocketmq.common.protocol.body.KVTable;
import org.apache.rocketmq.common.protocol.body.RegisterBrokerBody;
import org.apache.rocketmq.common.protocol.body.SubscriptionGroupWrapper;
import org.apache.rocketmq.common.protocol.body.TopicConfigSerializeWrapper;
import org.apache.rocketmq.common.protocol.header.namesrv.QueryDataVersionRequestHeader;
import org.apache.rocketmq.common.protocol.header.namesrv.QueryDataVersionResponseHeader;
import org.apache.rocketmq.common.protocol.header.namesrv.RegisterBrokerRequestHeader;
import org.apache.rocketmq.common.protocol.header.namesrv.RegisterBrokerResponseHeader;
import org.apache.rocketmq.common.protocol.header.namesrv.UnRegisterBrokerRequestHeader;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.remoting.RemotingClient;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;
import org.apache.rocketmq.remoting.exception.RemotingConnectException;
import org.apache.rocketmq.remoting.exception.RemotingSendRequestException;
import org.apache.rocketmq.remoting.exception.RemotingTimeoutException;
import org.apache.rocketmq.remoting.exception.RemotingTooMuchRequestException;
import org.apache.rocketmq.remoting.netty.NettyClientConfig;
import org.apache.rocketmq.remoting.netty.NettyRemotingClient;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;

import static org.apache.rocketmq.remoting.protocol.RemotingCommand.createRequestCommand;

/**
 * <p>
 *     broker的客户端网络接口，维护与namesvr的通讯，每隔30s执行一次registerBroker,用于把自身维护的topic信息发送到所有namesvr，同时这次报文也充当心跳的作用。
 * </p>
 */
public class BrokerOuterAPI {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.BROKER_LOGGER_NAME);
    private final RemotingClient remotingClient;
    private final TopAddressing topAddressing = new TopAddressing(MixAll.getWSAddr());
    //服务端地址
    private String nameSrvAddr = null;
    private BrokerFixedThreadPoolExecutor brokerOuterExecutor = new BrokerFixedThreadPoolExecutor(4, 10, 1, TimeUnit.MINUTES,
            new ArrayBlockingQueue<>(32), new ThreadFactoryImpl("brokerOutApi_thread_", true));

    public BrokerOuterAPI(final NettyClientConfig nettyClientConfig) {
        this(nettyClientConfig, null);
    }

    public BrokerOuterAPI(final NettyClientConfig nettyClientConfig, RPCHook rpcHook) {
        this.remotingClient = new NettyRemotingClient(nettyClientConfig);
        this.remotingClient.registerRPCHook(rpcHook);
    }

    public void start() {
        this.remotingClient.start();
    }

    public void shutdown() {
        this.remotingClient.shutdown();
        this.brokerOuterExecutor.shutdown();
    }

    /**
     * 获得nameServer地址
     * @return
     */
    public String fetchNameServerAddr() {
        try {
            String addrs = this.topAddressing.fetchNSAddr();
            if (addrs != null) {
                if (!addrs.equals(this.nameSrvAddr)) {
                    log.info("name server address changed, old: {} new: {}", this.nameSrvAddr, addrs);
                    this.updateNameServerAddressList(addrs);
                    this.nameSrvAddr = addrs;
                    return nameSrvAddr;
                }
            }
        } catch (Exception e) {
            log.error("fetchNameServerAddr Exception", e);
        }
        return nameSrvAddr;
    }

    /**
     * 更新as的list分号是；分开
     * @param addrs
     */
    public void updateNameServerAddressList(final String addrs) {
        List<String> lst = new ArrayList<String>();
        String[] addrArray = addrs.split(";");
        for (String addr : addrArray) {
            lst.add(addr);
        }
        this.remotingClient.updateNameServerAddressList(lst);
    }

    /**
     * 向所有的nameserver进行注册相关broker
     * @param clusterName
     * @param brokerAddr
     * @param brokerName
     * @param brokerId
     * @param haServerAddr
     * @param topicConfigWrapper
     * @param filterServerList
     * @param oneway
     * @param timeoutMills
     * @param compressed
     * @return
     */
    public List<RegisterBrokerResult> registerBrokerAll(
        final String clusterName,
        final String brokerAddr,
        final String brokerName,
        final long brokerId,
        final String haServerAddr,
        final TopicConfigSerializeWrapper topicConfigWrapper,
        final List<String> filterServerList,
        final boolean oneway,
        final int timeoutMills,
        final boolean compressed) {

        final List<RegisterBrokerResult> registerBrokerResultList = Lists.newArrayList();
        List<String> nameServerAddressList = this.remotingClient.getNameServerAddressList();
        if (nameServerAddressList != null && nameServerAddressList.size() > 0) {

            final RegisterBrokerRequestHeader reqHeader = new RegisterBrokerRequestHeader();
            reqHeader.setBrokerAddr(brokerAddr);
            reqHeader.setBrokerId(brokerId);
            reqHeader.setBrokerName(brokerName);
            reqHeader.setClusterName(clusterName);
            reqHeader.setHaServerAddr(haServerAddr);
            reqHeader.setCompressed(compressed);

            RegisterBrokerBody reqBody = new RegisterBrokerBody();
            reqBody.setTopicConfigSerializeWrapper(topicConfigWrapper);
            reqBody.setFilterServerList(filterServerList);
            final byte[] body = reqBody.encode(compressed);
            final int bodyCrc32 = UtilAll.crc32(body);
            reqHeader.setBodyCrc32(bodyCrc32);
            final CountDownLatch countDownLatch = new CountDownLatch(nameServerAddressList.size());
            for (final String namesrvAddr : nameServerAddressList) {
                brokerOuterExecutor.execute(() -> {
                    try {
                        RegisterBrokerResult result = registerBroker(namesrvAddr,oneway, timeoutMills,reqHeader,body);
                        if (result != null) {
                            registerBrokerResultList.add(result);
                        }

                        log.info("register broker[{}]to name server {} OK", brokerId, namesrvAddr);
                    } catch (Exception e) {
                        log.warn("registerBroker Exception, {}", namesrvAddr, e);
                    } finally {
                        countDownLatch.countDown();
                    }
                });
            }

            try {
                countDownLatch.await(timeoutMills, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
            }
        }

        return registerBrokerResultList;
    }

    /**
     * 向指定的namserver进行注册broker
     * @param namesrvAddr
     * @param oneway
     * @param timeoutMills
     * @param reqHeader
     * @param body
     * @return
     * @throws RemotingCommandException
     * @throws MQBrokerException
     * @throws RemotingConnectException
     * @throws RemotingSendRequestException
     * @throws RemotingTimeoutException
     * @throws InterruptedException
     */
    private RegisterBrokerResult registerBroker(final String namesrvAddr, final boolean oneway, final int timeoutMills, final RegisterBrokerRequestHeader reqHeader, final byte[] body)
            throws RemotingCommandException, MQBrokerException, RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException, InterruptedException {
        RemotingCommand req = createRequestCommand(RequestCode.REGISTER_BROKER, reqHeader);
        req.setBody(body);

        if (oneway) {
            try {
                this.remotingClient.invokeOneway(namesrvAddr, req, timeoutMills);
            } catch (RemotingTooMuchRequestException e) {
                // Ignore
            }
            return null;
        }

        RemotingCommand resp = this.remotingClient.invokeSync(namesrvAddr, req, timeoutMills);
        assert resp != null;
        switch (resp.getCode()) {
            case ResponseCode.SUCCESS: {
                RegisterBrokerResponseHeader respHeader =
                    (RegisterBrokerResponseHeader) resp.decodeCommandCustomHeader(RegisterBrokerResponseHeader.class);
                RegisterBrokerResult result = new RegisterBrokerResult();
                result.setMasterAddr(respHeader.getMasterAddr());
                result.setHaServerAddr(respHeader.getHaServerAddr());
                if (resp.getBody() != null) {
                    result.setKvTable(KVTable.decode(resp.getBody(), KVTable.class));
                }
                return result;
            }
            default:
                break;
        }

        throw new MQBrokerException(resp.getCode(), resp.getRemark());
    }

    /**
     * 向所有的namserver注销自己这个broker
     * @param clusterName
     * @param brokerAddr
     * @param brokerName
     * @param brokerId
     */
    public void unregisterBrokerAll(final String clusterName, final String brokerAddr, final String brokerName, final long brokerId) {
        List<String> nameServerAddressList = this.remotingClient.getNameServerAddressList();
        if (nameServerAddressList != null) {
            for (String namesrvAddr : nameServerAddressList) {
                try {
                    this.unregisterBroker(namesrvAddr, clusterName, brokerAddr, brokerName, brokerId);
                    log.info("unregisterBroker OK, NamesrvAddr: {}", namesrvAddr);
                } catch (Exception e) {
                    log.warn("unregisterBroker Exception, {}", namesrvAddr, e);
                }
            }
        }
    }

    /**
     * 在指定的namserver上注销本broker
     * @param namesrvAddr
     * @param clusterName
     * @param brokerAddr
     * @param brokerName
     * @param brokerId
     * @throws RemotingConnectException
     * @throws RemotingSendRequestException
     * @throws RemotingTimeoutException
     * @throws InterruptedException
     * @throws MQBrokerException
     */
    public void unregisterBroker(final String namesrvAddr, final String clusterName, final String brokerAddr, final String brokerName, final long brokerId
    ) throws RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException, InterruptedException, MQBrokerException {
        UnRegisterBrokerRequestHeader reqHeader = new UnRegisterBrokerRequestHeader();
        reqHeader.setBrokerAddr(brokerAddr);
        reqHeader.setBrokerId(brokerId);
        reqHeader.setBrokerName(brokerName);
        reqHeader.setClusterName(clusterName);
        RemotingCommand req = createRequestCommand(RequestCode.UNREGISTER_BROKER, reqHeader);

        RemotingCommand resp = this.remotingClient.invokeSync(namesrvAddr, req, 3000);
        assert resp != null;
        switch (resp.getCode()) {
            case ResponseCode.SUCCESS: {
                return;
            }
            default:
                break;
        }

        throw new MQBrokerException(resp.getCode(), resp.getRemark());
    }

    /**
     * 是否需要注册
     * @param clusterName
     * @param brokerAddr
     * @param brokerName
     * @param brokerId
     * @param topicConfigWrapper
     * @param timeoutMills
     * @return
     */
    public List<Boolean> needRegister(
        final String clusterName,
        final String brokerAddr,
        final String brokerName,
        final long brokerId,
        final TopicConfigSerializeWrapper topicConfigWrapper,
        final int timeoutMills) {
        final List<Boolean> changedList = new CopyOnWriteArrayList<>();
        List<String> nameServerAddressList = this.remotingClient.getNameServerAddressList();
        if (nameServerAddressList != null && nameServerAddressList.size() > 0) {
            final CountDownLatch countDownLatch = new CountDownLatch(nameServerAddressList.size());
            for (final String namesrvAddr : nameServerAddressList) {
                brokerOuterExecutor.execute(new Runnable() {
                    @Override
                    public void run() {
                        try {
                            QueryDataVersionRequestHeader requestHeader = new QueryDataVersionRequestHeader();
                            requestHeader.setBrokerAddr(brokerAddr);
                            requestHeader.setBrokerId(brokerId);
                            requestHeader.setBrokerName(brokerName);
                            requestHeader.setClusterName(clusterName);
                            RemotingCommand request = createRequestCommand(RequestCode.QUERY_DATA_VERSION, requestHeader);
                            request.setBody(topicConfigWrapper.getDataVersion().encode());
                            RemotingCommand response = remotingClient.invokeSync(namesrvAddr, request, timeoutMills);
                            DataVersion nameServerDataVersion = null;
                            Boolean changed = false;
                            switch (response.getCode()) {
                                case ResponseCode.SUCCESS: {
                                    QueryDataVersionResponseHeader queryDataVersionResponseHeader =
                                        (QueryDataVersionResponseHeader) response.decodeCommandCustomHeader(QueryDataVersionResponseHeader.class);
                                    changed = queryDataVersionResponseHeader.getChanged();
                                    byte[] body = response.getBody();
                                    if (body != null) {
                                        nameServerDataVersion = DataVersion.decode(body, DataVersion.class);
                                        if (!topicConfigWrapper.getDataVersion().equals(nameServerDataVersion)) {
                                            changed = true;
                                        }
                                    }
                                    if (changed == null || changed) {
                                        changedList.add(Boolean.TRUE);
                                    }
                                }
                                default:
                                    break;
                            }
                            log.warn("Query data version from name server {} OK,changed {}, broker {},name server {}", namesrvAddr, changed, topicConfigWrapper.getDataVersion(), nameServerDataVersion == null ? "" : nameServerDataVersion);
                        } catch (Exception e) {
                            changedList.add(Boolean.TRUE);
                            log.error("Query data version from name server {}  Exception, {}", namesrvAddr, e);
                        } finally {
                            countDownLatch.countDown();
                        }
                    }
                });

            }
            try {
                countDownLatch.await(timeoutMills, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                log.error("query dataversion from nameserver countDownLatch await Exception", e);
            }
        }
        return changedList;
    }


    //-------------给予slave进行使用------------//
    public TopicConfigSerializeWrapper getAllTopicConfig(
        final String addr) throws RemotingConnectException, RemotingSendRequestException,
        RemotingTimeoutException, InterruptedException, MQBrokerException {
        RemotingCommand request = createRequestCommand(RequestCode.GET_ALL_TOPIC_CONFIG, null);

        RemotingCommand response = this.remotingClient.invokeSync(MixAll.brokerVIPChannel(true, addr), request, 3000);
        assert response != null;
        switch (response.getCode()) {
            case ResponseCode.SUCCESS: {
                return TopicConfigSerializeWrapper.decode(response.getBody(), TopicConfigSerializeWrapper.class);
            }
            default:
                break;
        }

        throw new MQBrokerException(response.getCode(), response.getRemark());
    }

    public ConsumerOffsetSerializeWrapper getAllConsumerOffset(final String addr) throws InterruptedException, RemotingTimeoutException,
        RemotingSendRequestException, RemotingConnectException, MQBrokerException {
        RemotingCommand req = createRequestCommand(RequestCode.GET_ALL_CONSUMER_OFFSET, null);
        RemotingCommand res = this.remotingClient.invokeSync(addr, req, 3000);
        assert res != null;
        switch (res.getCode()) {
            case ResponseCode.SUCCESS: {
                return ConsumerOffsetSerializeWrapper.decode(res.getBody(), ConsumerOffsetSerializeWrapper.class);
            }
            default:
                break;
        }

        throw new MQBrokerException(res.getCode(), res.getRemark());
    }

    public String getAllDelayOffset(final String addr) throws InterruptedException, RemotingTimeoutException, RemotingSendRequestException,
        RemotingConnectException, MQBrokerException, UnsupportedEncodingException {
        RemotingCommand req = createRequestCommand(RequestCode.GET_ALL_DELAY_OFFSET, null);
        RemotingCommand resp = this.remotingClient.invokeSync(addr, req, 3000);
        assert resp != null;
        switch (resp.getCode()) {
            case ResponseCode.SUCCESS: {
                return new String(resp.getBody(), MixAll.DEFAULT_CHARSET);
            }
            default:
                break;
        }

        throw new MQBrokerException(resp.getCode(), resp.getRemark());
    }

    public SubscriptionGroupWrapper getAllSubscriptionGroupConfig(final String addr) throws InterruptedException, RemotingTimeoutException,
        RemotingSendRequestException, RemotingConnectException, MQBrokerException {
        RemotingCommand req = createRequestCommand(RequestCode.GET_ALL_SUBSCRIPTIONGROUP_CONFIG, null);
        RemotingCommand resp = this.remotingClient.invokeSync(addr, req, 3000);
        assert resp != null;
        switch (resp.getCode()) {
            case ResponseCode.SUCCESS: {
                return SubscriptionGroupWrapper.decode(resp.getBody(), SubscriptionGroupWrapper.class);
            }
            default:
                break;
        }

        throw new MQBrokerException(resp.getCode(), resp.getRemark());
    }

    public void registerRPCHook(RPCHook rpcHook) {
        remotingClient.registerRPCHook(rpcHook);
    }
}
