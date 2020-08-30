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
package org.apache.rocketmq.broker.subscription;

import java.util.Iterator;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.broker.BrokerPathConfigHelper;
import org.apache.rocketmq.common.ConfigManager;
import org.apache.rocketmq.common.DataVersion;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.common.subscription.SubscriptionGroupConfig;
import org.apache.rocketmq.remoting.protocol.RemotingSerializable;

import static org.apache.rocketmq.common.MixAll.isSysConsumerGroup;

/**
 * 订阅组管理器，存储了消费组名称和其对应的订阅组的配置，注意是订阅组配置而不是订阅的数据内容
 */
public class SubscriptionGroupManager extends ConfigManager {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.BROKER_LOGGER_NAME);

    //消费组名称和对应的订阅组配置
    private final ConcurrentMap<String, SubscriptionGroupConfig> subscriptionGroupTable = new ConcurrentHashMap<>(1024);
    //版本关系
    private final DataVersion dataVersion = new DataVersion();

    private transient BrokerController brokerController;

    public SubscriptionGroupManager() {
        this.init();
    }

    public SubscriptionGroupManager(BrokerController brokerController) {
        this.brokerController = brokerController;
        this.init();
    }

    /**
     * 构建系统自带一些订阅配置信息
     */
    private void init() {
        /**
         * 构建系统自带的消费组和相关的订阅配置信息
         */
        {
            SubscriptionGroupConfig subscriptionGroupConfig = new SubscriptionGroupConfig();
            subscriptionGroupConfig.setGroupName(MixAll.TOOLS_CONSUMER_GROUP); //TOOLS_CONSUMER
            this.subscriptionGroupTable.put(MixAll.TOOLS_CONSUMER_GROUP, subscriptionGroupConfig);
        }

        {
            SubscriptionGroupConfig subscriptionGroupConfig = new SubscriptionGroupConfig();
            subscriptionGroupConfig.setGroupName(MixAll.FILTERSRV_CONSUMER_GROUP);//FILTERSRV_CONSUMER
            this.subscriptionGroupTable.put(MixAll.FILTERSRV_CONSUMER_GROUP, subscriptionGroupConfig);
        }

        {
            SubscriptionGroupConfig subscriptionGroupConfig = new SubscriptionGroupConfig();
            subscriptionGroupConfig.setGroupName(MixAll.SELF_TEST_CONSUMER_GROUP); //SELF_TEST_C_GROUP
            this.subscriptionGroupTable.put(MixAll.SELF_TEST_CONSUMER_GROUP, subscriptionGroupConfig);
        }

        {
            SubscriptionGroupConfig subscriptionGroupConfig = new SubscriptionGroupConfig();
            subscriptionGroupConfig.setGroupName(MixAll.ONS_HTTP_PROXY_GROUP); //CID_ONS-HTTP-PROXY
            subscriptionGroupConfig.setConsumeBroadcastEnable(true);
            this.subscriptionGroupTable.put(MixAll.ONS_HTTP_PROXY_GROUP, subscriptionGroupConfig);
        }

        {
            SubscriptionGroupConfig subscriptionGroupConfig = new SubscriptionGroupConfig();
            subscriptionGroupConfig.setGroupName(MixAll.CID_ONSAPI_PULL_GROUP); //CID_ONSAPI_PULL
            subscriptionGroupConfig.setConsumeBroadcastEnable(true);
            this.subscriptionGroupTable.put(MixAll.CID_ONSAPI_PULL_GROUP, subscriptionGroupConfig);
        }

        {
            SubscriptionGroupConfig subscriptionGroupConfig = new SubscriptionGroupConfig();
            subscriptionGroupConfig.setGroupName(MixAll.CID_ONSAPI_PERMISSION_GROUP); //CID_ONSAPI_PERMISSION
            subscriptionGroupConfig.setConsumeBroadcastEnable(true);
            this.subscriptionGroupTable.put(MixAll.CID_ONSAPI_PERMISSION_GROUP, subscriptionGroupConfig);
        }

        {
            SubscriptionGroupConfig subscriptionGroupConfig = new SubscriptionGroupConfig();
            subscriptionGroupConfig.setGroupName(MixAll.CID_ONSAPI_OWNER_GROUP); //CID_ONSAPI_OWNER
            subscriptionGroupConfig.setConsumeBroadcastEnable(true);
            this.subscriptionGroupTable.put(MixAll.CID_ONSAPI_OWNER_GROUP, subscriptionGroupConfig);
        }
    }

    /**
     * 更新消费组对应的订阅组，包括新增
     * @param config
     */
    public void updateSubscriptionGroupConfig(final SubscriptionGroupConfig config) {
        SubscriptionGroupConfig old = this.subscriptionGroupTable.put(config.getGroupName(), config);
        if (old != null) {
            log.info("update subscription group config, old: {} new: {}", old, config);
        } else {
            log.info("create new subscription group, {}", config);
        }
        this.dataVersion.nextVersion();
        this.persist();
    }

    /**
     * 禁止被消费
     * @param groupName
     */
    public void disableConsume(final String groupName) {
        SubscriptionGroupConfig old = this.subscriptionGroupTable.get(groupName);
        if (old != null) {
            old.setConsumeEnable(false);
            this.dataVersion.nextVersion();
        }
    }

    /**
     * 查找消费组对应的订阅组，如果broker支持自动穿件订阅组，或者是系统，我们不需要通过手动的去创建，就能构建其中的配置
     * @param group
     * @return
     */
    public SubscriptionGroupConfig findSubscriptionGroupConfig(final String group) {
        SubscriptionGroupConfig config = this.subscriptionGroupTable.get(group);
        if (config != null){
            return config;
        }
        //线上，请关闭这个自动创建的选项，我们通过手动的方式去创建。
        //因此只有手动对某个订阅配置创建，最后才能支持消费
        if (brokerController.getBrokerConfig().isAutoCreateSubscriptionGroup() || isSysConsumerGroup(group)) { //自动创建订阅组或者是一个系统组，我们构建一个
            //支持自动的消费组对应的订阅配置信息，订阅配置信息并没有描述订阅内部的内容，而是描绘了配置
            config = new SubscriptionGroupConfig();
            config.setGroupName(group);
            SubscriptionGroupConfig preConfig = this.subscriptionGroupTable.putIfAbsent(group, config);
            if (null == preConfig) {
                log.info("auto create a subscription group, {}", config.toString());
            }
            //版本更新
            this.dataVersion.nextVersion();
            this.persist(); //持久化
        }
        return config;
    }

    @Override
    public String encode() {
        return this.encode(false);
    }

    @Override
    public String configFilePath() {
        return BrokerPathConfigHelper.getSubscriptionGroupPath(this.brokerController.getMessageStoreConfig()
            .getStorePathRootDir());
    }

    @Override
    public void decode(String jsonString) {
        if (jsonString != null) {
            //配置项反序列化
            SubscriptionGroupManager obj = RemotingSerializable.fromJson(jsonString, SubscriptionGroupManager.class);
            if (obj != null) {
                //copy对应订阅组结构数据
                this.subscriptionGroupTable.putAll(obj.subscriptionGroupTable);
                //copy下版本
                this.dataVersion.assignNewOne(obj.dataVersion);
                //打印一下
                this.printLoadDataWhenFirstBoot(obj);
            }
        }
    }

    public String encode(final boolean prettyFormat) {
        return RemotingSerializable.toJson(this, prettyFormat);
    }

    private void printLoadDataWhenFirstBoot(final SubscriptionGroupManager sgm) {
        Iterator<Entry<String, SubscriptionGroupConfig>> it = sgm.getSubscriptionGroupTable().entrySet().iterator();
        while (it.hasNext()) {
            Entry<String, SubscriptionGroupConfig> next = it.next();
            log.info("load exist subscription group, {}", next.getValue().toString());
        }
    }

    public ConcurrentMap<String, SubscriptionGroupConfig> getSubscriptionGroupTable() {
        return subscriptionGroupTable;
    }

    public DataVersion getDataVersion() {
        return dataVersion;
    }

    public void deleteSubscriptionGroupConfig(final String groupName) {
        SubscriptionGroupConfig old = this.subscriptionGroupTable.remove(groupName);
        if (old != null) {
            log.info("delete subscription group OK, subscription group:{}", old);
            this.dataVersion.nextVersion();
            this.persist();
        } else {
            log.warn("delete subscription group failed, subscription groupName: {} not exist", groupName);
        }
    }
}
