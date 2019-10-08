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

/**
 * $Id: HeartbeatData.java 1835 2013-05-16 02:00:50Z vintagewang@apache.org $
 */
package org.apache.rocketmq.common.protocol.heartbeat;

import java.util.HashSet;
import java.util.Set;
import org.apache.rocketmq.remoting.protocol.RemotingSerializable;

/**
 * 心跳数据
 * 保存了客户端的Id
 * 保存了客户端提交上来关于生产者的信息
 * 保存了客户端提交上来的关于消费者的信息
 */
public class HeartbeatData extends RemotingSerializable {
    //客户端的ID
    private String clientID;
    //客户端生产者数据
    private Set<ProducerData> producerDataSet = new HashSet<ProducerData>();
    //客户端消费者数据
    private Set<ConsumerData> consumerDataSet = new HashSet<ConsumerData>();

    public String getClientID() {
        return clientID;
    }

    public void setClientID(String clientID) {
        this.clientID = clientID;
    }

    public Set<ProducerData> getProducerDataSet() {
        return producerDataSet;
    }

    public void setProducerDataSet(Set<ProducerData> producerDataSet) {
        this.producerDataSet = producerDataSet;
    }

    public Set<ConsumerData> getConsumerDataSet() {
        return consumerDataSet;
    }

    public void setConsumerDataSet(Set<ConsumerData> consumerDataSet) {
        this.consumerDataSet = consumerDataSet;
    }

    @Override
    public String toString() {
        return "HeartbeatData [clientID=" + clientID + ", producerDataSet=" + producerDataSet + ", consumerDataSet=" + consumerDataSet + "]";
    }
}
