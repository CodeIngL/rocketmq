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

package org.apache.rocketmq.common.protocol;

import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.common.protocol.header.namesrv.RegisterBrokerRequestHeader;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;

import static org.apache.rocketmq.remoting.protocol.RemotingCommand.createRequestCommand;

/**
 * mq辅助类，用于将一个broker地址注册到指定的nameServer上
 */
public class MQProtosHelper {

    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.COMMON_LOGGER_NAME);

    public static boolean registerBrokerToNameServer(final String nsaddr, final String brokerAddr, final long timeoutMillis) {

        RegisterBrokerRequestHeader reqHeader = new RegisterBrokerRequestHeader();
        reqHeader.setBrokerAddr(brokerAddr);

        RemotingCommand req = createRequestCommand(RequestCode.REGISTER_BROKER, reqHeader);

        try {
            RemotingCommand response = RemotingHelper.invokeSync(nsaddr, req, timeoutMillis);
            if (response != null) {
                return ResponseCode.SUCCESS == response.getCode();
            }
        } catch (Exception e) {
            log.error("Failed to register broker", e);
        }

        return false;
    }
}
