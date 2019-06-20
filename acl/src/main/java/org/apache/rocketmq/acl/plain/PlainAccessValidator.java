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
package org.apache.rocketmq.acl.plain;

import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import org.apache.rocketmq.acl.AccessResource;
import org.apache.rocketmq.acl.AccessValidator;
import org.apache.rocketmq.acl.common.AclException;
import org.apache.rocketmq.acl.common.AclUtils;
import org.apache.rocketmq.acl.common.Permission;
import org.apache.rocketmq.acl.common.SessionCredentials;
import org.apache.rocketmq.common.protocol.RequestCode;
import org.apache.rocketmq.common.protocol.header.GetConsumerListByGroupRequestHeader;
import org.apache.rocketmq.common.protocol.header.UnregisterClientRequestHeader;
import org.apache.rocketmq.common.protocol.header.UpdateConsumerOffsetRequestHeader;
import org.apache.rocketmq.common.protocol.heartbeat.ConsumerData;
import org.apache.rocketmq.common.protocol.heartbeat.HeartbeatData;
import org.apache.rocketmq.common.protocol.heartbeat.SubscriptionData;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;

import static org.apache.rocketmq.acl.common.Permission.PUB;
import static org.apache.rocketmq.acl.common.Permission.SUB;
import static org.apache.rocketmq.acl.plain.PlainAccessResource.getRetryTopic;
import static org.apache.rocketmq.common.protocol.RequestCode.*;

public class PlainAccessValidator implements AccessValidator {

    private PlainPermissionLoader aclPlugEngine;

    public PlainAccessValidator() {
        aclPlugEngine = new PlainPermissionLoader();
    }

    @Override
    public AccessResource parse(RemotingCommand req, String remoteAddr) {
        PlainAccessResource accessResource = new PlainAccessResource();
        if (remoteAddr != null && remoteAddr.contains(":")) {
            accessResource.setWhiteRemoteAddress(remoteAddr.split(":")[0]);
        } else {
            accessResource.setWhiteRemoteAddress(remoteAddr);
        }

        if (req.getExtFields() == null) {
            throw new AclException("request's extFields value is null");
        }
        
        accessResource.setRequestCode(req.getCode());
        accessResource.setAccessKey(req.getExtFields().get(SessionCredentials.ACCESS_KEY));
        accessResource.setSignature(req.getExtFields().get(SessionCredentials.SIGNATURE));
        accessResource.setSecretToken(req.getExtFields().get(SessionCredentials.SECURITY_TOKEN));

        try {
            switch (req.getCode()) {
                case SEND_MESSAGE:
                    accessResource.addResourceAndPerm(req.getExtFields().get("topic"), PUB);
                    break;
                case SEND_MESSAGE_V2:
                    accessResource.addResourceAndPerm(req.getExtFields().get("b"), PUB);
                    break;
                case CONSUMER_SEND_MSG_BACK:
                    accessResource.addResourceAndPerm(req.getExtFields().get("originTopic"), PUB);
                    accessResource.addResourceAndPerm(getRetryTopic(req.getExtFields().get("group")), SUB);
                    break;
                case PULL_MESSAGE:
                    accessResource.addResourceAndPerm(req.getExtFields().get("topic"), SUB);
                    accessResource.addResourceAndPerm(getRetryTopic(req.getExtFields().get("consumerGroup")), SUB);
                    break;
                case QUERY_MESSAGE:
                    accessResource.addResourceAndPerm(req.getExtFields().get("topic"), SUB);
                    break;
                case HEART_BEAT:
                    HeartbeatData heartbeatData = HeartbeatData.decode(req.getBody(), HeartbeatData.class);
                    for (ConsumerData data : heartbeatData.getConsumerDataSet()) {
                        accessResource.addResourceAndPerm(getRetryTopic(data.getGroupName()), SUB);
                        for (SubscriptionData subscriptionData : data.getSubscriptionDataSet()) {
                            accessResource.addResourceAndPerm(subscriptionData.getTopic(), SUB);
                        }
                    }
                    break;
                case UNREGISTER_CLIENT:
                    final UnregisterClientRequestHeader reqHeader = (UnregisterClientRequestHeader) req.decodeCommandCustomHeader(UnregisterClientRequestHeader.class);
                    accessResource.addResourceAndPerm(getRetryTopic(reqHeader.getConsumerGroup()), SUB);
                    break;
                case GET_CONSUMER_LIST_BY_GROUP:
                    final GetConsumerListByGroupRequestHeader requestHeader = (GetConsumerListByGroupRequestHeader) req
                            .decodeCommandCustomHeader(GetConsumerListByGroupRequestHeader.class);
                    accessResource.addResourceAndPerm(getRetryTopic(requestHeader.getConsumerGroup()), SUB);
                    break;
                case UPDATE_CONSUMER_OFFSET:
                    final UpdateConsumerOffsetRequestHeader updateConsumerOffsetRequestHeader =
                        (UpdateConsumerOffsetRequestHeader) req
                            .decodeCommandCustomHeader(UpdateConsumerOffsetRequestHeader.class);
                    accessResource.addResourceAndPerm(getRetryTopic(updateConsumerOffsetRequestHeader.getConsumerGroup()), SUB);
                    accessResource.addResourceAndPerm(updateConsumerOffsetRequestHeader.getTopic(), SUB);
                    break;
                default:
                    break;

            }
        } catch (Throwable t) {
            throw new AclException(t.getMessage(), t);
        }
        // Content
        SortedMap<String, String> map = new TreeMap<String, String>();
        for (Map.Entry<String, String> entry : req.getExtFields().entrySet()) {
            if (!SessionCredentials.SIGNATURE.equals(entry.getKey())) {
                map.put(entry.getKey(), entry.getValue());
            }
        }
        accessResource.setContent(AclUtils.combineRequestContent(req, map));
        return accessResource;
    }

    @Override
    public void validate(AccessResource accessResource) {
        aclPlugEngine.validate((PlainAccessResource) accessResource);
    }

}
