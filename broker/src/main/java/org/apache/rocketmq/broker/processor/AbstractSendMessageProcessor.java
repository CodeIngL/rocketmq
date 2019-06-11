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
package org.apache.rocketmq.broker.processor;

import io.netty.channel.ChannelHandlerContext;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.broker.mqtrace.SendMessageContext;
import org.apache.rocketmq.broker.mqtrace.SendMessageHook;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.common.TopicFilterType;
import org.apache.rocketmq.common.constant.DBMsgConstants;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.constant.PermName;
import org.apache.rocketmq.common.help.FAQUrl;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.common.message.MessageAccessor;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.protocol.RequestCode;
import org.apache.rocketmq.common.protocol.ResponseCode;
import org.apache.rocketmq.common.protocol.header.SendMessageRequestHeader;
import org.apache.rocketmq.common.protocol.header.SendMessageRequestHeaderV2;
import org.apache.rocketmq.common.protocol.header.SendMessageResponseHeader;
import org.apache.rocketmq.common.sysflag.MessageSysFlag;
import org.apache.rocketmq.common.sysflag.TopicSysFlag;
import org.apache.rocketmq.common.utils.ChannelUtil;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;
import org.apache.rocketmq.remoting.netty.NettyRequestProcessor;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.store.MessageExtBrokerInner;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.List;
import java.util.Map;
import java.util.Random;

import static org.apache.rocketmq.common.message.MessageConst.PROPERTY_MSG_REGION;
import static org.apache.rocketmq.common.message.MessageConst.PROPERTY_TRACE_SWITCH;
import static org.apache.rocketmq.common.message.MessageConst.PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX;
import static org.apache.rocketmq.common.message.MessageDecoder.string2messageProperties;
import static org.apache.rocketmq.common.protocol.RequestCode.SEND_BATCH_MESSAGE;
import static org.apache.rocketmq.common.protocol.RequestCode.SEND_MESSAGE;
import static org.apache.rocketmq.common.protocol.RequestCode.SEND_MESSAGE_V2;
import static org.apache.rocketmq.remoting.common.RemotingHelper.parseChannelRemoteAddr;

public abstract class AbstractSendMessageProcessor implements NettyRequestProcessor {
    protected static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.BROKER_LOGGER_NAME);

    protected final static int DLQ_NUMS_PER_GROUP = 1;
    protected final BrokerController brokerController;
    protected final Random random = new Random(System.currentTimeMillis());
    protected final SocketAddress storeHost;
    private List<SendMessageHook> sendMessageHookList;

    public AbstractSendMessageProcessor(final BrokerController brokerController) {
        this.brokerController = brokerController;
        this.storeHost =
            new InetSocketAddress(brokerController.getBrokerConfig().getBrokerIP1(), brokerController
                .getNettyServerConfig().getListenPort());
    }

    protected SendMessageContext buildMsgContext(ChannelHandlerContext ctx, SendMessageRequestHeader reqHeader) {
        if (!this.hasSendMessageHook()) {
            return null;
        }
        SendMessageContext context;
        context = new SendMessageContext();
        context.setProducerGroup(reqHeader.getProducerGroup());
        context.setTopic(reqHeader.getTopic());
        context.setMsgProps(reqHeader.getProperties());
        context.setBornHost(parseChannelRemoteAddr(ctx.channel()));
        context.setBrokerAddr(this.brokerController.getBrokerAddr());
        context.setBrokerRegionId(this.brokerController.getBrokerConfig().getRegionId());
        context.setBornTimeStamp(reqHeader.getBornTimestamp());

        Map<String, String> properties = string2messageProperties(reqHeader.getProperties());
        String uniqueKey = properties.get(PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX);
        properties.put(PROPERTY_MSG_REGION, this.brokerController.getBrokerConfig().getRegionId());
        properties.put(PROPERTY_TRACE_SWITCH, String.valueOf(this.brokerController.getBrokerConfig().isTraceOn()));
        reqHeader.setProperties(MessageDecoder.messageProperties2String(properties));

        if (uniqueKey == null) {
            uniqueKey = "";
        }
        context.setMsgUniqueKey(uniqueKey);
        return context;
    }

    public boolean hasSendMessageHook() {
        return sendMessageHookList != null && !this.sendMessageHookList.isEmpty();
    }

    protected MessageExtBrokerInner buildInnerMsg(final ChannelHandlerContext ctx,
        final SendMessageRequestHeader requestHeader, final byte[] body, TopicConfig topicConfig) {
        int queueIdInt = requestHeader.getQueueId();
        if (queueIdInt < 0) {
            queueIdInt = Math.abs(this.random.nextInt() % 99999999) % topicConfig.getWriteQueueNums();
        }
        int sysFlag = requestHeader.getSysFlag();

        if (TopicFilterType.MULTI_TAG == topicConfig.getTopicFilterType()) {
            sysFlag |= MessageSysFlag.MULTI_TAGS_FLAG;
        }

        MessageExtBrokerInner msgInner = new MessageExtBrokerInner();
        msgInner.setTopic(requestHeader.getTopic());
        msgInner.setBody(body);
        msgInner.setFlag(requestHeader.getFlag());
        MessageAccessor.setProperties(msgInner,
            string2messageProperties(requestHeader.getProperties()));
        msgInner.setPropertiesString(requestHeader.getProperties());
        msgInner.setTagsCode(MessageExtBrokerInner.tagsString2tagsCode(topicConfig.getTopicFilterType(),
            msgInner.getTags()));

        msgInner.setQueueId(queueIdInt);
        msgInner.setSysFlag(sysFlag);
        msgInner.setBornTimestamp(requestHeader.getBornTimestamp());
        msgInner.setBornHost(ctx.channel().remoteAddress());
        msgInner.setStoreHost(this.getStoreHost());
        msgInner.setReconsumeTimes(requestHeader.getReconsumeTimes() == null ? 0 : requestHeader
            .getReconsumeTimes());
        return msgInner;
    }

    public SocketAddress getStoreHost() {
        return storeHost;
    }

    protected RemotingCommand msgContentCheck(final ChannelHandlerContext ctx,
        final SendMessageRequestHeader requestHeader, RemotingCommand request,
        final RemotingCommand response) {
        if (requestHeader.getTopic().length() > Byte.MAX_VALUE) {
            log.warn("putMessage message topic length too long {}", requestHeader.getTopic().length());
            response.setCode(ResponseCode.MESSAGE_ILLEGAL);
            return response;
        }
        if (requestHeader.getProperties() != null && requestHeader.getProperties().length() > Short.MAX_VALUE) {
            log.warn("putMessage message properties length too long {}", requestHeader.getProperties().length());
            response.setCode(ResponseCode.MESSAGE_ILLEGAL);
            return response;
        }
        if (request.getBody().length > DBMsgConstants.MAX_BODY_SIZE) {
            log.warn(" topic {}  msg body size {}  from {}", requestHeader.getTopic(),
                request.getBody().length, ChannelUtil.getRemoteIp(ctx.channel()));
            response.setRemark("msg body must be less 64KB");
            response.setCode(ResponseCode.MESSAGE_ILLEGAL);
            return response;
        }
        return response;
    }

    /**
     * 消息检查
     * @param ctx
     * @param reqHeader
     * @param resp
     * @return
     */
    protected RemotingCommand msgCheck(final ChannelHandlerContext ctx,
        final SendMessageRequestHeader reqHeader, final RemotingCommand resp) {
        //broker节点的权限，不同broker节点的权限可能是不同
        if (!PermName.isWriteable(this.brokerController.getBrokerConfig().getBrokerPermission())
            && this.brokerController.getTopicConfigManager().isOrderTopic(reqHeader.getTopic())) {
            //该broker不具备写入能力
            resp.setCode(ResponseCode.NO_PERMISSION);
            resp.setRemark("the broker[" + this.brokerController.getBrokerConfig().getBrokerIP1()
                + "] sending message is forbidden");
            return resp;
        }
        //检查一下配置
        if (!this.brokerController.getTopicConfigManager().isTopicCanSendMessage(reqHeader.getTopic())) {
            String errorMsg = "the topic[" + reqHeader.getTopic() + "] is conflict with system reserved words.";
            log.warn(errorMsg);
            resp.setCode(ResponseCode.SYSTEM_ERROR);
            resp.setRemark(errorMsg);
            return resp;
        }

        //获得消息对应的topic的配置
        TopicConfig topicConfig =
            this.brokerController.getTopicConfigManager().selectTopicConfig(reqHeader.getTopic());
        if (null == topicConfig) {
            int topicSysFlag = 0;
            if (reqHeader.isUnitMode()) {
                if (reqHeader.getTopic().startsWith(MixAll.RETRY_GROUP_TOPIC_PREFIX)) {
                    topicSysFlag = TopicSysFlag.buildSysFlag(false, true);
                } else {
                    topicSysFlag = TopicSysFlag.buildSysFlag(true, false);
                }
            }

            log.warn("the topic {} not exist, producer: {}", reqHeader.getTopic(), ctx.channel().remoteAddress());
            topicConfig = this.brokerController.getTopicConfigManager().createTopicInSendMessageMethod(
                reqHeader.getTopic(),
                reqHeader.getDefaultTopic(),
                parseChannelRemoteAddr(ctx.channel()),
                reqHeader.getDefaultTopicQueueNums(), topicSysFlag);

            if (null == topicConfig) {
                if (reqHeader.getTopic().startsWith(MixAll.RETRY_GROUP_TOPIC_PREFIX)) {
                    topicConfig =
                        this.brokerController.getTopicConfigManager().createTopicInSendMessageBackMethod(
                            reqHeader.getTopic(), 1, PermName.PERM_WRITE | PermName.PERM_READ,
                            topicSysFlag);
                }
            }

            if (null == topicConfig) {
                resp.setCode(ResponseCode.TOPIC_NOT_EXIST);
                resp.setRemark("topic[" + reqHeader.getTopic() + "] not exist, apply first please!"
                    + FAQUrl.suggestTodo(FAQUrl.APPLY_TOPIC_URL));
                return resp;
            }
        }

        //获得消息的queueId
        int queueIdInt = reqHeader.getQueueId();
        //校验这个id是否合理
        int idValid = Math.max(topicConfig.getWriteQueueNums(), topicConfig.getReadQueueNums());
        if (queueIdInt >= idValid) {
            String errorInfo = String.format("request queueId[%d] is illegal, %s Producer: %s",
                queueIdInt,
                topicConfig.toString(),
                parseChannelRemoteAddr(ctx.channel()));

            log.warn(errorInfo);
            resp.setCode(ResponseCode.SYSTEM_ERROR);
            resp.setRemark(errorInfo);

            return resp;
        }
        return resp;
    }

    public void registerSendMessageHook(List<SendMessageHook> sendMessageHookList) {
        this.sendMessageHookList = sendMessageHookList;
    }

    protected void doResponse(ChannelHandlerContext ctx, RemotingCommand req, final RemotingCommand resp) {
        if (!req.isOnewayRPC()) {
            try {
                ctx.writeAndFlush(resp);
            } catch (Throwable e) {
                log.error("SendMessageProcessor process request over, but response failed", e);
                log.error(req.toString());
                log.error(resp.toString());
            }
        }
    }

    public void executeSendMessageHookBefore(final ChannelHandlerContext ctx, final RemotingCommand req, SendMessageContext context) {
        if (!hasSendMessageHook()) {
            return;
        }
        //存在hook钩子
        for (SendMessageHook hook : this.sendMessageHookList) {
            try {
                //请求头
                final SendMessageRequestHeader reqHeader = parseRequestHeader(req);

                if (null != reqHeader) {
                    //设置发送组
                    context.setProducerGroup(reqHeader.getProducerGroup());
                    //设置主题
                    context.setTopic(reqHeader.getTopic());
                    //设置消息体长
                    context.setBodyLength(req.getBody().length);
                    //设置消息相关的属性
                    context.setMsgProps(reqHeader.getProperties());
                    //设置
                    context.setBornHost(parseChannelRemoteAddr(ctx.channel()));
                    //设置broker的地址
                    context.setBrokerAddr(this.brokerController.getBrokerAddr());
                    //设置队列的id
                    context.setQueueId(reqHeader.getQueueId());
                }

                //执行钩子
                hook.sendMessageBefore(context);
                if (reqHeader != null) {
                    //将上下文的消息属性写入请求头中
                    reqHeader.setProperties(context.getMsgProps());
                }
            } catch (Throwable e) {
                // Ignore
            }
        }
    }

    /**
     * 提取发送消息的相关头部
     * note: this operation 总是返回一个新的header，这是存在相关的问题的
     * @param req
     * @return
     * @throws RemotingCommandException
     */
    protected SendMessageRequestHeader parseRequestHeader(RemotingCommand req) throws RemotingCommandException {

        SendMessageRequestHeaderV2 reqHeaderV2 = null;
        SendMessageRequestHeader reqHeader = null;
        switch (req.getCode()) {
            case SEND_BATCH_MESSAGE:
            case SEND_MESSAGE_V2: //批量发送，v2版本
                reqHeaderV2 = (SendMessageRequestHeaderV2) req.decodeCommandCustomHeader(SendMessageRequestHeaderV2.class);
            case SEND_MESSAGE: //单个发送
                if (null == reqHeaderV2) {//版本不是v2，直接操作
                    reqHeader = (SendMessageRequestHeader) req.decodeCommandCustomHeader(SendMessageRequestHeader.class);
                } else {//版本是v2转换以前的版本
                    reqHeader = SendMessageRequestHeaderV2.createSendMessageRequestHeaderV1(reqHeaderV2);
                }
            default:
                break;
        }
        return reqHeader;
    }

    public void executeSendMessageHookAfter(final RemotingCommand resp, final SendMessageContext context) {
        if (!hasSendMessageHook()){
            return;
        }
        for (SendMessageHook hook : this.sendMessageHookList) {
            try {
                if (resp != null) {
                    final SendMessageResponseHeader respHeader = (SendMessageResponseHeader) resp.readCustomHeader();
                    context.setMsgId(respHeader.getMsgId());
                    context.setQueueId(respHeader.getQueueId());
                    context.setQueueOffset(respHeader.getQueueOffset());
                    context.setCode(resp.getCode());
                    context.setErrorMsg(resp.getRemark());
                }
                hook.sendMessageAfter(context);
            } catch (Throwable e) {
                // Ignore
            }
        }
    }

    @Override
    public boolean rejectRequest() {
        return false;
    }
}
