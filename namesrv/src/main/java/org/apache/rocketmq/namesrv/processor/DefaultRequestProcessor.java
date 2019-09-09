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
package org.apache.rocketmq.namesrv.processor;

import io.netty.channel.ChannelHandlerContext;

import java.io.UnsupportedEncodingException;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.rocketmq.common.DataVersion;
import org.apache.rocketmq.common.MQVersion;
import org.apache.rocketmq.common.MQVersion.Version;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.help.FAQUrl;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.common.namesrv.NamesrvUtil;
import org.apache.rocketmq.common.namesrv.RegisterBrokerResult;
import org.apache.rocketmq.common.protocol.RequestCode;
import org.apache.rocketmq.common.protocol.ResponseCode;
import org.apache.rocketmq.common.protocol.body.RegisterBrokerBody;
import org.apache.rocketmq.common.protocol.body.TopicConfigSerializeWrapper;
import org.apache.rocketmq.common.protocol.header.GetTopicsByClusterRequestHeader;
import org.apache.rocketmq.common.protocol.header.namesrv.DeleteKVConfigRequestHeader;
import org.apache.rocketmq.common.protocol.header.namesrv.DeleteTopicInNamesrvRequestHeader;
import org.apache.rocketmq.common.protocol.header.namesrv.GetKVConfigRequestHeader;
import org.apache.rocketmq.common.protocol.header.namesrv.GetKVConfigResponseHeader;
import org.apache.rocketmq.common.protocol.header.namesrv.GetKVListByNamespaceRequestHeader;
import org.apache.rocketmq.common.protocol.header.namesrv.GetRouteInfoRequestHeader;
import org.apache.rocketmq.common.protocol.header.namesrv.PutKVConfigRequestHeader;
import org.apache.rocketmq.common.protocol.header.namesrv.QueryDataVersionRequestHeader;
import org.apache.rocketmq.common.protocol.header.namesrv.QueryDataVersionResponseHeader;
import org.apache.rocketmq.common.protocol.header.namesrv.RegisterBrokerRequestHeader;
import org.apache.rocketmq.common.protocol.header.namesrv.RegisterBrokerResponseHeader;
import org.apache.rocketmq.common.protocol.header.namesrv.UnRegisterBrokerRequestHeader;
import org.apache.rocketmq.common.protocol.header.namesrv.WipeWritePermOfBrokerRequestHeader;
import org.apache.rocketmq.common.protocol.header.namesrv.WipeWritePermOfBrokerResponseHeader;
import org.apache.rocketmq.common.protocol.route.TopicRouteData;
import org.apache.rocketmq.namesrv.NamesrvController;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;
import org.apache.rocketmq.remoting.netty.NettyRequestProcessor;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;

import static org.apache.rocketmq.common.protocol.RequestCode.*;
import static org.apache.rocketmq.common.protocol.ResponseCode.TOPIC_NOT_EXIST;
import static org.apache.rocketmq.remoting.protocol.RemotingCommand.createResponseCommand;

/**
 * K，V 系统增删改查功能;
 * broker的注册注销;topic的增删改查;
 * 获取集群信息,包括broker列表和broker的集群名称和broker组名称;
 */
public class DefaultRequestProcessor implements NettyRequestProcessor {
    private static InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.NAMESRV_LOGGER_NAME);

    protected final NamesrvController namesrvController;

    public DefaultRequestProcessor(NamesrvController namesrvController) {
        this.namesrvController = namesrvController;
    }

    /**
     * 转换到KVConfigManager和RouteInfoManager进行处理
     *
     * @param ctx
     * @param req
     * @return
     * @throws RemotingCommandException
     */
    @Override
    public RemotingCommand processRequest(ChannelHandlerContext ctx,
                                          RemotingCommand req) throws RemotingCommandException {

        if (ctx != null) {
            log.debug("receive request, {} {} {}", req.getCode(), RemotingHelper.parseChannelRemoteAddr(ctx.channel()), req);
        }


        switch (req.getCode()) {
            case PUT_KV_CONFIG:
                return this.putKVConfig(ctx, req);
            case GET_KV_CONFIG:
                return this.getKVConfig(ctx, req);
            case DELETE_KV_CONFIG:
                return this.deleteKVConfig(ctx, req);

            case QUERY_DATA_VERSION:
                return queryBrokerTopicConfig(ctx, req);
            case REGISTER_BROKER: //注册broker，consumer和producer不需要注册到他上面，仅提供了对broker的注册
                Version brokerVersion = MQVersion.value2Version(req.getVersion());
                if (brokerVersion.ordinal() >= MQVersion.Version.V3_0_11.ordinal()) {
                    return this.registerBrokerWithFilterServer(ctx, req);//3.0.11以上存在filterServer
                } else {
                    return this.registerBroker(ctx, req); //一般注册
                }
            case UNREGISTER_BROKER:
                return this.unregisterBroker(ctx, req);

            case GET_ROUTEINTO_BY_TOPIC: //比较核心的请求，通过topic获得相关的路由信息
                return this.getRouteInfoByTopic(ctx, req);
            case GET_BROKER_CLUSTER_INFO:
                return this.getBrokerClusterInfo(ctx, req);
            case WIPE_WRITE_PERM_OF_BROKER:
                return this.wipeWritePermOfBroker(ctx, req);
            case GET_ALL_TOPIC_LIST_FROM_NAMESERVER:
                return getAllTopicListFromNameserver(ctx, req);
            case DELETE_TOPIC_IN_NAMESRV:
                return deleteTopicInNamesrv(ctx, req);
            case GET_KVLIST_BY_NAMESPACE:
                return this.getKVListByNamespace(ctx, req);
            case GET_TOPICS_BY_CLUSTER:
                return this.getTopicsByCluster(ctx, req);
            case GET_SYSTEM_TOPIC_LIST_FROM_NS:
                return this.getSystemTopicListFromNs(ctx, req);
            case GET_UNIT_TOPIC_LIST:
                return this.getUnitTopicList(ctx, req);
            case GET_HAS_UNIT_SUB_TOPIC_LIST:
                return this.getHasUnitSubTopicList(ctx, req);
            case GET_HAS_UNIT_SUB_UNUNIT_TOPIC_LIST:
                return this.getHasUnitSubUnUnitTopicList(ctx, req);
            case UPDATE_NAMESRV_CONFIG:
                return this.updateConfig(ctx, req);
            case GET_NAMESRV_CONFIG:
                return this.getConfig(ctx, req);
            default:
                break;
        }
        return null;
    }

    @Override
    public boolean rejectRequest() {
        return false;
    }

    public RemotingCommand putKVConfig(ChannelHandlerContext ctx,
                                       RemotingCommand request) throws RemotingCommandException {
        final RemotingCommand response = createResponseCommand(null);
        final PutKVConfigRequestHeader requestHeader =
                (PutKVConfigRequestHeader) request.decodeCommandCustomHeader(PutKVConfigRequestHeader.class);

        this.namesrvController.getKvConfigManager().putKVConfig(
                requestHeader.getNamespace(),
                requestHeader.getKey(),
                requestHeader.getValue()
        );

        response.setCode(ResponseCode.SUCCESS);
        response.setRemark(null);
        return response;
    }

    public RemotingCommand getKVConfig(ChannelHandlerContext ctx,
                                       RemotingCommand request) throws RemotingCommandException {
        final RemotingCommand resp = createResponseCommand(GetKVConfigResponseHeader.class);
        final GetKVConfigResponseHeader respHeader = (GetKVConfigResponseHeader) resp.readCustomHeader();
        final GetKVConfigRequestHeader reqHeader =
                (GetKVConfigRequestHeader) request.decodeCommandCustomHeader(GetKVConfigRequestHeader.class);

        String value = this.namesrvController.getKvConfigManager().getKVConfig(reqHeader.getNamespace(), reqHeader.getKey());

        if (value != null) {
            respHeader.setValue(value);
            resp.setCode(ResponseCode.SUCCESS);
            resp.setRemark(null);
            return resp;
        }

        resp.setCode(ResponseCode.QUERY_NOT_FOUND);
        resp.setRemark("No config item, Namespace: " + reqHeader.getNamespace() + " Key: " + reqHeader.getKey());
        return resp;
    }

    public RemotingCommand deleteKVConfig(ChannelHandlerContext ctx,
                                          RemotingCommand request) throws RemotingCommandException {
        final RemotingCommand response = createResponseCommand(null);
        final DeleteKVConfigRequestHeader requestHeader =
                (DeleteKVConfigRequestHeader) request.decodeCommandCustomHeader(DeleteKVConfigRequestHeader.class);

        this.namesrvController.getKvConfigManager().deleteKVConfig(
                requestHeader.getNamespace(),
                requestHeader.getKey()
        );

        response.setCode(ResponseCode.SUCCESS);
        response.setRemark(null);
        return response;
    }

    /**
     * 3.1.1以上版本支持
     * @param ctx
     * @param req
     * @return
     * @throws RemotingCommandException
     */
    public RemotingCommand registerBrokerWithFilterServer(ChannelHandlerContext ctx, RemotingCommand req)
            throws RemotingCommandException {
        final RemotingCommand resp = createResponseCommand(RegisterBrokerResponseHeader.class);
        final RegisterBrokerResponseHeader respHeader = (RegisterBrokerResponseHeader) resp.readCustomHeader();
        final RegisterBrokerRequestHeader reqHeader =
                (RegisterBrokerRequestHeader) req.decodeCommandCustomHeader(RegisterBrokerRequestHeader.class);

        if (!checksum(ctx, req, reqHeader)) {
            resp.setCode(ResponseCode.SYSTEM_ERROR);
            resp.setRemark("crc32 not match");
            return resp;
        }

        RegisterBrokerBody registerBrokerBody = new RegisterBrokerBody();

        if (req.getBody() != null) {
            try {
                registerBrokerBody = RegisterBrokerBody.decode(req.getBody(), reqHeader.isCompressed());
            } catch (Exception e) {
                throw new RemotingCommandException("Failed to decode RegisterBrokerBody", e);
            }
        } else {
            registerBrokerBody.getTopicConfigSerializeWrapper().getDataVersion().setCounter(new AtomicLong(0));
            registerBrokerBody.getTopicConfigSerializeWrapper().getDataVersion().setTimestamp(0);
        }

        RegisterBrokerResult result = this.namesrvController.getRouteInfoManager().registerBroker(
                reqHeader.getClusterName(),
                reqHeader.getBrokerAddr(),
                reqHeader.getBrokerName(),
                reqHeader.getBrokerId(),
                reqHeader.getHaServerAddr(),
                registerBrokerBody.getTopicConfigSerializeWrapper(),
                registerBrokerBody.getFilterServerList(),
                ctx.channel());

        respHeader.setHaServerAddr(result.getHaServerAddr());
        respHeader.setMasterAddr(result.getMasterAddr());

        byte[] jsonValue = this.namesrvController.getKvConfigManager().getKVListByNamespace(NamesrvUtil.NAMESPACE_ORDER_TOPIC_CONFIG);
        resp.setBody(jsonValue);

        resp.setCode(ResponseCode.SUCCESS);
        resp.setRemark(null);
        return resp;
    }

    private boolean checksum(ChannelHandlerContext ctx, RemotingCommand request,
                             RegisterBrokerRequestHeader requestHeader) {
        if (requestHeader.getBodyCrc32() != 0) {
            final int crc32 = UtilAll.crc32(request.getBody());
            if (crc32 != requestHeader.getBodyCrc32()) {
                log.warn(String.format("receive registerBroker request,crc32 not match,from %s",
                        RemotingHelper.parseChannelRemoteAddr(ctx.channel())));
                return false;
            }
        }
        return true;
    }

    public RemotingCommand queryBrokerTopicConfig(ChannelHandlerContext ctx,
                                                  RemotingCommand request) throws RemotingCommandException {
        final RemotingCommand response = createResponseCommand(QueryDataVersionResponseHeader.class);
        final QueryDataVersionResponseHeader responseHeader = (QueryDataVersionResponseHeader) response.readCustomHeader();
        final QueryDataVersionRequestHeader requestHeader =
                (QueryDataVersionRequestHeader) request.decodeCommandCustomHeader(QueryDataVersionRequestHeader.class);
        DataVersion dataVersion = DataVersion.decode(request.getBody(), DataVersion.class);

        Boolean changed = this.namesrvController.getRouteInfoManager().isBrokerTopicConfigChanged(requestHeader.getBrokerAddr(), dataVersion);
        if (!changed) {
            this.namesrvController.getRouteInfoManager().updateBrokerInfoUpdateTimestamp(requestHeader.getBrokerAddr());
        }

        DataVersion nameSeverDataVersion = this.namesrvController.getRouteInfoManager().queryBrokerTopicConfig(requestHeader.getBrokerAddr());
        response.setCode(ResponseCode.SUCCESS);
        response.setRemark(null);

        if (nameSeverDataVersion != null) {
            response.setBody(nameSeverDataVersion.encode());
        }
        responseHeader.setChanged(changed);
        return response;
    }

    /**
     * 注册broker
     *
     * @param ctx
     * @param req
     * @return
     * @throws RemotingCommandException
     */
    public RemotingCommand registerBroker(ChannelHandlerContext ctx, RemotingCommand req) throws RemotingCommandException {
        final RemotingCommand resp = createResponseCommand(RegisterBrokerResponseHeader.class);
        final RegisterBrokerResponseHeader respHeader = (RegisterBrokerResponseHeader) resp.readCustomHeader();
        final RegisterBrokerRequestHeader reqHeader = (RegisterBrokerRequestHeader) req.decodeCommandCustomHeader(RegisterBrokerRequestHeader.class);

        if (!checksum(ctx, req, reqHeader)) {
            resp.setCode(ResponseCode.SYSTEM_ERROR);
            resp.setRemark("crc32 not match");
            return resp;
        }

        TopicConfigSerializeWrapper topicConfigWrapper;
        if (req.getBody() != null) {
            topicConfigWrapper = TopicConfigSerializeWrapper.decode(req.getBody(), TopicConfigSerializeWrapper.class);
        } else {
            topicConfigWrapper = new TopicConfigSerializeWrapper();
            topicConfigWrapper.getDataVersion().setCounter(new AtomicLong(0));
            topicConfigWrapper.getDataVersion().setTimestamp(0);
        }

        RegisterBrokerResult result = this.namesrvController.getRouteInfoManager().registerBroker(
                reqHeader.getClusterName(),
                reqHeader.getBrokerAddr(),
                reqHeader.getBrokerName(),
                reqHeader.getBrokerId(),
                reqHeader.getHaServerAddr(),
                topicConfigWrapper,
                null,
                ctx.channel()
        );

        respHeader.setHaServerAddr(result.getHaServerAddr());
        respHeader.setMasterAddr(result.getMasterAddr());

        byte[] jsonValue = this.namesrvController.getKvConfigManager().getKVListByNamespace(NamesrvUtil.NAMESPACE_ORDER_TOPIC_CONFIG);
        resp.setBody(jsonValue);
        resp.setCode(ResponseCode.SUCCESS);
        resp.setRemark(null);
        return resp;
    }

    public RemotingCommand unregisterBroker(ChannelHandlerContext ctx,
                                            RemotingCommand req) throws RemotingCommandException {
        final RemotingCommand resp = createResponseCommand(null);
        final UnRegisterBrokerRequestHeader reqHeader = (UnRegisterBrokerRequestHeader) req.decodeCommandCustomHeader(UnRegisterBrokerRequestHeader.class);

        this.namesrvController.getRouteInfoManager().unregisterBroker(
                reqHeader.getClusterName(),
                reqHeader.getBrokerAddr(),
                reqHeader.getBrokerName(),
                reqHeader.getBrokerId());

        resp.setCode(ResponseCode.SUCCESS);
        resp.setRemark(null);
        return resp;
    }

    /**
     * 获得topic的路由信息，从nameServer中获得相关的信息
     *
     * @param ctx
     * @param req
     * @return
     * @throws RemotingCommandException
     */
    public RemotingCommand getRouteInfoByTopic(ChannelHandlerContext ctx, RemotingCommand req) throws RemotingCommandException {
        final RemotingCommand resp = createResponseCommand(null);
        final GetRouteInfoRequestHeader reqHeader = (GetRouteInfoRequestHeader) req.decodeCommandCustomHeader(GetRouteInfoRequestHeader.class);

        //根据topic选择相关的路由信息
        TopicRouteData topicRouteData = this.namesrvController.getRouteInfoManager().pickupTopicRouteData(reqHeader.getTopic());

        if (topicRouteData != null) {
            if (this.namesrvController.getNamesrvConfig().isOrderMessageEnable()) { //nameserver配置支持顺序消费
                String orderTopicConf = this.namesrvController.getKvConfigManager().getKVConfig(NamesrvUtil.NAMESPACE_ORDER_TOPIC_CONFIG, reqHeader.getTopic());
                topicRouteData.setOrderTopicConf(orderTopicConf);
            }

            byte[] content = topicRouteData.encode();
            resp.setBody(content);
            resp.setCode(ResponseCode.SUCCESS);
            resp.setRemark(null);
            return resp;
        }

        resp.setCode(TOPIC_NOT_EXIST);
        resp.setRemark("No topic route info in name server for the topic: " + reqHeader.getTopic() + FAQUrl.suggestTodo(FAQUrl.APPLY_TOPIC_URL));
        return resp;
    }

    private RemotingCommand getBrokerClusterInfo(ChannelHandlerContext ctx, RemotingCommand request) {
        final RemotingCommand response = createResponseCommand(null);

        byte[] content = this.namesrvController.getRouteInfoManager().getAllClusterInfo();
        response.setBody(content);

        response.setCode(ResponseCode.SUCCESS);
        response.setRemark(null);
        return response;
    }

    private RemotingCommand wipeWritePermOfBroker(ChannelHandlerContext ctx,
                                                  RemotingCommand request) throws RemotingCommandException {
        final RemotingCommand response = createResponseCommand(WipeWritePermOfBrokerResponseHeader.class);
        final WipeWritePermOfBrokerResponseHeader responseHeader = (WipeWritePermOfBrokerResponseHeader) response.readCustomHeader();
        final WipeWritePermOfBrokerRequestHeader requestHeader =
                (WipeWritePermOfBrokerRequestHeader) request.decodeCommandCustomHeader(WipeWritePermOfBrokerRequestHeader.class);

        int wipeTopicCnt = this.namesrvController.getRouteInfoManager().wipeWritePermOfBrokerByLock(requestHeader.getBrokerName());

        log.info("wipe write perm of broker[{}], client: {}, {}",
                requestHeader.getBrokerName(),
                RemotingHelper.parseChannelRemoteAddr(ctx.channel()),
                wipeTopicCnt);

        responseHeader.setWipeTopicCount(wipeTopicCnt);
        response.setCode(ResponseCode.SUCCESS);
        response.setRemark(null);
        return response;
    }

    private RemotingCommand getAllTopicListFromNameserver(ChannelHandlerContext ctx, RemotingCommand request) {
        final RemotingCommand response = createResponseCommand(null);

        byte[] body = this.namesrvController.getRouteInfoManager().getAllTopicList();

        response.setBody(body);
        response.setCode(ResponseCode.SUCCESS);
        response.setRemark(null);
        return response;
    }

    private RemotingCommand deleteTopicInNamesrv(ChannelHandlerContext ctx,
                                                 RemotingCommand request) throws RemotingCommandException {
        final RemotingCommand response = createResponseCommand(null);
        final DeleteTopicInNamesrvRequestHeader requestHeader =
                (DeleteTopicInNamesrvRequestHeader) request.decodeCommandCustomHeader(DeleteTopicInNamesrvRequestHeader.class);

        this.namesrvController.getRouteInfoManager().deleteTopic(requestHeader.getTopic());

        response.setCode(ResponseCode.SUCCESS);
        response.setRemark(null);
        return response;
    }

    private RemotingCommand getKVListByNamespace(ChannelHandlerContext ctx,
                                                 RemotingCommand request) throws RemotingCommandException {
        final RemotingCommand resp = createResponseCommand(null);
        final GetKVListByNamespaceRequestHeader requestHeader =
                (GetKVListByNamespaceRequestHeader) request.decodeCommandCustomHeader(GetKVListByNamespaceRequestHeader.class);

        byte[] jsonValue = this.namesrvController.getKvConfigManager().getKVListByNamespace(
                requestHeader.getNamespace());
        if (null != jsonValue) {
            resp.setBody(jsonValue);
            resp.setCode(ResponseCode.SUCCESS);
            resp.setRemark(null);
            return resp;
        }

        resp.setCode(ResponseCode.QUERY_NOT_FOUND);
        resp.setRemark("No config item, Namespace: " + requestHeader.getNamespace());
        return resp;
    }

    private RemotingCommand getTopicsByCluster(ChannelHandlerContext ctx,
                                               RemotingCommand request) throws RemotingCommandException {
        final RemotingCommand resp = createResponseCommand(null);
        final GetTopicsByClusterRequestHeader reqHeader =
                (GetTopicsByClusterRequestHeader) request.decodeCommandCustomHeader(GetTopicsByClusterRequestHeader.class);

        byte[] body = this.namesrvController.getRouteInfoManager().getTopicsByCluster(reqHeader.getCluster());

        resp.setBody(body);
        resp.setCode(ResponseCode.SUCCESS);
        resp.setRemark(null);
        return resp;
    }

    private RemotingCommand getSystemTopicListFromNs(ChannelHandlerContext ctx,
                                                     RemotingCommand request) throws RemotingCommandException {
        final RemotingCommand resp = createResponseCommand(null);

        byte[] body = this.namesrvController.getRouteInfoManager().getSystemTopicList();

        resp.setBody(body);
        resp.setCode(ResponseCode.SUCCESS);
        resp.setRemark(null);
        return resp;
    }

    private RemotingCommand getUnitTopicList(ChannelHandlerContext ctx,
                                             RemotingCommand request) throws RemotingCommandException {
        final RemotingCommand resp = createResponseCommand(null);

        byte[] body = this.namesrvController.getRouteInfoManager().getUnitTopics();

        resp.setBody(body);
        resp.setCode(ResponseCode.SUCCESS);
        resp.setRemark(null);
        return resp;
    }

    private RemotingCommand getHasUnitSubTopicList(ChannelHandlerContext ctx,
                                                   RemotingCommand request) throws RemotingCommandException {
        final RemotingCommand response = createResponseCommand(null);

        byte[] body = this.namesrvController.getRouteInfoManager().getHasUnitSubTopicList();

        response.setBody(body);
        response.setCode(ResponseCode.SUCCESS);
        response.setRemark(null);
        return response;
    }

    private RemotingCommand getHasUnitSubUnUnitTopicList(ChannelHandlerContext ctx, RemotingCommand request)
            throws RemotingCommandException {
        final RemotingCommand resp = createResponseCommand(null);

        byte[] body = this.namesrvController.getRouteInfoManager().getHasUnitSubUnUnitTopicList();

        resp.setBody(body);
        resp.setCode(ResponseCode.SUCCESS);
        resp.setRemark(null);
        return resp;
    }

    private RemotingCommand updateConfig(ChannelHandlerContext ctx, RemotingCommand request) {
        log.info("updateConfig called by {}", RemotingHelper.parseChannelRemoteAddr(ctx.channel()));

        final RemotingCommand response = createResponseCommand(null);

        byte[] body = request.getBody();
        if (body != null) {
            String bodyStr;
            try {
                bodyStr = new String(body, MixAll.DEFAULT_CHARSET);
            } catch (UnsupportedEncodingException e) {
                log.error("updateConfig byte array to string error: ", e);
                response.setCode(ResponseCode.SYSTEM_ERROR);
                response.setRemark("UnsupportedEncodingException " + e);
                return response;
            }

            if (bodyStr == null) {
                log.error("updateConfig get null body!");
                response.setCode(ResponseCode.SYSTEM_ERROR);
                response.setRemark("string2Properties error");
                return response;
            }

            Properties properties = MixAll.string2Properties(bodyStr);
            if (properties == null) {
                log.error("updateConfig MixAll.string2Properties error {}", bodyStr);
                response.setCode(ResponseCode.SYSTEM_ERROR);
                response.setRemark("string2Properties error");
                return response;
            }

            this.namesrvController.getConfiguration().update(properties);
        }

        response.setCode(ResponseCode.SUCCESS);
        response.setRemark(null);
        return response;
    }

    private RemotingCommand getConfig(ChannelHandlerContext ctx, RemotingCommand request) {
        final RemotingCommand response = createResponseCommand(null);

        String content = this.namesrvController.getConfiguration().getAllConfigsFormatString();
        if (content != null && content.length() > 0) {
            try {
                response.setBody(content.getBytes(MixAll.DEFAULT_CHARSET));
            } catch (UnsupportedEncodingException e) {
                log.error("getConfig error, ", e);
                response.setCode(ResponseCode.SYSTEM_ERROR);
                response.setRemark("UnsupportedEncodingException " + e);
                return response;
            }
        }

        response.setCode(ResponseCode.SUCCESS);
        response.setRemark(null);
        return response;
    }

}
