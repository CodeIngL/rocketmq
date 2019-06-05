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
package org.apache.rocketmq.broker;

import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.joran.JoranConfigurator;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;
import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.common.MQVersion;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.remoting.common.RemotingUtil;
import org.apache.rocketmq.remoting.common.TlsMode;
import org.apache.rocketmq.remoting.netty.NettyClientConfig;
import org.apache.rocketmq.remoting.netty.NettyServerConfig;
import org.apache.rocketmq.remoting.netty.NettySystemConfig;
import org.apache.rocketmq.remoting.netty.TlsSystemConfig;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.srvutil.ServerUtil;
import org.apache.rocketmq.store.config.BrokerRole;
import org.apache.rocketmq.store.config.MessageStoreConfig;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

import static java.lang.String.valueOf;
import static java.lang.System.getProperty;
import static org.apache.rocketmq.common.MixAll.MASTER_ID;
import static org.apache.rocketmq.common.MixAll.printObjectProperties;
import static org.apache.rocketmq.common.MixAll.properties2Object;
import static org.apache.rocketmq.remoting.common.TlsMode.ENFORCING;
import static org.apache.rocketmq.remoting.netty.NettySystemConfig.COM_ROCKETMQ_REMOTING_SOCKET_RCVBUF_SIZE;
import static org.apache.rocketmq.remoting.netty.NettySystemConfig.COM_ROCKETMQ_REMOTING_SOCKET_SNDBUF_SIZE;
import static org.apache.rocketmq.remoting.netty.TlsSystemConfig.TLS_ENABLE;
import static org.apache.rocketmq.remoting.netty.TlsSystemConfig.tlsMode;
import static org.apache.rocketmq.remoting.protocol.RemotingCommand.REMOTING_VERSION_KEY;
import static org.apache.rocketmq.remoting.protocol.RemotingCommand.getSerializeTypeConfigInThisServer;
import static org.apache.rocketmq.store.config.BrokerRole.SLAVE;

/**
 * Broker启动类
 */
public class BrokerStartup {
    public static Properties properties = null;
    public static CommandLine commandLine = null;
    public static String configFile = null;
    public static InternalLogger log;

    /**
     * 入口
     * @param args
     */
    public static void main(String[] args) {
        start(createBrokerController(args));
    }

    /**
     * broker启动
     * @param controller
     * @return
     */
    public static BrokerController start(BrokerController controller) {
        try {

            //开启
            controller.start();
            BrokerConfig config = controller.getBrokerConfig();

            //开启完成信息，用于输出展示
            String tip = "The broker[" + config.getBrokerName() + ", " + controller.getBrokerAddr() + "] boot success. serializeType=" + getSerializeTypeConfigInThisServer();

            if (null != config.getNamesrvAddr()) {
                tip += " and name server is " + config.getNamesrvAddr();
            }

            log.info(tip);
            System.out.printf("%s%n", tip);
            return controller;
        } catch (Throwable e) {
            e.printStackTrace();
            System.exit(-1);
        }

        return null;
    }

    public static void shutdown(final BrokerController controller) {
        if (null != controller) {
            controller.shutdown();
        }
    }

    /**
     * 根据程序入参，进行解构建程序的逻辑主体类
     * BrokerController
     * @param args
     * @return
     */
    public static BrokerController createBrokerController(String[] args) {
        //rocketmq.remoting.version--V4_4_0
        System.setProperty(REMOTING_VERSION_KEY, Integer.toString(MQVersion.CURRENT_VERSION));

        //com.rocketmq.remoting.socket.sndbuf.size--131072（32K）
        if (null == getProperty(COM_ROCKETMQ_REMOTING_SOCKET_SNDBUF_SIZE)) {
            NettySystemConfig.socketSndbufSize = 131072;
        }

        //com.rocketmq.remoting.socket.rcvbuf.size--131072（32K）
        if (null == getProperty(COM_ROCKETMQ_REMOTING_SOCKET_RCVBUF_SIZE)) {
            NettySystemConfig.socketRcvbufSize = 131072;
        }

        try {
            //PackageConflictDetect.detectFastjson();
            Options options = ServerUtil.buildCommandlineOptions(new Options());
            //解析命令行选项
            commandLine = ServerUtil.parseCmdLine("mqbroker", args, buildCommandlineOptions(options), new PosixParser());
            if (null == commandLine) {
                System.exit(-1);
            }

            //Borker配置
            final BrokerConfig brokerConfig = new BrokerConfig();
            //Broker作为网路服务端配置
            final NettyServerConfig serverConfig = new NettyServerConfig();
            //Broker作为网络客户端配置
            final NettyClientConfig clientConfig = new NettyClientConfig();

            //tls.enable--false
            clientConfig.setUseTLS(Boolean.parseBoolean(getProperty(TLS_ENABLE, valueOf(tlsMode == ENFORCING))));
            //监听端口
            serverConfig.setListenPort(10911);
            //消息存储的配置项
            final MessageStoreConfig storeConfig = new MessageStoreConfig();

            //slave节点
            if (SLAVE == storeConfig.getBrokerRole()) {
                int ratio = storeConfig.getAccessMessageInMemoryMaxRatio() - 10;
                storeConfig.setAccessMessageInMemoryMaxRatio(ratio);
            }

            //c，指定了配置文件，使用配置文件来进行配置
            if (commandLine.hasOption('c')) {
                String file = commandLine.getOptionValue('c');
                if (file != null) {
                    configFile = file;
                    InputStream in = new BufferedInputStream(new FileInputStream(file));
                    properties = new Properties();
                    properties.load(in);

                    properties2SystemEnv(properties);
                    properties2Object(properties, brokerConfig);
                    properties2Object(properties, serverConfig);
                    properties2Object(properties, clientConfig);
                    properties2Object(properties, storeConfig);

                    BrokerPathConfigHelper.setBrokerConfigPath(file);
                    in.close();
                }
            }

            properties2Object(ServerUtil.commandLine2Properties(commandLine), brokerConfig);

            if (null == brokerConfig.getRocketmqHome()) {
                System.out.printf("Please set the %s variable in your environment to match the location of the RocketMQ installation", MixAll.ROCKETMQ_HOME_ENV);
                System.exit(-2);
            }

            //namesrv地址分号分割
            String namesrvAddr = brokerConfig.getNamesrvAddr();
            if (null != namesrvAddr) {//校验配置
                try {
                    String[] addrArray = namesrvAddr.split(";");
                    for (String addr : addrArray) {
                        RemotingUtil.string2SocketAddress(addr);
                    }
                } catch (Exception e) {
                    System.out.printf(
                        "The Name Server Address[%s] illegal, please set it as follows, \"127.0.0.1:9876;192.168.0.1:9876\"%n",
                        namesrvAddr);
                    System.exit(-3);
                }
            }

            /**
             * 节点角色
             */
            switch (storeConfig.getBrokerRole()) {
                case ASYNC_MASTER:
                case SYNC_MASTER: //brokerid
                    brokerConfig.setBrokerId(MASTER_ID);
                    break;
                case SLAVE:
                    if (brokerConfig.getBrokerId() <= 0) {
                        System.out.printf("Slave's brokerId must be > 0");
                        System.exit(-3);
                    }

                    break;
                default:
                    break;
            }

            //高可用端口
            storeConfig.setHaListenPort(serverConfig.getListenPort() + 1);
            //logback配置
            LoggerContext lc = (LoggerContext) LoggerFactory.getILoggerFactory();
            JoranConfigurator configurator = new JoranConfigurator();
            configurator.setContext(lc);
            lc.reset();
            //logback相关配置
            configurator.doConfigure(brokerConfig.getRocketmqHome() + "/conf/logback_broker.xml");

            //p选项，打印相关选项
            if (commandLine.hasOption('p')) {
                InternalLogger console = InternalLoggerFactory.getLogger(LoggerName.BROKER_CONSOLE_NAME);
                printObjectProperties(console, brokerConfig);
                printObjectProperties(console, serverConfig);
                printObjectProperties(console, clientConfig);
                printObjectProperties(console, storeConfig);
                System.exit(0);
            } else if (commandLine.hasOption('m')) { //m选项
                InternalLogger console = InternalLoggerFactory.getLogger(LoggerName.BROKER_CONSOLE_NAME);
                printObjectProperties(console, brokerConfig, true);
                printObjectProperties(console, serverConfig, true);
                printObjectProperties(console, clientConfig, true);
                printObjectProperties(console, storeConfig, true);
                System.exit(0);
            }

            log = InternalLoggerFactory.getLogger(LoggerName.BROKER_LOGGER_NAME);
            printObjectProperties(log, brokerConfig);
            printObjectProperties(log, serverConfig);
            printObjectProperties(log, clientConfig);
            printObjectProperties(log, storeConfig);

            //构建核心的控制类实例
            final BrokerController controller = new BrokerController(brokerConfig, serverConfig, clientConfig, storeConfig);
            // remember all configs to prevent discard
            controller.getConfiguration().registerConfig(properties);

            //controller类初始化整个逻辑，返回操作结果
            boolean initResult = controller.initialize();
            if (!initResult) {
                controller.shutdown();
                System.exit(-3);
            }

            //构建系统退出钩子，退出时执行controller.shutdown
            Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
                private volatile boolean hasShutdown = false;
                private AtomicInteger shutdownTimes = new AtomicInteger(0);
                @Override
                public void run() {
                    synchronized (this) {
                        log.info("Shutdown hook was invoked, {}", this.shutdownTimes.incrementAndGet());
                        if (!this.hasShutdown) {
                            this.hasShutdown = true;
                            long beginTime = System.currentTimeMillis();
                            controller.shutdown();
                            long consumingTimeTotal = System.currentTimeMillis() - beginTime;
                            log.info("Shutdown hook over, consuming total time(ms): {}", consumingTimeTotal);
                        }
                    }
                }
            }, "ShutdownHook"));

            return controller;
        } catch (Throwable e) {
            e.printStackTrace();
            System.exit(-1);
        }

        return null;
    }

    /**
     * 设置属性到环境中
     * @param properties
     */
    private static void properties2SystemEnv(Properties properties) {
        if (properties == null) {
            return;
        }
        String rmqAddressServerDomain = properties.getProperty("rmqAddressServerDomain", MixAll.WS_DOMAIN_NAME);
        String rmqAddressServerSubGroup = properties.getProperty("rmqAddressServerSubGroup", MixAll.WS_DOMAIN_SUBGROUP);
        System.setProperty("rocketmq.namesrv.domain", rmqAddressServerDomain);
        System.setProperty("rocketmq.namesrv.domain.subgroup", rmqAddressServerSubGroup);
    }

    private static Options buildCommandlineOptions(final Options options) {
        Option opt = new Option("c", "configFile", true, "Broker config properties file");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("p", "printConfigItem", false, "Print all config item");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("m", "printImportantConfig", false, "Print important config item");
        opt.setRequired(false);
        options.addOption(opt);

        return options;
    }
}
