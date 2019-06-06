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
package org.apache.rocketmq.common;

import java.io.IOException;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;

public abstract class ConfigManager {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.COMMON_LOGGER_NAME);

    public abstract String encode();

    /**
     * 关于管理器开始加载，任何子类，通过实现相关的protect方法来实现相关自定义处理逻辑
     *
     * @return
     */
    public boolean load() {
        String fileName = null;
        try {
            //配置文件
            fileName = this.configFilePath();
            //读取配置文件，里面的配置都是json格式的
            String jsonString = MixAll.file2String(fileName);

            //不存在相关的配置，尝试读取相关的备份，备份是加了尾缀.bak
            if (null == jsonString || jsonString.length() == 0) {
                return this.loadBak();
            } else {
                //进行解码
                this.decode(jsonString);
                log.info("load " + fileName + " OK");
                return true;
            }
        } catch (Exception e) {
            log.error("load " + fileName + " failed, and try to load backup file", e);
            return this.loadBak();
        }
    }

    /**
     * 获取配置文件的路径
     * @return 配置文件的路径
     */
    public abstract String configFilePath();


    /**
     * 读取备份的配置文件
     * @return
     */
    private boolean loadBak() {
        String fileName = null;
        try {
            fileName = this.configFilePath();
            String jsonString = MixAll.file2String(fileName + ".bak");
            if (jsonString != null && jsonString.length() > 0) {
                this.decode(jsonString);
                log.info("load " + fileName + " OK");
                return true;
            }
        } catch (Exception e) {
            log.error("load " + fileName + " Failed", e);
            return false;
        }

        return true;
    }

    /**
     *由子类进行从配置文件中加载出来的相关的字符串
     * @param jsonString
     */
    public abstract void decode(final String jsonString);

    /**
     * 持久化，持久化内存的中的配置项到实际的文件中，由子类负责加密
     */
    public synchronized void persist() {
        String jsonString = this.encode(true);
        if (jsonString != null) {
            String fileName = this.configFilePath();
            try {
                MixAll.string2File(jsonString, fileName);
            } catch (IOException e) {
                log.error("persist file " + fileName + " exception", e);
            }
        }
    }

    public abstract String encode(final boolean prettyFormat);
}
