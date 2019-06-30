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
package org.apache.rocketmq.common.filter;

import java.net.URL;
import org.apache.rocketmq.common.protocol.heartbeat.SubscriptionData;

import static org.apache.rocketmq.common.protocol.heartbeat.SubscriptionData.SUB_ALL;

public class FilterAPI {
    public static URL classFile(final String className) {
        final String javaSource = simpleClassName(className) + ".java";
        URL url = FilterAPI.class.getClassLoader().getResource(javaSource);
        return url;
    }

    public static String simpleClassName(final String className) {
        String simple = className;
        int index = className.lastIndexOf(".");
        if (index >= 0) {
            simple = className.substring(index + 1);
        }

        return simple;
    }

    /**
     * 构建订阅数据，订阅数据的构建，发现消费组是没什么乱用的
     * @param consumerGroup 消费组
     * @param topic 主题
     * @param subString 内容
     * @return
     * @throws Exception
     */
    public static SubscriptionData buildSubscriptionData(final String consumerGroup, String topic, String subString) throws Exception {
        SubscriptionData subscriptionData = new SubscriptionData();
        subscriptionData.setTopic(topic);
        subscriptionData.setSubString(subString);

        if (null == subString || subString.equals(SUB_ALL) || subString.length() == 0) {
            subscriptionData.setSubString(SUB_ALL);
        } else { //使用的是tag
            String[] tags = subString.split("\\|\\|");
            for (String tag : tags) {
                String trim = tag.trim();
                if (trim.length() > 0) {
                    //tag
                    subscriptionData.getTagsSet().add(trim);
                    //tag的哈市code
                    subscriptionData.getCodeSet().add(trim.hashCode());
                }
            }
        }
        return subscriptionData;
    }

    /**
     * 构建订阅的数据
     * @param topic
     * @param subString
     * @param type
     * @return
     * @throws Exception
     */
    public static SubscriptionData build(final String topic, final String subString, final String type) throws Exception {
        if (ExpressionType.TAG.equals(type) || type == null) { //有tag 或者type不存在
            return buildSubscriptionData(null, topic, subString);
        }

        if (subString == null || subString.length() < 1) {
            throw new IllegalArgumentException("Expression can't be null! " + type);
        }

        SubscriptionData data = new SubscriptionData();
        data.setTopic(topic);
        data.setSubString(subString); //设置新的表达是
        data.setExpressionType(type); //设置类型
        return data;
    }
}
