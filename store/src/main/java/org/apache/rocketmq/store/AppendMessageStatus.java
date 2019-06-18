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
package org.apache.rocketmq.store;

/**
 * When write a message to the commit log, returns code
 * <p>将消息写入提交日志时，返回code</p>
 */
public enum AppendMessageStatus {
    PUT_OK, //投递成功
    END_OF_FILE, //遇到文件尾
    MESSAGE_SIZE_EXCEEDED, //消息大小超过
    PROPERTIES_SIZE_EXCEEDED, //属性大小超过
    UNKNOWN_ERROR, //未知的错误
}
