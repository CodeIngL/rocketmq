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
package org.apache.rocketmq.broker.transaction.queue;

import org.apache.rocketmq.broker.transaction.AbstractTransactionalMessageCheckListener;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;

public class DefaultTransactionalMessageCheckListener extends AbstractTransactionalMessageCheckListener {

    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.TRANSACTION_LOGGER_NAME);

    public DefaultTransactionalMessageCheckListener() {
        super();
    }

    /**
     * 消息被丢弃了，在存储中丢弃是非常简单的，我们不需要去做额外的事情，而仅仅是通过记录一些日志就可以了
     * @param msgExt Message to be discarded.
     */
    @Override
    public void resolveDiscardMsg(MessageExt msgExt) {
        log.error("MsgExt:{} has been checked too many times, so discard it", msgExt);
    }
}
