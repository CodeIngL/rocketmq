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
package org.apache.rocketmq.common.sysflag;

/**
 * 拉取消息时带上的相关标记
 * 支持commitOffset支持挂起，支持自带的订阅标记，支持classFilter模式
 * 最终pull和push在表现上会存在相关的不同
 */
public class PullSysFlag {
    private final static int FLAG_COMMIT_OFFSET = 0x1 << 0;
    private final static int FLAG_SUSPEND = 0x1 << 1; //支持挂起
    private final static int FLAG_SUBSCRIPTION = 0x1 << 2; //支持订阅
    private final static int FLAG_CLASS_FILTER = 0x1 << 3;//支持类过滤

    /**
     *
     * @param commitOffset 是否有commitOffset
     * @param suspend
     * @param subscription
     * @param classFilter
     * @return
     */
    public static int buildSysFlag(final boolean commitOffset, final boolean suspend, final boolean subscription, final boolean classFilter) {
        int flag = 0;

        if (commitOffset) {
            flag |= FLAG_COMMIT_OFFSET;
        }

        if (suspend) {
            flag |= FLAG_SUSPEND;
        }

        if (subscription) {
            flag |= FLAG_SUBSCRIPTION;
        }

        if (classFilter) {
            flag |= FLAG_CLASS_FILTER;
        }

        return flag;
    }

    /**
     * 清除commit标记
     * @param sysFlag
     * @return
     */
    public static int clearCommitOffsetFlag(final int sysFlag) {
        return sysFlag & (~FLAG_COMMIT_OFFSET);
    }

    //校验是否存在相关的标记

    public static boolean hasCommitOffsetFlag(final int sysFlag) {
        return (sysFlag & FLAG_COMMIT_OFFSET) == FLAG_COMMIT_OFFSET;
    }

    public static boolean hasSuspendFlag(final int sysFlag) {
        return (sysFlag & FLAG_SUSPEND) == FLAG_SUSPEND;
    }

    public static boolean hasSubscriptionFlag(final int sysFlag) {
        return (sysFlag & FLAG_SUBSCRIPTION) == FLAG_SUBSCRIPTION;
    }

    public static boolean hasClassFilterFlag(final int sysFlag) {
        return (sysFlag & FLAG_CLASS_FILTER) == FLAG_CLASS_FILTER;
    }
}
