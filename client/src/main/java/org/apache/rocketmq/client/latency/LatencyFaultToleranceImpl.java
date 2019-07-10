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

package org.apache.rocketmq.client.latency;

import java.util.Collections;
import java.util.Enumeration;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.rocketmq.client.common.ThreadLocalIndex;

public class LatencyFaultToleranceImpl implements LatencyFaultTolerance<String> {
    private final ConcurrentHashMap<String, FaultItem> faultItemTable = new ConcurrentHashMap<String, FaultItem>(16);

    private final ThreadLocalIndex whichItemWorst = new ThreadLocalIndex();

    /**
     * 更新一下容错的item
     * @param name
     * @param currentLatency
     * @param notAvailableDuration
     */
    @Override
    public void updateFaultItem(final String name, final long currentLatency, final long notAvailableDuration) {
        //老的容错项
        FaultItem old = this.faultItemTable.get(name);
        if (null == old) {//不存在，直接更新
            final FaultItem faultItem = new FaultItem(name);
            faultItem.setCurrentLatency(currentLatency);
            //更新不可用时间，当前时间加上延迟时间
            faultItem.setStartTimestamp(System.currentTimeMillis() + notAvailableDuration);

            old = this.faultItemTable.putIfAbsent(name, faultItem);
            if (old != null) {
                old.setCurrentLatency(currentLatency);
                old.setStartTimestamp(System.currentTimeMillis() + notAvailableDuration);
            }
        } else {
            old.setCurrentLatency(currentLatency);
            old.setStartTimestamp(System.currentTimeMillis() + notAvailableDuration);
        }
    }

    /**
     * 从发生过错误的项数中进行操作，如果存在，我们检测一下这个项的可用性
     * @param name
     * @return
     */
    @Override
    public boolean isAvailable(final String name) {
        final FaultItem faultItem = this.faultItemTable.get(name);
        if (faultItem != null) {
            return faultItem.isAvailable();
        }
        return true;
    }

    @Override
    public void remove(final String name) {
        this.faultItemTable.remove(name);
    }

    /**
     * 选择最近的一个broker
     * @return
     */
    @Override
    public String pickOneAtLeast() {
        final Enumeration<FaultItem> elements = this.faultItemTable.elements();
        List<FaultItem> copyList = new LinkedList<FaultItem>();
        while (elements.hasMoreElements()) {
            copyList.add(elements.nextElement());
        }
        if (copyList.size() == 0) {
            return null;
        }

        Collections.shuffle(copyList);

        Collections.sort(copyList);

        final int half = copyList.size() / 2;
        if (half <= 0) {
            return copyList.get(0).getName();
        } else {
            final int i = this.whichItemWorst.getAndIncrement() % half;
            return copyList.get(i).getName();
        }

    }

    @Override
    public String toString() {
        return "LatencyFaultToleranceImpl{" +
            "faultItemTable=" + faultItemTable +
            ", whichItemWorst=" + whichItemWorst +
            '}';
    }

    /**
     * 容错项
     */
    class FaultItem implements Comparable<FaultItem> {
        //名字
        private final String name;
        //当前延迟
        private volatile long currentLatency;
        //开始时间
        private volatile long startTimestamp;

        public FaultItem(final String name) {
            this.name = name;
        }

        @Override
        public int compareTo(final FaultItem other) {
            if (this.isAvailable() != other.isAvailable()) {
                if (this.isAvailable())
                    return -1;

                if (other.isAvailable())
                    return 1;
            }

            if (this.currentLatency < other.currentLatency)
                return -1;
            else if (this.currentLatency > other.currentLatency) {
                return 1;
            }

            if (this.startTimestamp < other.startTimestamp)
                return -1;
            else if (this.startTimestamp > other.startTimestamp) {
                return 1;
            }

            return 0;
        }

        /**
         * 当前时间大于开始时间
         * @return
         */
        public boolean isAvailable() {
            return (System.currentTimeMillis() - startTimestamp) >= 0;
        }

        @Override
        public int hashCode() {
            int result = getName() != null ? getName().hashCode() : 0;
            result = 31 * result + (int) (getCurrentLatency() ^ (getCurrentLatency() >>> 32));
            result = 31 * result + (int) (getStartTimestamp() ^ (getStartTimestamp() >>> 32));
            return result;
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o)
                return true;
            if (!(o instanceof FaultItem))
                return false;

            final FaultItem faultItem = (FaultItem) o;

            if (getCurrentLatency() != faultItem.getCurrentLatency())
                return false;
            if (getStartTimestamp() != faultItem.getStartTimestamp())
                return false;
            return getName() != null ? getName().equals(faultItem.getName()) : faultItem.getName() == null;

        }

        @Override
        public String toString() {
            return "FaultItem{" +
                "name='" + name + '\'' +
                ", currentLatency=" + currentLatency +
                ", startTimestamp=" + startTimestamp +
                '}';
        }

        public String getName() {
            return name;
        }

        public long getCurrentLatency() {
            return currentLatency;
        }

        public void setCurrentLatency(final long currentLatency) {
            this.currentLatency = currentLatency;
        }

        public long getStartTimestamp() {
            return startTimestamp;
        }

        public void setStartTimestamp(final long startTimestamp) {
            this.startTimestamp = startTimestamp;
        }

    }
}
