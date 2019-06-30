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
package org.apache.rocketmq.tools.command;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.rocketmq.remoting.RPCHook;

public interface SubCommand {
    /**
     * 命令名
     * @return
     */
    String commandName();

    /**
     * 命令描述
     * @return
     */
    String commandDesc();

    /**
     * 构建命令选项
     * @param options
     * @return
     */
    Options buildCommandlineOptions(final Options options);

    /**
     * 执行
     * @param commandLine
     * @param options
     * @param rpcHook
     * @throws SubCommandException
     */
    void execute(final CommandLine commandLine, final Options options, RPCHook rpcHook) throws SubCommandException;
}
