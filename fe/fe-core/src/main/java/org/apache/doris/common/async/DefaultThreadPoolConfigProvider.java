// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.doris.common.async;

import org.apache.doris.common.Config;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;

public class DefaultThreadPoolConfigProvider implements AsyncThreadPoolFactory.ThreadPoolConfigProvider {

    public static final long KEEP_ALIVE_TIME = 60L;

    @Override
    public AsyncThreadPoolFactory.ThreadPoolConfig getConfig(AsyncThreadPoolFactory.ThreadPoolType type) {
        switch (type) {
            case PRIVILEGE_CHECK:
                int authCorePoolSize = Config.parallel_auth_thread_num;
                if (authCorePoolSize <= 0) {
                    authCorePoolSize = Math.max(8, Runtime.getRuntime().availableProcessors() / 8);
                }
                int authMaxPoolSize = authCorePoolSize * 2;
                int authQueueSize = Config.parallel_auth_queue_size;
                return new AsyncThreadPoolFactory.ThreadPoolConfig(
                        authCorePoolSize, authMaxPoolSize, KEEP_ALIVE_TIME, TimeUnit.SECONDS,
                        new ArrayBlockingQueue<>(authQueueSize)
                );
            case COMMON_BUSINESS:
                int cbCorePoolSize = Config.common_business_async_thread_num;
                if (cbCorePoolSize <= 0) {
                    cbCorePoolSize = Math.max(32, Runtime.getRuntime().availableProcessors());
                }
                int cbMaxPoolSize = cbCorePoolSize * 2;
                int cbQueueSize = Config.common_business_async_queue_size;
                return new AsyncThreadPoolFactory.ThreadPoolConfig(
                        cbCorePoolSize, cbMaxPoolSize, KEEP_ALIVE_TIME, TimeUnit.SECONDS,
                        new ArrayBlockingQueue<>(cbQueueSize)
                );
            default:
                throw new IllegalArgumentException("Unknown thread pool type: " + type);
        }
    }
}
