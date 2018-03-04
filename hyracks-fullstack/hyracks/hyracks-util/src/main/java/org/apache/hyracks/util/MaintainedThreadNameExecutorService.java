/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hyracks.util;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class MaintainedThreadNameExecutorService extends ThreadPoolExecutor {

    private final Map<Thread, String> threadNames = new ConcurrentHashMap<>();

    private MaintainedThreadNameExecutorService(ThreadFactory threadFactory) {
        super(0, Integer.MAX_VALUE, 60L, TimeUnit.SECONDS, new SynchronousQueue<>(), threadFactory);
    }

    public static ExecutorService newCachedThreadPool(ThreadFactory threadFactory) {
        return new MaintainedThreadNameExecutorService(threadFactory);
    }

    @Override
    protected void beforeExecute(Thread t, Runnable r) {
        threadNames.put(t, t.getName());
        super.beforeExecute(t, r);
    }

    @Override
    protected void afterExecute(Runnable r, Throwable t) {
        super.afterExecute(r, t);
        final Thread thread = Thread.currentThread();
        final String originalThreadName = threadNames.remove(thread);
        if (originalThreadName != null) {
            thread.setName(originalThreadName);
        }
    }
}
