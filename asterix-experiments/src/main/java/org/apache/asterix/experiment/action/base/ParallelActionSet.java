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

package org.apache.asterix.experiment.action.base;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

public class ParallelActionSet extends AbstractAction {

    private final ExecutorService executor;

    private final List<IAction> actions;

    public ParallelActionSet() {
        executor = Executors.newCachedThreadPool(new ThreadFactory() {

            private final AtomicInteger tid = new AtomicInteger(0);

            @Override
            public Thread newThread(Runnable r) {
                Thread t = new Thread(r);
                t.setDaemon(true);
                t.setName("ParallelActionThread " + tid.getAndIncrement());
                return t;
            }
        });
        actions = new ArrayList<>();
    }

    public void add(IAction action) {
        actions.add(action);
    }

    @Override
    protected void doPerform() throws Exception {
        final Semaphore sem = new Semaphore(-(actions.size() - 1));
        for (final IAction a : actions) {
            executor.execute(new Runnable() {

                @Override
                public void run() {
                    try {
                        a.perform();
                    } finally {
                        sem.release();
                    }
                }
            });
        }
        sem.acquire();
    }

}
