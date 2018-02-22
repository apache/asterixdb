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
package org.apache.hyracks.control.nc.io;

import java.util.concurrent.BlockingQueue;

import org.apache.hyracks.api.util.InvokeUtil;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class IoRequestHandler implements Runnable {
    private static final Logger LOGGER = LogManager.getLogger();
    public static final IoRequest POISON_PILL = new IoRequest(null, null, null);
    private final int num;
    private final BlockingQueue<IoRequest> queue;

    public IoRequestHandler(int num, BlockingQueue<IoRequest> queue) {
        this.num = num;
        this.queue = queue;
    }

    @Override
    public void run() {
        Thread.currentThread().setName(getClass().getSimpleName() + "-" + num);
        while (true) { // NOSONAR: Suppress 1 continue and 1 break
            IoRequest next;
            try {
                next = queue.take();
            } catch (InterruptedException e) { // NOSONAR: This is not supposed to be ever interrupted
                LOGGER.log(Level.WARN, "Ignoring interrupt. IO threads should never be interrupted.");
                continue;
            }
            if (next == POISON_PILL) {
                LOGGER.log(Level.INFO, "Exiting");
                InvokeUtil.doUninterruptibly(() -> queue.put(POISON_PILL));
                if (Thread.interrupted()) {
                    LOGGER.log(Level.ERROR, "Ignoring interrupt. IO threads should never be interrupted.");
                }
                break;
            }
            next.handle();
        }
    }
}
