/*
 * Copyright 2009-2010 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.uci.ics.hyracks.control.common.work;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.logging.Logger;

public class WorkQueue {
    private static final Logger LOGGER = Logger.getLogger(WorkQueue.class.getName());

    private final LinkedBlockingQueue<AbstractWork> queue;
    private final JobThread thread;

    public WorkQueue() {
        queue = new LinkedBlockingQueue<AbstractWork>();
        thread = new JobThread();
        thread.start();
    }

    public void schedule(AbstractWork event) {
        if (LOGGER.isLoggable(event.logLevel())) {
            LOGGER.info("Scheduling: " + event);
        }
        queue.offer(event);
    }

    public void scheduleAndSync(SynchronizableWork sRunnable) throws Exception {
        schedule(sRunnable);
        sRunnable.sync();
    }

    private class JobThread extends Thread {
        JobThread() {
            setDaemon(true);
        }

        @Override
        public void run() {
            Runnable r;
            while (true) {
                try {
                    r = queue.take();
                } catch (InterruptedException e) {
                    continue;
                }
                try {
                    r.run();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
    }
}