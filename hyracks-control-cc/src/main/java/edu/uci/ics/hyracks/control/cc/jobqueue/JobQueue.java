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
package edu.uci.ics.hyracks.control.cc.jobqueue;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.logging.Logger;

public class JobQueue {
    private static final Logger LOGGER = Logger.getLogger(JobQueue.class.getName());

    private final LinkedBlockingQueue<Runnable> queue;
    private final JobThread thread;

    public JobQueue() {
        queue = new LinkedBlockingQueue<Runnable>();
        thread = new JobThread();
        thread.start();
    }

    public void schedule(Runnable runnable) {
        LOGGER.info("Scheduling: " + runnable);
        queue.offer(runnable);
    }

    public void scheduleAndSync(SynchronizableRunnable sRunnable) throws Exception {
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