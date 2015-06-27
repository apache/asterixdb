/*
 * Copyright 2009-2013 by The Regents of the University of California
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
package edu.uci.ics.asterix.metadata.feeds;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import edu.uci.ics.asterix.common.feeds.api.IFeedWork;
import edu.uci.ics.asterix.common.feeds.api.IFeedWorkEventListener;
import edu.uci.ics.asterix.common.feeds.api.IFeedWorkManager;

/**
 * Handles asynchronous execution of feed management related tasks.
 */
public class FeedWorkManager implements IFeedWorkManager {

    public static final FeedWorkManager INSTANCE = new FeedWorkManager();

    private final ExecutorService executorService = Executors.newCachedThreadPool();

    private FeedWorkManager() {
    }

    public void submitWork(IFeedWork work, IFeedWorkEventListener listener) {
        Runnable runnable = work.getRunnable();
        try {
            executorService.execute(runnable);
            listener.workCompleted(work);
        } catch (Exception e) {
            listener.workFailed(work, e);
        }
    }

}