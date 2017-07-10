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
package org.apache.asterix.external.feed.watch;

import org.apache.asterix.active.IActiveEntityEventsListener;
import org.apache.asterix.active.IActiveEventSubscriber;
import org.apache.hyracks.api.exceptions.HyracksDataException;

public abstract class AbstractSubscriber implements IActiveEventSubscriber {

    protected final IActiveEntityEventsListener listener;
    private boolean done = false;

    public AbstractSubscriber(IActiveEntityEventsListener listener) {
        this.listener = listener;
    }

    @Override
    public synchronized boolean isDone() {
        return done;
    }

    public synchronized void complete() throws HyracksDataException {
        done = true;
        notifyAll();
    }

    @Override
    public synchronized void sync() throws InterruptedException {
        while (!done) {
            wait();
        }
    }

    @Override
    public synchronized void unsubscribe() {
        done = true;
        notifyAll();
    }
}
