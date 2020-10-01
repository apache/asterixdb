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

import java.util.Objects;

import org.apache.asterix.active.IActiveEntityEventSubscriber;
import org.apache.asterix.active.IActiveEntityEventsListener;
import org.apache.hyracks.util.Span;

public abstract class AbstractSubscriber implements IActiveEntityEventSubscriber {

    protected final IActiveEntityEventsListener listener;
    private final Object lockObject;
    private volatile boolean done = false;
    private volatile Exception failure = null;

    public AbstractSubscriber(IActiveEntityEventsListener listener, Object lockObject) {
        Objects.requireNonNull(lockObject);
        this.listener = listener;
        this.lockObject = lockObject;
    }

    public AbstractSubscriber(IActiveEntityEventsListener listener) {
        this.listener = listener;
        this.lockObject = this;
    }

    @Override
    public boolean isDone() {
        return done;
    }

    public void complete(Exception failure) {
        synchronized (lockObject) {
            if (failure != null) {
                this.failure = failure;
            }
            done = true;
            lockObject.notifyAll();
        }
    }

    @Override
    public void sync() throws InterruptedException {
        synchronized (lockObject) {
            while (!done) {
                lockObject.wait();
            }
        }
    }

    public boolean sync(Span span) throws InterruptedException {
        synchronized (lockObject) {
            while (!done) {
                span.wait(lockObject);
                if (done || span.elapsed()) {
                    return done;
                }
            }
            return done;
        }
    }

    public Exception getFailure() {
        return failure;
    }

    protected void reset() {
        synchronized (lockObject) {
            done = false;
            failure = null;
        }
    }
}
