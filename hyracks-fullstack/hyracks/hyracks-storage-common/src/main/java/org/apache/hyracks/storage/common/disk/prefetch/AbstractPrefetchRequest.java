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
package org.apache.hyracks.storage.common.disk.prefetch;

import java.util.concurrent.locks.ReentrantLock;

import org.apache.hyracks.api.exceptions.HyracksDataException;

public abstract class AbstractPrefetchRequest implements Runnable {
    private final ReentrantLock lock;
    private volatile PrefetchState state;

    protected AbstractPrefetchRequest() {
        lock = new ReentrantLock();
        state = PrefetchState.QUEUED;
    }

    public final void reset() {
        state = PrefetchState.QUEUED;
    }

    @Override
    public final void run() {
        try {
            if (state == PrefetchState.QUEUED) {
                // Acquire a lock so the prefetching op. won't get aborted
                lock.lock();
                doPrefetch();
                state = PrefetchState.FINISHED;
            }
        } catch (HyracksDataException e) {
            onException(e);
            state = PrefetchState.FAILED;
        } catch (Throwable e) {
            onFailure(e);
            state = PrefetchState.FAILED;
        } finally {
            lock.unlock();
        }
    }

    /**
     * @return true if aborted or failed, false otherwise
     */
    public final boolean waitOrAbort() {
        // Acquire lock to ensure the prefetching op. is not running
        lock.lock();
        try {
            switch (state) {
                case QUEUED:
                    // The request is still queued, abort
                    state = PrefetchState.ABORTED;
                case FAILED:
                    // The request failed
                    return true;
                default:
                    // The request finished
                    return false;
            }
        } finally {
            lock.unlock();
        }
    }

    protected abstract void doPrefetch() throws HyracksDataException;

    protected abstract void onException(HyracksDataException e);

    protected abstract void onFailure(Throwable e);
}
