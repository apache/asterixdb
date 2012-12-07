/*
 * Copyright 2009-2012 by The Regents of the University of California
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

package edu.uci.ics.hyracks.storage.am.lsm.common.impls;

import java.util.concurrent.atomic.AtomicInteger;

import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMComponent;

public abstract class AbstractLSMComponent implements ILSMComponent {

    private final AtomicInteger threadRef = new AtomicInteger();
    private LSMComponentState state;

    private final Object componentSync = new Object();

    @Override
    public void activate() {

    }

    @Override
    public void deactivate() {

    }

    @Override
    public void threadEnter() {
        threadRef.incrementAndGet();
    }

    @Override
    public void threadExit() {
        threadRef.decrementAndGet();
    }

    @Override
    public int getThreadReferenceCount() {
        return threadRef.get();
    }

    @Override
    public void setState(LSMComponentState state) {
        synchronized (componentSync) {
            this.state = state;
        }
    }

    @Override
    public LSMComponentState getState() {
        synchronized (componentSync) {
            return state;
        }
    }

    @Override
    public boolean negativeCompareAndSet(LSMComponentState compare, LSMComponentState update) {
        synchronized (componentSync) {
            if (state != compare) {
                state = update;
                return true;
            }
        }
        return false;
    }
}
