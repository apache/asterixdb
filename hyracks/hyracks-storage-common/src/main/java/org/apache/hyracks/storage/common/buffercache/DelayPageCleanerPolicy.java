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
package edu.uci.ics.hyracks.storage.common.buffercache;

import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;

public class DelayPageCleanerPolicy implements IPageCleanerPolicy {
    private long delay;

    public DelayPageCleanerPolicy(long delay) {
        this.delay = delay;
    }

    @Override
    public void notifyCleanCycleStart(Object monitor) throws HyracksDataException {

    }

    @Override
    public void notifyCleanCycleFinish(Object monitor) throws HyracksDataException {
        try {
            monitor.wait(delay);
        } catch (InterruptedException e) {
            throw new HyracksDataException(e);
        }
    }

    @Override
    public void notifyVictimNotFound(Object monitor) throws HyracksDataException {
        monitor.notifyAll();
    }
}