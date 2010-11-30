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
package edu.uci.ics.hyracks.control.nc.runtime;

import java.util.UUID;

import edu.uci.ics.hyracks.api.context.IHyracksContext;
import edu.uci.ics.hyracks.api.job.profiling.counters.ICounterContext;
import edu.uci.ics.hyracks.api.resources.IResourceManager;

public class DelegateHyracksContext implements IHyracksContext {
    private final IHyracksContext delegate;

    private final ICounterContext counterContext;

    private final UUID jobId;

    public DelegateHyracksContext(IHyracksContext delegate, UUID jobId, ICounterContext counterContext) {
        this.delegate = delegate;
        this.counterContext = counterContext;
        this.jobId = jobId;
    }

    @Override
    public IResourceManager getResourceManager() {
        return delegate.getResourceManager();
    }

    @Override
    public int getFrameSize() {
        return delegate.getFrameSize();
    }

    @Override
    public UUID getJobId() {
        return jobId;
    }

    @Override
    public ICounterContext getCounterContext() {
        return counterContext;
    }
}