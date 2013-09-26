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

package edu.uci.ics.hyracks.storage.am.lsm.common.dataflow;

import java.util.Map;

import edu.uci.ics.hyracks.storage.am.common.dataflow.IIndexDataflowHelperFactory;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMIOOperationCallbackFactory;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMIOOperationSchedulerProvider;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMMergePolicyFactory;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMOperationTrackerProvider;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.IVirtualBufferCacheProvider;

public abstract class AbstractLSMIndexDataflowHelperFactory implements IIndexDataflowHelperFactory {
    protected static final long serialVersionUID = 1L;

    protected final IVirtualBufferCacheProvider virtualBufferCacheProvider;
    protected final ILSMMergePolicyFactory mergePolicyFactory;
    protected final Map<String, String> mergePolicyProperties;
    protected final ILSMOperationTrackerProvider opTrackerFactory;
    protected final ILSMIOOperationSchedulerProvider ioSchedulerProvider;
    protected final ILSMIOOperationCallbackFactory ioOpCallbackFactory;
    protected final double bloomFilterFalsePositiveRate;

    public AbstractLSMIndexDataflowHelperFactory(IVirtualBufferCacheProvider virtualBufferCacheProvider,
            ILSMMergePolicyFactory mergePolicyFactory, Map<String, String> mergePolicyProperties,
            ILSMOperationTrackerProvider opTrackerFactory, ILSMIOOperationSchedulerProvider ioSchedulerProvider,
            ILSMIOOperationCallbackFactory ioOpCallbackFactory, double bloomFilterFalsePositiveRate) {
        this.virtualBufferCacheProvider = virtualBufferCacheProvider;
        this.mergePolicyFactory = mergePolicyFactory;
        this.opTrackerFactory = opTrackerFactory;
        this.ioSchedulerProvider = ioSchedulerProvider;
        this.ioOpCallbackFactory = ioOpCallbackFactory;
        this.bloomFilterFalsePositiveRate = bloomFilterFalsePositiveRate;
        this.mergePolicyProperties = mergePolicyProperties;
    }
}
