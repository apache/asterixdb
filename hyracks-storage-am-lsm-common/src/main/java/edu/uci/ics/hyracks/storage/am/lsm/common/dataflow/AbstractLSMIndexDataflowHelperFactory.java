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

package edu.uci.ics.hyracks.storage.am.lsm.common.dataflow;

import edu.uci.ics.hyracks.storage.am.common.dataflow.IIndexDataflowHelperFactory;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMFlushControllerProvider;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMIOOperationSchedulerProvider;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMMergePolicyProvider;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMOperationTrackerFactory;

public abstract class AbstractLSMIndexDataflowHelperFactory implements IIndexDataflowHelperFactory {
    protected static final long serialVersionUID = 1L;

    protected final ILSMFlushControllerProvider flushControllerProvider;
    protected final ILSMMergePolicyProvider mergePolicyProvider;
    protected final ILSMOperationTrackerFactory opTrackerFactory;
    protected final ILSMIOOperationSchedulerProvider ioSchedulerProvider;

    public AbstractLSMIndexDataflowHelperFactory(ILSMFlushControllerProvider flushControllerProvider,
            ILSMMergePolicyProvider mergePolicyProvider, ILSMOperationTrackerFactory opTrackerFactory,
            ILSMIOOperationSchedulerProvider ioSchedulerProvider) {
        this.flushControllerProvider = flushControllerProvider;
        this.mergePolicyProvider = mergePolicyProvider;
        this.opTrackerFactory = opTrackerFactory;
        this.ioSchedulerProvider = ioSchedulerProvider;
    }
}
