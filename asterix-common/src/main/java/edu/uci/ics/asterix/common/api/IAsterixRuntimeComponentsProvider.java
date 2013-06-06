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
package edu.uci.ics.asterix.common.api;

import edu.uci.ics.hyracks.api.io.IIOManager;
import edu.uci.ics.hyracks.storage.am.common.api.IIndexLifecycleManager;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMIOOperationCallbackProvider;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMIOOperationScheduler;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMMergePolicy;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMOperationTrackerProvider;
import edu.uci.ics.hyracks.storage.common.buffercache.IBufferCache;
import edu.uci.ics.hyracks.storage.common.file.IFileMapProvider;
import edu.uci.ics.hyracks.storage.common.file.ILocalResourceRepository;
import edu.uci.ics.hyracks.storage.common.file.ResourceIdFactory;

public interface IAsterixRuntimeComponentsProvider {

    public IBufferCache getBufferCache();

    public IFileMapProvider getFileMapManager();

    public IIndexLifecycleManager getIndexLifecycleManager();

    public double getBloomFilterFalsePositiveRate();

    public ILSMMergePolicy getLSMMergePolicy();

    public ILSMOperationTrackerProvider getLSMBTreeOperationTrackerFactory();

    public ILSMOperationTrackerProvider getLSMRTreeOperationTrackerFactory();

    public ILSMOperationTrackerProvider getLSMInvertedIndexOperationTrackerFactory();

    public ILSMIOOperationCallbackProvider getLSMBTreeIOOperationCallbackProvider();

    public ILSMIOOperationCallbackProvider getLSMRTreeIOOperationCallbackProvider();

    public ILSMIOOperationCallbackProvider getLSMInvertedIndexIOOperationCallbackProvider();

    public ILSMIOOperationCallbackProvider getNoOpIOOperationCallbackProvider();

    public ILSMIOOperationScheduler getLSMIOScheduler();

    public ILocalResourceRepository getLocalResourceRepository();

    public ResourceIdFactory getResourceIdFactory();

    public IIOManager getIOManager();
}
