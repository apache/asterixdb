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
package org.apache.asterix.common.api;

import org.apache.hyracks.api.io.IIOManager;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperationCallbackProvider;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperationScheduler;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMMergePolicy;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMOperationTrackerProvider;
import org.apache.hyracks.storage.common.buffercache.IBufferCache;
import org.apache.hyracks.storage.common.file.IFileMapProvider;
import org.apache.hyracks.storage.common.file.ILocalResourceRepository;
import org.apache.hyracks.storage.common.file.ResourceIdFactory;

public interface IAsterixRuntimeComponentsProvider {

    public IBufferCache getBufferCache();

    public IFileMapProvider getFileMapManager();

    public IDatasetLifecycleManager getDatasetLifecycleManager();

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
