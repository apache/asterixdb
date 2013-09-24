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

import java.io.IOException;
import java.util.List;

import edu.uci.ics.asterix.common.exceptions.ACIDException;
import edu.uci.ics.asterix.common.exceptions.AsterixException;
import edu.uci.ics.asterix.common.transactions.ITransactionSubsystem;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.storage.am.common.api.IIndexLifecycleManager;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMIOOperationScheduler;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMMergePolicyFactory;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMOperationTracker;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.IVirtualBufferCache;
import edu.uci.ics.hyracks.storage.common.buffercache.IBufferCache;
import edu.uci.ics.hyracks.storage.common.file.IFileMapProvider;
import edu.uci.ics.hyracks.storage.common.file.ILocalResourceRepository;
import edu.uci.ics.hyracks.storage.common.file.ResourceIdFactory;

public interface IAsterixAppRuntimeContext {

    public ITransactionSubsystem getTransactionSubsystem();

    public boolean isShuttingdown();

    public ILSMIOOperationScheduler getLSMIOScheduler();

    public ILSMMergePolicyFactory getMetadataMergePolicyFactory();

    public int getMetaDataIODeviceId();

    public IBufferCache getBufferCache();

    public IFileMapProvider getFileMapManager();

    public ILocalResourceRepository getLocalResourceRepository();

    public IIndexLifecycleManager getIndexLifecycleManager();

    public ResourceIdFactory getResourceIdFactory();

    public ILSMOperationTracker getLSMBTreeOperationTracker(int datasetID);

    public void initialize() throws IOException, ACIDException, AsterixException;

    public void setShuttingdown(boolean b);

    public void deinitialize() throws HyracksDataException;

    public double getBloomFilterFalsePositiveRate();

    public List<IVirtualBufferCache> getVirtualBufferCaches(int datasetID);
}
