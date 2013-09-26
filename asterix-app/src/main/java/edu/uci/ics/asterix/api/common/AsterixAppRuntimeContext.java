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
package edu.uci.ics.asterix.api.common;

import java.io.IOException;
import java.util.List;
import java.util.logging.Logger;

import edu.uci.ics.asterix.common.api.IAsterixAppRuntimeContext;
import edu.uci.ics.asterix.common.config.AsterixCompilerProperties;
import edu.uci.ics.asterix.common.config.AsterixExternalProperties;
import edu.uci.ics.asterix.common.config.AsterixMetadataProperties;
import edu.uci.ics.asterix.common.config.AsterixPropertiesAccessor;
import edu.uci.ics.asterix.common.config.AsterixStorageProperties;
import edu.uci.ics.asterix.common.config.AsterixTransactionProperties;
import edu.uci.ics.asterix.common.config.IAsterixPropertiesProvider;
import edu.uci.ics.asterix.common.context.AsterixFileMapManager;
import edu.uci.ics.asterix.common.context.DatasetLifecycleManager;
import edu.uci.ics.asterix.common.exceptions.ACIDException;
import edu.uci.ics.asterix.common.exceptions.AsterixException;
import edu.uci.ics.asterix.common.transactions.IAsterixAppRuntimeContextProvider;
import edu.uci.ics.asterix.common.transactions.ITransactionSubsystem;
import edu.uci.ics.asterix.metadata.bootstrap.MetadataPrimaryIndexes;
import edu.uci.ics.asterix.transaction.management.resource.PersistentLocalResourceRepository;
import edu.uci.ics.asterix.transaction.management.resource.PersistentLocalResourceRepositoryFactory;
import edu.uci.ics.asterix.transaction.management.service.transaction.TransactionSubsystem;
import edu.uci.ics.hyracks.api.application.INCApplicationContext;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.io.IIOManager;
import edu.uci.ics.hyracks.api.lifecycle.ILifeCycleComponent;
import edu.uci.ics.hyracks.api.lifecycle.LifeCycleComponentManager;
import edu.uci.ics.hyracks.storage.am.common.api.IIndexLifecycleManager;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMIOOperationScheduler;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMMergePolicyFactory;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMOperationTracker;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.IVirtualBufferCache;
import edu.uci.ics.hyracks.storage.am.lsm.common.impls.AsynchronousScheduler;
import edu.uci.ics.hyracks.storage.am.lsm.common.impls.PrefixMergePolicyFactory;
import edu.uci.ics.hyracks.storage.common.buffercache.BufferCache;
import edu.uci.ics.hyracks.storage.common.buffercache.ClockPageReplacementStrategy;
import edu.uci.ics.hyracks.storage.common.buffercache.DelayPageCleanerPolicy;
import edu.uci.ics.hyracks.storage.common.buffercache.HeapBufferAllocator;
import edu.uci.ics.hyracks.storage.common.buffercache.IBufferCache;
import edu.uci.ics.hyracks.storage.common.buffercache.ICacheMemoryAllocator;
import edu.uci.ics.hyracks.storage.common.buffercache.IPageCleanerPolicy;
import edu.uci.ics.hyracks.storage.common.buffercache.IPageReplacementStrategy;
import edu.uci.ics.hyracks.storage.common.file.IFileMapManager;
import edu.uci.ics.hyracks.storage.common.file.IFileMapProvider;
import edu.uci.ics.hyracks.storage.common.file.ILocalResourceRepository;
import edu.uci.ics.hyracks.storage.common.file.ILocalResourceRepositoryFactory;
import edu.uci.ics.hyracks.storage.common.file.ResourceIdFactory;
import edu.uci.ics.hyracks.storage.common.file.ResourceIdFactoryProvider;

public class AsterixAppRuntimeContext implements IAsterixAppRuntimeContext, IAsterixPropertiesProvider {
    private static final int METADATA_IO_DEVICE_ID = 0;

    private ILSMMergePolicyFactory metadataMergePolicyFactory;
    private final INCApplicationContext ncApplicationContext;

    private AsterixCompilerProperties compilerProperties;
    private AsterixExternalProperties externalProperties;
    private AsterixMetadataProperties metadataProperties;
    private AsterixStorageProperties storageProperties;
    private AsterixTransactionProperties txnProperties;

    private DatasetLifecycleManager indexLifecycleManager;
    private IFileMapManager fileMapManager;
    private IBufferCache bufferCache;
    private ITransactionSubsystem txnSubsystem;

    private ILSMIOOperationScheduler lsmIOScheduler;
    private ILocalResourceRepository localResourceRepository;
    private ResourceIdFactory resourceIdFactory;
    private IIOManager ioManager;
    private boolean isShuttingdown;

    public AsterixAppRuntimeContext(INCApplicationContext ncApplicationContext) {
        this.ncApplicationContext = ncApplicationContext;
    }

    public void initialize() throws IOException, ACIDException, AsterixException {
        AsterixPropertiesAccessor propertiesAccessor = new AsterixPropertiesAccessor();
        compilerProperties = new AsterixCompilerProperties(propertiesAccessor);
        externalProperties = new AsterixExternalProperties(propertiesAccessor);
        metadataProperties = new AsterixMetadataProperties(propertiesAccessor);
        storageProperties = new AsterixStorageProperties(propertiesAccessor);
        txnProperties = new AsterixTransactionProperties(propertiesAccessor);

        Logger.getLogger("edu.uci.ics").setLevel(externalProperties.getLogLevel());

        fileMapManager = new AsterixFileMapManager();
        ICacheMemoryAllocator allocator = new HeapBufferAllocator();
        IPageReplacementStrategy prs = new ClockPageReplacementStrategy();
        IPageCleanerPolicy pcp = new DelayPageCleanerPolicy(600000);
        ioManager = ncApplicationContext.getRootContext().getIOManager();
        bufferCache = new BufferCache(ioManager, allocator, prs, pcp, fileMapManager,
                storageProperties.getBufferCachePageSize(), storageProperties.getBufferCacheNumPages(),
                storageProperties.getBufferCacheMaxOpenFiles(), ncApplicationContext.getThreadFactory());

        AsynchronousScheduler.INSTANCE.init(ncApplicationContext.getThreadFactory());
        lsmIOScheduler = AsynchronousScheduler.INSTANCE;

        metadataMergePolicyFactory = new PrefixMergePolicyFactory();

        ILocalResourceRepositoryFactory persistentLocalResourceRepositoryFactory = new PersistentLocalResourceRepositoryFactory(
                ioManager);
        localResourceRepository = (PersistentLocalResourceRepository) persistentLocalResourceRepositoryFactory
                .createRepository();
        resourceIdFactory = (new ResourceIdFactoryProvider(localResourceRepository)).createResourceIdFactory();
        indexLifecycleManager = new DatasetLifecycleManager(storageProperties, localResourceRepository,
                MetadataPrimaryIndexes.FIRST_AVAILABLE_USER_DATASET_ID);
        IAsterixAppRuntimeContextProvider asterixAppRuntimeContextProvider = new AsterixAppRuntimeContextProviderForRecovery(
                this);
        txnSubsystem = new TransactionSubsystem(ncApplicationContext.getNodeId(), asterixAppRuntimeContextProvider,
                txnProperties);
        isShuttingdown = false;

        // The order of registration is important. The buffer cache must registered before recovery and transaction managers.
        LifeCycleComponentManager.INSTANCE.register((ILifeCycleComponent) bufferCache);
        LifeCycleComponentManager.INSTANCE.register((ILifeCycleComponent) indexLifecycleManager);
        LifeCycleComponentManager.INSTANCE.register((ILifeCycleComponent) txnSubsystem.getTransactionManager());
        LifeCycleComponentManager.INSTANCE.register((ILifeCycleComponent) txnSubsystem.getLogManager());
        LifeCycleComponentManager.INSTANCE.register((ILifeCycleComponent) txnSubsystem.getLockManager());
        LifeCycleComponentManager.INSTANCE.register((ILifeCycleComponent) txnSubsystem.getRecoveryManager());
    }

    public boolean isShuttingdown() {
        return isShuttingdown;
    }

    public void setShuttingdown(boolean isShuttingdown) {
        this.isShuttingdown = isShuttingdown;
    }

    public void deinitialize() throws HyracksDataException {
    }

    public IBufferCache getBufferCache() {
        return bufferCache;
    }

    public IFileMapProvider getFileMapManager() {
        return fileMapManager;
    }

    public ITransactionSubsystem getTransactionSubsystem() {
        return txnSubsystem;
    }

    public IIndexLifecycleManager getIndexLifecycleManager() {
        return indexLifecycleManager;
    }

    public double getBloomFilterFalsePositiveRate() {
        return storageProperties.getBloomFilterFalsePositiveRate();
    }

    public ILSMIOOperationScheduler getLSMIOScheduler() {
        return lsmIOScheduler;
    }

    public ILocalResourceRepository getLocalResourceRepository() {
        return localResourceRepository;
    }

    public ResourceIdFactory getResourceIdFactory() {
        return resourceIdFactory;
    }

    public IIOManager getIOManager() {
        return ioManager;
    }

    public int getMetaDataIODeviceId() {
        return METADATA_IO_DEVICE_ID;
    }

    @Override
    public AsterixStorageProperties getStorageProperties() {
        return storageProperties;
    }

    @Override
    public AsterixTransactionProperties getTransactionProperties() {
        return txnProperties;
    }

    @Override
    public AsterixCompilerProperties getCompilerProperties() {
        return compilerProperties;
    }

    @Override
    public AsterixMetadataProperties getMetadataProperties() {
        return metadataProperties;
    }

    @Override
    public AsterixExternalProperties getExternalProperties() {
        return externalProperties;
    }

    @Override
    public List<IVirtualBufferCache> getVirtualBufferCaches(int datasetID) {
        return indexLifecycleManager.getVirtualBufferCaches(datasetID);
    }

    @Override
    public ILSMOperationTracker getLSMBTreeOperationTracker(int datasetID) {
        return indexLifecycleManager.getOperationTracker(datasetID);
    }

    @Override
    public ILSMMergePolicyFactory getMetadataMergePolicyFactory() {
        return metadataMergePolicyFactory;
    }

}