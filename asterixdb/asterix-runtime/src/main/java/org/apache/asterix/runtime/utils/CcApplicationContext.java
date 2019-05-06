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
package org.apache.asterix.runtime.utils;

import java.io.IOException;
import java.util.function.Supplier;

import org.apache.asterix.common.api.IConfigValidator;
import org.apache.asterix.common.api.IConfigValidatorFactory;
import org.apache.asterix.common.api.ICoordinationService;
import org.apache.asterix.common.api.IMetadataLockManager;
import org.apache.asterix.common.api.INodeJobTracker;
import org.apache.asterix.common.api.IReceptionist;
import org.apache.asterix.common.api.IReceptionistFactory;
import org.apache.asterix.common.api.IRequestTracker;
import org.apache.asterix.common.cluster.IClusterStateManager;
import org.apache.asterix.common.cluster.IGlobalRecoveryManager;
import org.apache.asterix.common.config.ActiveProperties;
import org.apache.asterix.common.config.BuildProperties;
import org.apache.asterix.common.config.CompilerProperties;
import org.apache.asterix.common.config.ExtensionProperties;
import org.apache.asterix.common.config.ExternalProperties;
import org.apache.asterix.common.config.MessagingProperties;
import org.apache.asterix.common.config.MetadataProperties;
import org.apache.asterix.common.config.NodeProperties;
import org.apache.asterix.common.config.PropertiesAccessor;
import org.apache.asterix.common.config.ReplicationProperties;
import org.apache.asterix.common.config.StorageProperties;
import org.apache.asterix.common.config.TransactionProperties;
import org.apache.asterix.common.context.IStorageComponentProvider;
import org.apache.asterix.common.dataflow.ICcApplicationContext;
import org.apache.asterix.common.library.ILibraryManager;
import org.apache.asterix.common.metadata.IMetadataBootstrap;
import org.apache.asterix.common.replication.INcLifecycleCoordinator;
import org.apache.asterix.common.storage.ICompressionManager;
import org.apache.asterix.common.transactions.IResourceIdManager;
import org.apache.asterix.common.transactions.ITxnIdFactory;
import org.apache.asterix.runtime.compression.CompressionManager;
import org.apache.asterix.runtime.job.listener.NodeJobTracker;
import org.apache.asterix.runtime.transaction.ResourceIdManager;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.api.application.ICCServiceContext;
import org.apache.hyracks.api.client.IHyracksClientConnection;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.job.IJobLifecycleListener;
import org.apache.hyracks.ipc.impl.HyracksConnection;
import org.apache.hyracks.storage.common.IStorageManager;

/*
 * Acts as an holder class for IndexRegistryProvider, AsterixStorageManager
 * instances that are accessed from the NCs. In addition an instance of ICCApplicationContext
 * is stored for access by the CC.
 */
public class CcApplicationContext implements ICcApplicationContext {

    private ICCServiceContext ccServiceCtx;
    private IStorageComponentProvider storageComponentProvider;
    private IGlobalRecoveryManager globalRecoveryManager;
    private ILibraryManager libraryManager;
    private IResourceIdManager resourceIdManager;
    private CompilerProperties compilerProperties;
    private ExternalProperties externalProperties;
    private MetadataProperties metadataProperties;
    private StorageProperties storageProperties;
    private TransactionProperties txnProperties;
    private ActiveProperties activeProperties;
    private BuildProperties buildProperties;
    private ReplicationProperties replicationProperties;
    private ExtensionProperties extensionProperties;
    private MessagingProperties messagingProperties;
    private NodeProperties nodeProperties;
    private Supplier<IMetadataBootstrap> metadataBootstrapSupplier;
    private IHyracksClientConnection hcc;
    private Object extensionManager;
    private INcLifecycleCoordinator ftStrategy;
    private IJobLifecycleListener activeLifeCycleListener;
    private IMetadataLockManager mdLockManager;
    private IClusterStateManager clusterStateManager;
    private final INodeJobTracker nodeJobTracker;
    private final ITxnIdFactory txnIdFactory;
    private final ICompressionManager compressionManager;
    private final IReceptionist receptionist;
    private final IRequestTracker requestTracker;
    private final IConfigValidator configValidator;

    public CcApplicationContext(ICCServiceContext ccServiceCtx, IHyracksClientConnection hcc,
            ILibraryManager libraryManager, Supplier<IMetadataBootstrap> metadataBootstrapSupplier,
            IGlobalRecoveryManager globalRecoveryManager, INcLifecycleCoordinator ftStrategy,
            IJobLifecycleListener activeLifeCycleListener, IStorageComponentProvider storageComponentProvider,
            IMetadataLockManager mdLockManager, IReceptionistFactory receptionistFactory,
            IConfigValidatorFactory configValidatorFactory, Object extensionManager)
            throws AlgebricksException, IOException {
        this.ccServiceCtx = ccServiceCtx;
        this.hcc = hcc;
        this.libraryManager = libraryManager;
        this.activeLifeCycleListener = activeLifeCycleListener;
        this.extensionManager = extensionManager;
        // Determine whether to use old-style asterix-configuration.xml or new-style configuration.
        // QQQ strip this out eventually
        PropertiesAccessor propertiesAccessor = PropertiesAccessor.getInstance(ccServiceCtx.getAppConfig());
        compilerProperties = new CompilerProperties(propertiesAccessor);
        externalProperties = new ExternalProperties(propertiesAccessor);
        metadataProperties = new MetadataProperties(propertiesAccessor);
        storageProperties = new StorageProperties(propertiesAccessor);
        txnProperties = new TransactionProperties(propertiesAccessor);
        activeProperties = new ActiveProperties(propertiesAccessor);
        extensionProperties = new ExtensionProperties(propertiesAccessor);
        replicationProperties = new ReplicationProperties(propertiesAccessor);
        this.ftStrategy = ftStrategy;
        this.buildProperties = new BuildProperties(propertiesAccessor);
        this.messagingProperties = new MessagingProperties(propertiesAccessor);
        this.nodeProperties = new NodeProperties(propertiesAccessor);
        this.metadataBootstrapSupplier = metadataBootstrapSupplier;
        this.globalRecoveryManager = globalRecoveryManager;
        this.storageComponentProvider = storageComponentProvider;
        this.mdLockManager = mdLockManager;
        clusterStateManager = new ClusterStateManager();
        clusterStateManager.setCcAppCtx(this);
        this.resourceIdManager = new ResourceIdManager(clusterStateManager);
        nodeJobTracker = new NodeJobTracker();
        txnIdFactory = new BulkTxnIdFactory();
        compressionManager = new CompressionManager(storageProperties);
        receptionist = receptionistFactory.create();
        requestTracker = new RequestTracker(this);
        configValidator = configValidatorFactory.create();
    }

    @Override
    public ICCServiceContext getServiceContext() {
        return ccServiceCtx;
    }

    @Override
    public StorageProperties getStorageProperties() {
        return storageProperties;
    }

    @Override
    public TransactionProperties getTransactionProperties() {
        return txnProperties;
    }

    @Override
    public CompilerProperties getCompilerProperties() {
        return compilerProperties;
    }

    @Override
    public MetadataProperties getMetadataProperties() {
        return metadataProperties;
    }

    @Override
    public ExternalProperties getExternalProperties() {
        return externalProperties;
    }

    @Override
    public ActiveProperties getActiveProperties() {
        return activeProperties;
    }

    @Override
    public BuildProperties getBuildProperties() {
        return buildProperties;
    }

    @Override
    public IHyracksClientConnection getHcc() throws HyracksDataException {
        if (!hcc.isConnected()) {
            synchronized (this) {
                if (!hcc.isConnected()) {
                    try {
                        hcc = new HyracksConnection(hcc.getHost(), hcc.getPort());
                    } catch (Exception e) {
                        throw HyracksDataException.create(e);
                    }
                }
            }
        }
        return hcc;
    }

    @Override
    public IStorageManager getStorageManager() {
        return RuntimeComponentsProvider.RUNTIME_PROVIDER;
    }

    @Override
    public ReplicationProperties getReplicationProperties() {
        return replicationProperties;
    }

    @Override
    public IGlobalRecoveryManager getGlobalRecoveryManager() {
        return globalRecoveryManager;
    }

    @Override
    public ILibraryManager getLibraryManager() {
        return libraryManager;
    }

    @Override
    public Object getExtensionManager() {
        return extensionManager;
    }

    @Override
    public ExtensionProperties getExtensionProperties() {
        return extensionProperties;
    }

    @Override
    public MessagingProperties getMessagingProperties() {
        return messagingProperties;
    }

    @Override
    public NodeProperties getNodeProperties() {
        return nodeProperties;
    }

    @Override
    public IResourceIdManager getResourceIdManager() {
        return resourceIdManager;
    }

    @Override
    public IMetadataBootstrap getMetadataBootstrap() {
        return metadataBootstrapSupplier.get();
    }

    @Override
    public INcLifecycleCoordinator getNcLifecycleCoordinator() {
        return ftStrategy;
    }

    @Override
    public IJobLifecycleListener getActiveNotificationHandler() {
        return activeLifeCycleListener;
    }

    @Override
    public IStorageComponentProvider getStorageComponentProvider() {
        return storageComponentProvider;
    }

    @Override
    public IMetadataLockManager getMetadataLockManager() {
        return mdLockManager;
    }

    @Override
    public IClusterStateManager getClusterStateManager() {
        return clusterStateManager;
    }

    @Override
    public INodeJobTracker getNodeJobTracker() {
        return nodeJobTracker;
    }

    @Override
    public ICoordinationService getCoordinationService() {
        return NoOpCoordinationService.INSTANCE;
    }

    @Override
    public ITxnIdFactory getTxnIdFactory() {
        return txnIdFactory;
    }

    @Override
    public ICompressionManager getCompressionManager() {
        return compressionManager;
    }

    @Override
    public IReceptionist getReceptionist() {
        return receptionist;
    }

    @Override
    public IConfigValidator getConfigValidator() {
        return configValidator;
    }

    @Override
    public IRequestTracker getRequestTracker() {
        return requestTracker;
    }
}
