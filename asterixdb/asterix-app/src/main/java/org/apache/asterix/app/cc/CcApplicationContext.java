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
package org.apache.asterix.app.cc;

import static org.apache.hyracks.control.common.controllers.ControllerConfig.Option.CLOUD_DEPLOYMENT;

import java.io.IOException;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Supplier;

import org.apache.asterix.app.result.ResultReader;
import org.apache.asterix.common.api.IConfigValidator;
import org.apache.asterix.common.api.IConfigValidatorFactory;
import org.apache.asterix.common.api.ICoordinationService;
import org.apache.asterix.common.api.IMetadataLockManager;
import org.apache.asterix.common.api.INamespacePathResolver;
import org.apache.asterix.common.api.INamespaceResolver;
import org.apache.asterix.common.api.INodeJobTracker;
import org.apache.asterix.common.api.IReceptionist;
import org.apache.asterix.common.api.IReceptionistFactory;
import org.apache.asterix.common.api.IRequestTracker;
import org.apache.asterix.common.cluster.IClusterStateManager;
import org.apache.asterix.common.cluster.IGlobalRecoveryManager;
import org.apache.asterix.common.cluster.IGlobalTxManager;
import org.apache.asterix.common.config.ActiveProperties;
import org.apache.asterix.common.config.BuildProperties;
import org.apache.asterix.common.config.CloudProperties;
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
import org.apache.asterix.common.dataflow.IDataPartitioningProvider;
import org.apache.asterix.common.external.IAdapterFactoryService;
import org.apache.asterix.common.metadata.IMetadataBootstrap;
import org.apache.asterix.common.metadata.IMetadataLockUtil;
import org.apache.asterix.common.replication.INcLifecycleCoordinator;
import org.apache.asterix.common.storage.ICompressionManager;
import org.apache.asterix.common.transactions.IResourceIdManager;
import org.apache.asterix.common.transactions.ITxnIdFactory;
import org.apache.asterix.metadata.utils.DataPartitioningProvider;
import org.apache.asterix.runtime.compression.CompressionManager;
import org.apache.asterix.runtime.job.listener.NodeJobTracker;
import org.apache.asterix.runtime.transaction.ResourceIdManager;
import org.apache.asterix.runtime.utils.BulkTxnIdFactory;
import org.apache.asterix.runtime.utils.ClusterStateManager;
import org.apache.asterix.runtime.utils.NoOpCoordinationService;
import org.apache.asterix.runtime.utils.RequestTracker;
import org.apache.asterix.runtime.utils.RuntimeComponentsProvider;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.api.application.ICCServiceContext;
import org.apache.hyracks.api.client.IHyracksClientConnection;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.job.IJobLifecycleListener;
import org.apache.hyracks.api.result.IResultSet;
import org.apache.hyracks.client.result.ResultSet;
import org.apache.hyracks.control.nc.io.IOManager;
import org.apache.hyracks.ipc.impl.HyracksConnection;
import org.apache.hyracks.storage.common.IStorageManager;
import org.apache.hyracks.util.NetworkUtil;

/*
 * Acts as an holder class for IndexRegistryProvider, AsterixStorageManager
 * instances that are accessed from the NCs. In addition an instance of ICCApplicationContext
 * is stored for access by the CC.
 */
public class CcApplicationContext implements ICcApplicationContext {

    private ICCServiceContext ccServiceCtx;
    private IStorageComponentProvider storageComponentProvider;
    private IGlobalRecoveryManager globalRecoveryManager;
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
    private final CloudProperties cloudProperties;
    private Supplier<IMetadataBootstrap> metadataBootstrapSupplier;
    private volatile HyracksConnection hcc;
    private volatile ResultSet resultSet;
    private Object extensionManager;
    private INcLifecycleCoordinator ftStrategy;
    private IJobLifecycleListener activeLifeCycleListener;
    private IMetadataLockManager mdLockManager;
    private IMetadataLockUtil mdLockUtil;
    private IClusterStateManager clusterStateManager;
    private final INodeJobTracker nodeJobTracker;
    private final ITxnIdFactory txnIdFactory;
    private final ICompressionManager compressionManager;
    private final IReceptionist receptionist;
    private final IRequestTracker requestTracker;
    private final IConfigValidator configValidator;
    private final IAdapterFactoryService adapterFactoryService;
    private final ReentrantReadWriteLock compilationLock = new ReentrantReadWriteLock(true);
    private final IDataPartitioningProvider dataPartitioningProvider;
    private final IGlobalTxManager globalTxManager;
    private final IOManager ioManager;
    private final INamespacePathResolver namespacePathResolver;
    private final INamespaceResolver namespaceResolver;

    public CcApplicationContext(ICCServiceContext ccServiceCtx, HyracksConnection hcc,
            Supplier<IMetadataBootstrap> metadataBootstrapSupplier, IGlobalRecoveryManager globalRecoveryManager,
            INcLifecycleCoordinator ftStrategy, IJobLifecycleListener activeLifeCycleListener,
            IStorageComponentProvider storageComponentProvider, IMetadataLockManager mdLockManager,
            IMetadataLockUtil mdLockUtil, IReceptionistFactory receptionistFactory,
            IConfigValidatorFactory configValidatorFactory, Object extensionManager,
            IAdapterFactoryService adapterFactoryService, IGlobalTxManager globalTxManager, IOManager ioManager,
            CloudProperties cloudProperties, INamespaceResolver namespaceResolver,
            INamespacePathResolver namespacePathResolver) throws AlgebricksException, IOException {
        this.ccServiceCtx = ccServiceCtx;
        this.hcc = hcc;
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
        this.cloudProperties = cloudProperties;
        this.ftStrategy = ftStrategy;
        this.buildProperties = new BuildProperties(propertiesAccessor);
        this.messagingProperties = new MessagingProperties(propertiesAccessor);
        this.nodeProperties = new NodeProperties(propertiesAccessor);
        this.metadataBootstrapSupplier = metadataBootstrapSupplier;
        this.globalRecoveryManager = globalRecoveryManager;
        this.storageComponentProvider = storageComponentProvider;
        this.mdLockManager = mdLockManager;
        this.mdLockUtil = mdLockUtil;
        clusterStateManager = new ClusterStateManager();
        clusterStateManager.setCcAppCtx(this);
        this.resourceIdManager = new ResourceIdManager(clusterStateManager);
        nodeJobTracker = new NodeJobTracker();
        txnIdFactory = new BulkTxnIdFactory();
        compressionManager = new CompressionManager(storageProperties);
        receptionist = receptionistFactory.create();
        requestTracker = new RequestTracker(this);
        configValidator = configValidatorFactory.create();
        this.adapterFactoryService = adapterFactoryService;
        this.namespacePathResolver = namespacePathResolver;
        this.namespaceResolver = namespaceResolver;
        this.globalTxManager = globalTxManager;
        this.ioManager = ioManager;
        dataPartitioningProvider = DataPartitioningProvider.create(this);
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
        HyracksConnection hc = hcc;
        if (!hc.isConnected()) {
            synchronized (this) {
                hc = hcc;
                if (!hc.isConnected()) {
                    try {
                        ResultSet rs = resultSet;
                        resultSet = null;
                        NetworkUtil.closeQuietly(rs);

                        NetworkUtil.closeQuietly(hc);
                        hcc = hc = new HyracksConnection(hcc.getHost(), hcc.getPort());
                    } catch (Exception e) {
                        throw HyracksDataException.create(e);
                    }
                }
            }
        }
        return hc;
    }

    @Override
    public IResultSet getResultSet() throws HyracksDataException {
        ResultSet rs = resultSet;
        if (rs == null) {
            synchronized (this) {
                rs = resultSet;
                if (rs == null) {
                    try {
                        resultSet = rs = ResultReader.createResultSet(getHcc(), ccServiceCtx.getControllerService(),
                                compilerProperties);
                    } catch (Exception e) {
                        throw HyracksDataException.create(e);
                    }
                }
            }
        }
        return rs;
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
    public IMetadataLockUtil getMetadataLockUtil() {
        return mdLockUtil;
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

    @Override
    public IAdapterFactoryService getAdapterFactoryService() {
        return adapterFactoryService;
    }

    @Override
    public ReentrantReadWriteLock getCompilationLock() {
        return compilationLock;
    }

    @Override
    public IDataPartitioningProvider getDataPartitioningProvider() {
        return dataPartitioningProvider;
    }

    @Override
    public INamespaceResolver getNamespaceResolver() {
        return namespaceResolver;
    }

    @Override
    public INamespacePathResolver getNamespacePathResolver() {
        return namespacePathResolver;
    }

    @Override
    public boolean isCloudDeployment() {
        return ccServiceCtx.getAppConfig().getBoolean(CLOUD_DEPLOYMENT);
    }

    @Override
    public CloudProperties getCloudProperties() {
        return cloudProperties;
    }

    @Override
    public IGlobalTxManager getGlobalTxManager() {
        return globalTxManager;
    }

    @Override
    public IOManager getIoManager() {
        return ioManager;
    }
}
