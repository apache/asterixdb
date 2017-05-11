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
import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.common.library.ILibraryManager;
import org.apache.asterix.common.metadata.IMetadataBootstrap;
import org.apache.asterix.common.replication.IFaultToleranceStrategy;
import org.apache.asterix.common.transactions.IResourceIdManager;
import org.apache.hyracks.api.application.ICCServiceContext;
import org.apache.hyracks.api.client.IHyracksClientConnection;
import org.apache.hyracks.api.job.IJobLifecycleListener;
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
    private IFaultToleranceStrategy ftStrategy;
    private IJobLifecycleListener activeLifeCycleListener;

    public CcApplicationContext(ICCServiceContext ccServiceCtx, IHyracksClientConnection hcc,
            ILibraryManager libraryManager, IResourceIdManager resourceIdManager,
            Supplier<IMetadataBootstrap> metadataBootstrapSupplier, IGlobalRecoveryManager globalRecoveryManager,
            IFaultToleranceStrategy ftStrategy, IJobLifecycleListener activeLifeCycleListener,
            IStorageComponentProvider storageComponentProvider)
            throws AsterixException, IOException {
        this.ccServiceCtx = ccServiceCtx;
        this.hcc = hcc;
        this.libraryManager = libraryManager;
        this.resourceIdManager = resourceIdManager;
        this.activeLifeCycleListener = activeLifeCycleListener;
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
        this.hcc = hcc;
        this.buildProperties = new BuildProperties(propertiesAccessor);
        this.messagingProperties = new MessagingProperties(propertiesAccessor);
        this.nodeProperties = new NodeProperties(propertiesAccessor);
        this.metadataBootstrapSupplier = metadataBootstrapSupplier;
        this.globalRecoveryManager = globalRecoveryManager;
        this.storageComponentProvider = storageComponentProvider;
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
    public IHyracksClientConnection getHcc() {
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

    public void setExtensionManager(Object extensionManager) {
        this.extensionManager = extensionManager;
    }

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

    public IMetadataBootstrap getMetadataBootstrap() {
        return metadataBootstrapSupplier.get();
    }

    public IFaultToleranceStrategy getFaultToleranceStrategy() {
        return ftStrategy;
    }

    @Override
    public IJobLifecycleListener getActiveLifecycleListener() {
        return activeLifeCycleListener;
    }

    @Override
    public IStorageComponentProvider getStorageComponentProvider() {
        return storageComponentProvider;
    }
}
