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
package org.apache.asterix.hyracks.bootstrap;

import java.io.File;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.asterix.app.nc.NCAppRuntimeContext;
import org.apache.asterix.app.replication.message.RegistrationTasksRequestMessage;
import org.apache.asterix.common.api.AsterixThreadFactory;
import org.apache.asterix.common.api.INcApplicationContext;
import org.apache.asterix.common.config.AsterixExtension;
import org.apache.asterix.common.config.ClusterProperties;
import org.apache.asterix.common.config.ExternalProperties;
import org.apache.asterix.common.config.MessagingProperties;
import org.apache.asterix.common.config.MetadataProperties;
import org.apache.asterix.common.config.NodeProperties;
import org.apache.asterix.common.config.StorageProperties;
import org.apache.asterix.common.config.TransactionProperties;
import org.apache.asterix.common.transactions.IRecoveryManager;
import org.apache.asterix.common.transactions.IRecoveryManager.SystemState;
import org.apache.asterix.common.utils.PrintUtil;
import org.apache.asterix.common.utils.StoragePathUtil;
import org.apache.asterix.event.schema.cluster.Cluster;
import org.apache.asterix.event.schema.cluster.Node;
import org.apache.asterix.messaging.MessagingChannelInterfaceFactory;
import org.apache.asterix.messaging.NCMessageBroker;
import org.apache.asterix.transaction.management.resource.PersistentLocalResourceRepository;
import org.apache.hyracks.api.application.INCServiceContext;
import org.apache.hyracks.api.application.IServiceContext;
import org.apache.hyracks.api.client.NodeStatus;
import org.apache.hyracks.api.config.IConfigManager;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.IFileDeviceResolver;
import org.apache.hyracks.api.job.resource.NodeCapacity;
import org.apache.hyracks.api.messages.IMessageBroker;
import org.apache.hyracks.api.util.IoUtil;
import org.apache.hyracks.control.common.controllers.NCConfig;
import org.apache.hyracks.control.nc.BaseNCApplication;
import org.apache.hyracks.control.nc.NodeControllerService;
import org.apache.hyracks.http.server.WebManager;

public class NCApplication extends BaseNCApplication {
    private static final Logger LOGGER = Logger.getLogger(NCApplication.class.getName());

    protected INCServiceContext ncServiceCtx;
    private INcApplicationContext runtimeContext;
    private String nodeId;
    private boolean stopInitiated;
    private boolean startupCompleted;
    protected WebManager webManager;

    @Override
    public void registerConfig(IConfigManager configManager) {
        super.registerConfig(configManager);
        ApplicationConfigurator.registerConfigOptions(configManager);
    }

    @Override
    public void init(IServiceContext serviceCtx) throws Exception {
        ncServiceCtx = (INCServiceContext) serviceCtx;
        ncServiceCtx.setThreadFactory(
                new AsterixThreadFactory(ncServiceCtx.getThreadFactory(), ncServiceCtx.getLifeCycleComponentManager()));
    }

    @Override
    public void start(String[] args) throws Exception {
        if (args.length > 0) {
            throw new IllegalArgumentException("Unrecognized argument(s): " + Arrays.toString(args));
        }
        nodeId = this.ncServiceCtx.getNodeId();
        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info("Starting Asterix node controller: " + nodeId);
        }
        configureLoggingLevel(ncServiceCtx.getAppConfig().getLoggingLevel(ExternalProperties.Option.LOG_LEVEL));

        final NodeControllerService controllerService = (NodeControllerService) ncServiceCtx.getControllerService();

        if (System.getProperty("java.rmi.server.hostname") == null) {
            System.setProperty("java.rmi.server.hostname",
                    (controllerService).getConfiguration().getClusterPublicAddress());
        }
        runtimeContext = new NCAppRuntimeContext(ncServiceCtx, getExtensions());
        MetadataProperties metadataProperties = runtimeContext.getMetadataProperties();
        if (!metadataProperties.getNodeNames().contains(this.ncServiceCtx.getNodeId())) {
            if (LOGGER.isLoggable(Level.INFO)) {
                LOGGER.info("Substitute node joining : " + this.ncServiceCtx.getNodeId());
            }
            updateOnNodeJoin();
        }
        runtimeContext.initialize(runtimeContext.getNodeProperties().isInitialRun());
        MessagingProperties messagingProperties = runtimeContext.getMessagingProperties();
        IMessageBroker messageBroker = new NCMessageBroker(controllerService, messagingProperties);
        this.ncServiceCtx.setMessageBroker(messageBroker);
        MessagingChannelInterfaceFactory interfaceFactory =
                new MessagingChannelInterfaceFactory((NCMessageBroker) messageBroker, messagingProperties);
        this.ncServiceCtx.setMessagingChannelInterfaceFactory(interfaceFactory);

        IRecoveryManager recoveryMgr = runtimeContext.getTransactionSubsystem().getRecoveryManager();
        final SystemState stateOnStartup = recoveryMgr.getSystemState();
        if (stateOnStartup == SystemState.PERMANENT_DATA_LOSS) {
            if (LOGGER.isLoggable(Level.INFO)) {
                LOGGER.info("System state: " + SystemState.PERMANENT_DATA_LOSS);
                LOGGER.info("Node ID: " + nodeId);
                LOGGER.info("Stores: " + PrintUtil.toString(metadataProperties.getStores()));
                LOGGER.info("Root Metadata Store: " + metadataProperties.getStores().get(nodeId)[0]);
            }
            PersistentLocalResourceRepository localResourceRepository =
                    (PersistentLocalResourceRepository) runtimeContext.getLocalResourceRepository();
            localResourceRepository.initializeNewUniverse(ClusterProperties.INSTANCE.getStorageDirectoryName());
        }

        webManager = new WebManager();

        performLocalCleanUp();
    }

    @Override
    protected void configureLoggingLevel(Level level) {
        super.configureLoggingLevel(level);
        Logger.getLogger("org.apache.asterix").setLevel(level);
    }

    protected void configureServers() throws Exception {
        // override to start web services on NC nodes
    }

    protected List<AsterixExtension> getExtensions() {
        return Collections.emptyList();
    }

    @Override
    public void stop() throws Exception {
        if (!stopInitiated) {
            runtimeContext.setShuttingdown(true);
            stopInitiated = true;
            if (LOGGER.isLoggable(Level.INFO)) {
                LOGGER.info("Stopping Asterix node controller: " + nodeId);
            }

            webManager.stop();

            //Clean any temporary files
            performLocalCleanUp();

            //Note: stopping recovery manager will make a sharp checkpoint
            ncServiceCtx.getLifeCycleComponentManager().stopAll(false);
            runtimeContext.deinitialize();
        } else {
            if (LOGGER.isLoggable(Level.INFO)) {
                LOGGER.info("Duplicate attempt to stop ignored: " + nodeId);
            }
        }
    }

    @Override
    public void preStop() throws Exception {
        runtimeContext.preStop();
    }

    @Override
    public void startupCompleted() throws Exception {
        // configure servlets after joining the cluster, so we can create HyracksClientConnection
        configureServers();
        webManager.start();

        // Since we don't pass initial run flag in AsterixHyracksIntegrationUtil, we use the virtualNC flag
        final NodeProperties nodeProperties = runtimeContext.getNodeProperties();
        IRecoveryManager recoveryMgr = runtimeContext.getTransactionSubsystem().getRecoveryManager();
        SystemState state = recoveryMgr.getSystemState();
        if (state == SystemState.PERMANENT_DATA_LOSS && (nodeProperties.isInitialRun() || nodeProperties
                .isVirtualNc())) {
            state = SystemState.BOOTSTRAPPING;
        }
        // Request registration tasks from CC
        RegistrationTasksRequestMessage.send((NodeControllerService) ncServiceCtx.getControllerService(),
                NodeStatus.BOOTING, state);
        startupCompleted = true;
    }

    @Override
    public void onRegisterNode() throws Exception {
        if (startupCompleted) {
            /*
             * If the node completed its startup before, then this is a re-registration with
             * the CC and therefore the system state should be HEALTHY and the node status is ACTIVE
             */
            RegistrationTasksRequestMessage.send((NodeControllerService) ncServiceCtx.getControllerService(),
                    NodeStatus.ACTIVE, SystemState.HEALTHY);
        }
    }

    @Override
    public NodeCapacity getCapacity() {
        StorageProperties storageProperties = runtimeContext.getStorageProperties();
        // Deducts the reserved buffer cache size and memory component size from the maxium heap size,
        // and deducts one core for processing heartbeats.
        long memorySize = Runtime.getRuntime().maxMemory() - storageProperties.getBufferCacheSize()
                - storageProperties.getMemoryComponentGlobalBudget();
        int allCores = Runtime.getRuntime().availableProcessors();
        int maximumCoresForComputation = allCores > 1 ? allCores - 1 : allCores;
        return new NodeCapacity(memorySize, maximumCoresForComputation);
    }

    private void performLocalCleanUp() throws HyracksDataException {
        //Delete working area files from failed jobs
        runtimeContext.getIoManager().deleteWorkspaceFiles();

        //TODO
        //Reclaim storage for orphaned index artifacts in NCs.
        //Note: currently LSM indexes invalid components are deleted when an index is activated.
    }

    private void updateOnNodeJoin() {
        MetadataProperties metadataProperties = runtimeContext.getMetadataProperties();
        if (!metadataProperties.getNodeNames().contains(nodeId)) {
            Cluster cluster = ClusterProperties.INSTANCE.getCluster();
            if (cluster == null) {
                throw new IllegalStateException("No cluster configuration found for this instance");
            }
            NCConfig ncConfig = ((NodeControllerService) ncServiceCtx.getControllerService()).getConfiguration();
            ncConfig.getConfigManager().ensureNode(nodeId);
            String asterixInstanceName = metadataProperties.getInstanceName();
            TransactionProperties txnProperties = runtimeContext.getTransactionProperties();
            Node self = null;
            List<Node> nodes;
            if (cluster.getSubstituteNodes() != null) {
                nodes = cluster.getSubstituteNodes().getNode();
            } else {
                throw new IllegalStateException("Unknown node joining the cluster");
            }
            for (Node node : nodes) {
                String ncId = asterixInstanceName + "_" + node.getId();
                if (ncId.equalsIgnoreCase(nodeId)) {
                    String storeDir = ClusterProperties.INSTANCE.getStorageDirectoryName();
                    String nodeIoDevices = node.getIodevices() == null ? cluster.getIodevices() : node.getIodevices();
                    String[] ioDevicePaths = nodeIoDevices.trim().split(",");
                    for (int i = 0; i < ioDevicePaths.length; i++) {
                        // construct full store path
                        ioDevicePaths[i] += File.separator + storeDir;
                    }
                    metadataProperties.getStores().put(nodeId, ioDevicePaths);

                    String coredumpPath = node.getLogDir() == null ? cluster.getLogDir() : node.getLogDir();
                    metadataProperties.getCoredumpPaths().put(nodeId, coredumpPath);

                    String txnLogDir = node.getTxnLogDir() == null ? cluster.getTxnLogDir() : node.getTxnLogDir();
                    txnProperties.getLogDirectories().put(nodeId, txnLogDir);

                    if (LOGGER.isLoggable(Level.INFO)) {
                        LOGGER.info("Store set to : " + storeDir);
                        LOGGER.info("Coredump dir set to : " + coredumpPath);
                        LOGGER.info("Transaction log dir set to :" + txnLogDir);
                    }
                    self = node;
                    break;
                }
            }
            if (self != null) {
                cluster.getSubstituteNodes().getNode().remove(self);
                cluster.getNode().add(self);
            } else {
                throw new IllegalStateException("Unknown node joining the cluster");
            }
        }
    }

    @Override
    public INcApplicationContext getApplicationContext() {
        return runtimeContext;
    }

    @Override
    public IFileDeviceResolver getFileDeviceResolver() {
        return (relPath, devices) -> {
            int ioDeviceIndex = Math.abs(StoragePathUtil.getPartitionNumFromRelativePath(relPath) % devices.size());
            return devices.get(ioDeviceIndex);
        };
    }
}
