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

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import org.apache.asterix.api.http.server.ServletConstants;
import org.apache.asterix.api.http.server.StorageApiServlet;
import org.apache.asterix.app.nc.NCAppRuntimeContext;
import org.apache.asterix.app.replication.message.RegistrationTasksRequestMessage;
import org.apache.asterix.common.api.AsterixThreadFactory;
import org.apache.asterix.common.api.INcApplicationContext;
import org.apache.asterix.common.config.AsterixExtension;
import org.apache.asterix.common.config.ExternalProperties;
import org.apache.asterix.common.config.GlobalConfig;
import org.apache.asterix.common.config.MessagingProperties;
import org.apache.asterix.common.config.MetadataProperties;
import org.apache.asterix.common.config.NodeProperties;
import org.apache.asterix.common.config.StorageProperties;
import org.apache.asterix.common.transactions.Checkpoint;
import org.apache.asterix.common.transactions.IRecoveryManager;
import org.apache.asterix.common.transactions.IRecoveryManager.SystemState;
import org.apache.asterix.common.utils.PrintUtil;
import org.apache.asterix.common.utils.Servlets;
import org.apache.asterix.common.utils.StorageConstants;
import org.apache.asterix.common.utils.StoragePathUtil;
import org.apache.asterix.messaging.MessagingChannelInterfaceFactory;
import org.apache.asterix.messaging.NCMessageBroker;
import org.apache.asterix.transaction.management.resource.PersistentLocalResourceRepository;
import org.apache.asterix.util.MetadataBuiltinFunctions;
import org.apache.hyracks.api.application.INCServiceContext;
import org.apache.hyracks.api.application.IServiceContext;
import org.apache.hyracks.api.client.NodeStatus;
import org.apache.hyracks.api.config.IConfigManager;
import org.apache.hyracks.api.control.CcId;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.IFileDeviceResolver;
import org.apache.hyracks.api.job.resource.NodeCapacity;
import org.apache.hyracks.api.messages.IMessageBroker;
import org.apache.hyracks.control.common.controllers.NCConfig;
import org.apache.hyracks.control.nc.BaseNCApplication;
import org.apache.hyracks.control.nc.NodeControllerService;
import org.apache.hyracks.http.server.HttpServer;
import org.apache.hyracks.http.server.WebManager;
import org.apache.hyracks.util.LoggingConfigUtil;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class NCApplication extends BaseNCApplication {
    private static final Logger LOGGER = LogManager.getLogger();

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
        if (LOGGER.isInfoEnabled()) {
            LOGGER.info("Starting Asterix node controller: " + nodeId);
        }
        configureLoggingLevel(ncServiceCtx.getAppConfig().getLoggingLevel(ExternalProperties.Option.LOG_LEVEL));

        final NodeControllerService controllerService = (NodeControllerService) ncServiceCtx.getControllerService();

        if (System.getProperty("java.rmi.server.hostname") == null) {
            System.setProperty("java.rmi.server.hostname",
                    (controllerService).getConfiguration().getClusterPublicAddress());
        }
        MetadataBuiltinFunctions.init();
        runtimeContext = new NCAppRuntimeContext(ncServiceCtx, getExtensions());
        MetadataProperties metadataProperties = runtimeContext.getMetadataProperties();
        if (!metadataProperties.getNodeNames().contains(this.ncServiceCtx.getNodeId())) {
            if (LOGGER.isInfoEnabled()) {
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
        final Checkpoint latestCheckpoint = runtimeContext.getTransactionSubsystem().getCheckpointManager().getLatest();
        if (latestCheckpoint != null && latestCheckpoint.getStorageVersion() != StorageConstants.VERSION) {
            throw new IllegalStateException(
                    String.format("Storage version mismatch.. Current version (%s). On disk version: (%s)",
                            StorageConstants.VERSION, latestCheckpoint.getStorageVersion()));
        }
        if (LOGGER.isInfoEnabled()) {
            IRecoveryManager recoveryMgr = runtimeContext.getTransactionSubsystem().getRecoveryManager();
            LOGGER.info("System state: " + recoveryMgr.getSystemState());
            LOGGER.info("Node ID: " + nodeId);
            LOGGER.info("Stores: " + PrintUtil.toString(metadataProperties.getStores()));
        }
        webManager = new WebManager();
        performLocalCleanUp();
    }

    @Override
    protected void configureLoggingLevel(Level level) {
        super.configureLoggingLevel(level);
        LoggingConfigUtil.defaultIfMissing(GlobalConfig.ASTERIX_LOGGER_NAME, level);
    }

    protected void configureServers() throws Exception {
        HttpServer apiServer = new HttpServer(webManager.getBosses(), webManager.getWorkers(),
                getApplicationContext().getExternalProperties().getNcApiPort());
        apiServer.setAttribute(ServletConstants.SERVICE_CONTEXT_ATTR, ncServiceCtx);
        apiServer.addServlet(new StorageApiServlet(apiServer.ctx(), getApplicationContext(), Servlets.STORAGE));
        webManager.add(apiServer);
    }

    protected List<AsterixExtension> getExtensions() {
        return Collections.emptyList();
    }

    @Override
    public void stop() throws Exception {
        if (!stopInitiated) {
            runtimeContext.setShuttingdown(true);
            stopInitiated = true;
            if (LOGGER.isInfoEnabled()) {
                LOGGER.info("Stopping Asterix node controller: " + nodeId);
            }

            webManager.stop();

            //Clean any temporary files
            performLocalCleanUp();

            //Note: stopping recovery manager will make a sharp checkpoint
            ncServiceCtx.getLifeCycleComponentManager().stopAll(false);
            runtimeContext.deinitialize();
        } else {
            if (LOGGER.isInfoEnabled()) {
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
        if (state == SystemState.PERMANENT_DATA_LOSS
                && (nodeProperties.isInitialRun() || nodeProperties.isVirtualNc())) {
            state = SystemState.BOOTSTRAPPING;
        }
        // Request registration tasks from CC (we only do this from our primary CC, in the case of multiple CCs)
        final NodeControllerService ncControllerService = (NodeControllerService) ncServiceCtx.getControllerService();
        RegistrationTasksRequestMessage.send(ncControllerService.getPrimaryCcId(), ncControllerService,
                NodeStatus.BOOTING, state);
        startupCompleted = true;
    }

    @Override
    public void onRegisterNode(CcId ccId) throws Exception {
        if (startupCompleted) {
            /*
             * If the node completed its startup before, then this is a re-registration with
             * the primary (or supplemental) CC and therefore the system state should be HEALTHY and the node status
             * is ACTIVE
             */
            RegistrationTasksRequestMessage.send(ccId, (NodeControllerService) ncServiceCtx.getControllerService(),
                    NodeStatus.ACTIVE, SystemState.HEALTHY);
        }
    }

    @Override
    public NodeCapacity getCapacity() {
        StorageProperties storageProperties = runtimeContext.getStorageProperties();
        final long memorySize = storageProperties.getJobExecutionMemoryBudget();
        int allCores = Runtime.getRuntime().availableProcessors();
        return new NodeCapacity(memorySize, allCores);
    }

    private void performLocalCleanUp() throws HyracksDataException {
        //Delete working area files from failed jobs
        runtimeContext.getIoManager().deleteWorkspaceFiles();
        // Reclaim storage for orphaned index artifacts in NCs.
        final Set<Integer> nodePartitions = runtimeContext.getReplicaManager().getPartitions();
        final PersistentLocalResourceRepository localResourceRepository =
                (PersistentLocalResourceRepository) runtimeContext.getLocalResourceRepository();
        for (Integer partition : nodePartitions) {
            localResourceRepository.cleanup(partition);
        }
    }

    private void updateOnNodeJoin() {
        MetadataProperties metadataProperties = runtimeContext.getMetadataProperties();
        if (!metadataProperties.getNodeNames().contains(nodeId)) {
            NCConfig ncConfig = ((NodeControllerService) ncServiceCtx.getControllerService()).getConfiguration();
            ncConfig.getConfigManager().ensureNode(nodeId);
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
