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

import org.apache.asterix.app.nc.NCAppRuntimeContext;
import org.apache.asterix.app.replication.message.StartupTaskRequestMessage;
import org.apache.asterix.common.api.AsterixThreadFactory;
import org.apache.asterix.common.api.IAppRuntimeContext;
import org.apache.asterix.common.config.AsterixExtension;
import org.apache.asterix.common.config.ClusterProperties;
import org.apache.asterix.common.config.IPropertiesProvider;
import org.apache.asterix.common.config.MessagingProperties;
import org.apache.asterix.common.config.MetadataProperties;
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
import org.apache.commons.io.FileUtils;
import org.apache.hyracks.api.application.INCApplicationContext;
import org.apache.hyracks.api.application.INCApplicationEntryPoint;
import org.apache.hyracks.api.job.resource.NodeCapacity;
import org.apache.hyracks.api.messages.IMessageBroker;
import org.apache.hyracks.control.nc.NodeControllerService;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

import java.io.File;
import java.util.Collections;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

public class NCApplicationEntryPoint implements INCApplicationEntryPoint {
    private static final Logger LOGGER = Logger.getLogger(NCApplicationEntryPoint.class.getName());

    @Option(name = "-initial-run", usage = "A flag indicating if it's the first time the NC is started "
            + "(default: false)", required = false)
    public boolean initialRun = false;

    @Option(name = "-virtual-NC", usage = "A flag indicating if this NC is running on virtual cluster "
            + "(default: false)", required = false)
    public boolean virtualNC = false;

    private INCApplicationContext ncApplicationContext = null;
    private IAppRuntimeContext runtimeContext;
    private String nodeId;
    private boolean stopInitiated = false;
    private SystemState systemState;
    private IMessageBroker messageBroker;

    @Override
    public void start(INCApplicationContext ncAppCtx, String[] args) throws Exception {
        CmdLineParser parser = new CmdLineParser(this);
        try {
            parser.parseArgument(args);
        } catch (CmdLineException e) {
            LOGGER.severe(e.getMessage());
            LOGGER.severe("Usage:");
            parser.printUsage(System.err);
            throw e;
        }
        ncAppCtx.setThreadFactory(
                new AsterixThreadFactory(ncAppCtx.getThreadFactory(), ncAppCtx.getLifeCycleComponentManager()));
        ncApplicationContext = ncAppCtx;
        nodeId = ncApplicationContext.getNodeId();
        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info("Starting Asterix node controller: " + nodeId);
        }

        final NodeControllerService controllerService = (NodeControllerService) ncAppCtx.getControllerService();

        if (System.getProperty("java.rmi.server.hostname") == null) {
            System.setProperty("java.rmi.server.hostname",
                    (controllerService).getConfiguration().clusterNetPublicIPAddress);
        }
        runtimeContext = new NCAppRuntimeContext(ncApplicationContext, getExtensions());
        MetadataProperties metadataProperties = ((IPropertiesProvider) runtimeContext).getMetadataProperties();
        if (!metadataProperties.getNodeNames().contains(ncApplicationContext.getNodeId())) {
            if (LOGGER.isLoggable(Level.INFO)) {
                LOGGER.info("Substitute node joining : " + ncApplicationContext.getNodeId());
            }
            updateOnNodeJoin();
        }
        runtimeContext.initialize(initialRun);
        ncApplicationContext.setApplicationObject(runtimeContext);
        MessagingProperties messagingProperties = ((IPropertiesProvider) runtimeContext).getMessagingProperties();
        messageBroker = new NCMessageBroker(controllerService, messagingProperties);
        ncApplicationContext.setMessageBroker(messageBroker);
        MessagingChannelInterfaceFactory interfaceFactory = new MessagingChannelInterfaceFactory(
                (NCMessageBroker) messageBroker, messagingProperties);
        ncApplicationContext.setMessagingChannelInterfaceFactory(interfaceFactory);

        IRecoveryManager recoveryMgr = runtimeContext.getTransactionSubsystem().getRecoveryManager();
        systemState = recoveryMgr.getSystemState();

        if (systemState == SystemState.PERMANENT_DATA_LOSS) {
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

        performLocalCleanUp();
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
            //Clean any temporary files
            performLocalCleanUp();

            //Note: stopping recovery manager will make a sharp checkpoint
            ncApplicationContext.getLifeCycleComponentManager().stopAll(false);
            runtimeContext.deinitialize();
        } else {
            if (LOGGER.isLoggable(Level.INFO)) {
                LOGGER.info("Duplicate attempt to stop ignored: " + nodeId);
            }
        }
    }

    @Override
    public void notifyStartupComplete() throws Exception {
        // Since we don't pass initial run flag in AsterixHyracksIntegrationUtil, we use the virtualNC flag
        if (systemState == SystemState.PERMANENT_DATA_LOSS && (initialRun || virtualNC)) {
            systemState = SystemState.BOOTSTRAPPING;
        }
        // Request startup tasks from CC
        StartupTaskRequestMessage.send((NodeControllerService) ncApplicationContext.getControllerService(),
                systemState);
    }

    @Override
    public NodeCapacity getCapacity() {
        IPropertiesProvider propertiesProvider = (IPropertiesProvider) runtimeContext;
        StorageProperties storageProperties = propertiesProvider.getStorageProperties();
        // Deducts the reserved buffer cache size and memory component size from the maxium heap size,
        // and deducts one core for processing heartbeats.
        long memorySize = Runtime.getRuntime().maxMemory() - storageProperties.getBufferCacheSize()
                - storageProperties.getMemoryComponentGlobalBudget();
        int allCores = Runtime.getRuntime().availableProcessors();
        int maximumCoresForComputation = allCores > 1 ? allCores - 1 : allCores;
        return new NodeCapacity(memorySize, maximumCoresForComputation);
    }

    private void performLocalCleanUp() {
        //Delete working area files from failed jobs
        runtimeContext.getIOManager().deleteWorkspaceFiles();

        //Reclaim storage for temporary datasets.
        String storageDirName = ClusterProperties.INSTANCE.getStorageDirectoryName();
        String[] ioDevices = ((PersistentLocalResourceRepository) runtimeContext.getLocalResourceRepository())
                .getStorageMountingPoints();
        for (String ioDevice : ioDevices) {
            String tempDatasetsDir = ioDevice + storageDirName + File.separator
                    + StoragePathUtil.TEMP_DATASETS_STORAGE_FOLDER;
            FileUtils.deleteQuietly(new File(tempDatasetsDir));
        }

        //TODO
        //Reclaim storage for orphaned index artifacts in NCs.
        //Note: currently LSM indexes invalid components are deleted when an index is activated.
    }

    private void updateOnNodeJoin() {
        MetadataProperties metadataProperties = ((IPropertiesProvider) runtimeContext).getMetadataProperties();
        if (!metadataProperties.getNodeNames().contains(nodeId)) {
            metadataProperties.getNodeNames().add(nodeId);
            Cluster cluster = ClusterProperties.INSTANCE.getCluster();
            if (cluster == null) {
                throw new IllegalStateException("No cluster configuration found for this instance");
            }
            String asterixInstanceName = metadataProperties.getInstanceName();
            TransactionProperties txnProperties = ((IPropertiesProvider) runtimeContext).getTransactionProperties();
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
}
