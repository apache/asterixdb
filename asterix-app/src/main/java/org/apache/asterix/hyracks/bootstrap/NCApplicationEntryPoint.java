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
import java.io.IOException;
import java.rmi.server.UnicastRemoteObject;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.asterix.api.common.AsterixAppRuntimeContext;
import org.apache.asterix.common.api.AsterixThreadFactory;
import org.apache.asterix.common.api.IAsterixAppRuntimeContext;
import org.apache.asterix.common.config.AsterixMetadataProperties;
import org.apache.asterix.common.config.AsterixReplicationProperties;
import org.apache.asterix.common.config.AsterixTransactionProperties;
import org.apache.asterix.common.config.IAsterixPropertiesProvider;
import org.apache.asterix.common.messaging.api.INCMessageBroker;
import org.apache.asterix.common.replication.IRemoteRecoveryManager;
import org.apache.asterix.common.transactions.IRecoveryManager;
import org.apache.asterix.common.transactions.IRecoveryManager.SystemState;
import org.apache.asterix.event.schema.cluster.Cluster;
import org.apache.asterix.event.schema.cluster.Node;
import org.apache.asterix.messaging.NCMessageBroker;
import org.apache.asterix.metadata.MetadataManager;
import org.apache.asterix.metadata.MetadataNode;
import org.apache.asterix.metadata.api.IAsterixStateProxy;
import org.apache.asterix.metadata.api.IMetadataNode;
import org.apache.asterix.metadata.bootstrap.MetadataBootstrap;
import org.apache.asterix.metadata.utils.SplitsAndConstraintsUtil;
import org.apache.asterix.om.util.AsterixClusterProperties;
import org.apache.asterix.transaction.management.resource.PersistentLocalResourceRepository;
import org.apache.asterix.transaction.management.service.recovery.RecoveryManager;
import org.apache.commons.io.FileUtils;
import org.apache.hyracks.api.application.INCApplicationContext;
import org.apache.hyracks.api.application.INCApplicationEntryPoint;
import org.apache.hyracks.api.lifecycle.ILifeCycleComponentManager;
import org.apache.hyracks.api.lifecycle.LifeCycleComponentManager;
import org.apache.hyracks.api.messages.IMessageBroker;
import org.apache.hyracks.control.nc.NodeControllerService;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

public class NCApplicationEntryPoint implements INCApplicationEntryPoint {
    private static final Logger LOGGER = Logger.getLogger(NCApplicationEntryPoint.class.getName());

    @Option(name = "-metadata-port", usage = "IP port to bind metadata listener (default: random port)", required = false)
    public int metadataRmiPort = 0;

    @Option(name = "-initial-run", usage = "A flag indicating if it's the first time the NC is started (default: false)", required = false)
    public boolean initialRun = false;

    private INCApplicationContext ncApplicationContext = null;
    private IAsterixAppRuntimeContext runtimeContext;
    private String nodeId;
    private boolean isMetadataNode = false;
    private boolean stopInitiated = false;
    private SystemState systemState = SystemState.NEW_UNIVERSE;
    private boolean performedRemoteRecovery = false;
    private boolean replicationEnabled = false;
    private IMessageBroker messageBroker;

    @Override
    public void start(INCApplicationContext ncAppCtx, String[] args) throws Exception {
        CmdLineParser parser = new CmdLineParser(this);

        try {
            parser.parseArgument(args);
        } catch (CmdLineException e) {
            System.err.println(e.getMessage());
            System.err.println("Usage:");
            parser.printUsage(System.err);
            throw e;
        }

        ncAppCtx.setThreadFactory(new AsterixThreadFactory(ncAppCtx.getLifeCycleComponentManager()));
        ncApplicationContext = ncAppCtx;
        messageBroker = new NCMessageBroker((NodeControllerService) ncAppCtx.getControllerService());
        ncApplicationContext.setMessageBroker(messageBroker);
        nodeId = ncApplicationContext.getNodeId();
        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info("Starting Asterix node controller: " + nodeId);
        }

        runtimeContext = new AsterixAppRuntimeContext(ncApplicationContext);
        AsterixMetadataProperties metadataProperties = ((IAsterixPropertiesProvider) runtimeContext)
                .getMetadataProperties();
        if (!metadataProperties.getNodeNames().contains(ncApplicationContext.getNodeId())) {
            if (LOGGER.isLoggable(Level.INFO)) {
                LOGGER.info("Substitute node joining : " + ncApplicationContext.getNodeId());
            }
            updateOnNodeJoin();
        }
        runtimeContext.initialize(initialRun);
        ncApplicationContext.setApplicationObject(runtimeContext);

        //if replication is enabled, check if there is a replica for this node
        AsterixReplicationProperties asterixReplicationProperties = ((IAsterixPropertiesProvider) runtimeContext)
                .getReplicationProperties();

        replicationEnabled = asterixReplicationProperties.isReplicationEnabled();

        if (initialRun) {
            LOGGER.info("System is being initialized. (first run)");
        } else {
            // #. recover if the system is corrupted by checking system state.
            IRecoveryManager recoveryMgr = runtimeContext.getTransactionSubsystem().getRecoveryManager();
            systemState = recoveryMgr.getSystemState();

            if (LOGGER.isLoggable(Level.INFO)) {
                LOGGER.info("System is in a state: " + systemState);
            }

            if (replicationEnabled) {
                if (systemState == SystemState.NEW_UNIVERSE || systemState == SystemState.CORRUPTED) {
                    //try to perform remote recovery
                    IRemoteRecoveryManager remoteRecoveryMgr = runtimeContext.getRemoteRecoveryManager();
                    remoteRecoveryMgr.performRemoteRecovery();
                    performedRemoteRecovery = true;
                    systemState = SystemState.HEALTHY;
                }
            }

            if (systemState == SystemState.CORRUPTED) {
                recoveryMgr.startRecovery(true);
            }
        }

        if (replicationEnabled) {
            startReplicationService();
        }
    }

    private void startReplicationService() throws IOException {
        //open replication channel
        runtimeContext.getReplicationChannel().start();

        //check the state of remote replicas
        runtimeContext.getReplicationManager().initializeReplicasState();

        if (performedRemoteRecovery) {
            //notify remote replicas about the new IP Address if changed
            //Note: this is a hack since each node right now maintains its own copy of the cluster configuration.
            //Once the configuration is centralized on the CC, this step wont be needed.
            runtimeContext.getReplicationManager().broadcastNewIPAddress();
        }

        //start replication after the state of remote replicas has been initialized. 
        runtimeContext.getReplicationManager().startReplicationThreads();
    }

    @Override
    public void stop() throws Exception {
        if (!stopInitiated) {
            runtimeContext.setShuttingdown(true);
            stopInitiated = true;
            if (LOGGER.isLoggable(Level.INFO)) {
                LOGGER.info("Stopping Asterix node controller: " + nodeId);
            }

            if (isMetadataNode) {
                MetadataBootstrap.stopUniverse();
            }

            //clean any temporary files
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
        //send max resource id on this NC to the CC
        ((INCMessageBroker) ncApplicationContext.getMessageBroker()).reportMaxResourceId();

        AsterixMetadataProperties metadataProperties = ((IAsterixPropertiesProvider) runtimeContext)
                .getMetadataProperties();
        if (initialRun || systemState == SystemState.NEW_UNIVERSE) {
            if (LOGGER.isLoggable(Level.INFO)) {
                LOGGER.info("System state: " + SystemState.NEW_UNIVERSE);
                LOGGER.info("Node ID: " + nodeId);
                LOGGER.info("Stores: " + metadataProperties.getStores());
                LOGGER.info("Root Metadata Store: " + metadataProperties.getStores().get(nodeId)[0]);
            }

            PersistentLocalResourceRepository localResourceRepository = (PersistentLocalResourceRepository) runtimeContext
                    .getLocalResourceRepository();
            localResourceRepository.initializeNewUniverse(AsterixClusterProperties.INSTANCE.getStorageDirectoryName());
        }

        IAsterixStateProxy proxy = null;
        isMetadataNode = nodeId.equals(metadataProperties.getMetadataNodeName());
        if (isMetadataNode) {
            if (LOGGER.isLoggable(Level.INFO)) {
                LOGGER.info("Bootstrapping metadata");
            }
            MetadataNode.INSTANCE.initialize(runtimeContext);

            proxy = (IAsterixStateProxy) ncApplicationContext.getDistributedState();
            if (proxy == null) {
                throw new IllegalStateException("Metadata node cannot access distributed state");
            }

            // This is a special case, we just give the metadataNode directly.
            // This way we can delay the registration of the metadataNode until
            // it is completely initialized.
            MetadataManager.INSTANCE = new MetadataManager(proxy, MetadataNode.INSTANCE);
            MetadataBootstrap.startUniverse(((IAsterixPropertiesProvider) runtimeContext), ncApplicationContext,
                    systemState == SystemState.NEW_UNIVERSE);
            MetadataBootstrap.startDDLRecovery();

            if (LOGGER.isLoggable(Level.INFO)) {
                LOGGER.info("Metadata node bound");
            }
        }

        ExternalLibraryBootstrap.setUpExternaLibraries(isMetadataNode);
        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info("Starting lifecycle components");
        }

        Map<String, String> lifecycleMgmtConfiguration = new HashMap<String, String>();
        String dumpPathKey = LifeCycleComponentManager.Config.DUMP_PATH_KEY;
        String dumpPath = metadataProperties.getCoredumpPath(nodeId);
        lifecycleMgmtConfiguration.put(dumpPathKey, dumpPath);
        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info("Coredump directory for NC is: " + dumpPath);
        }
        ILifeCycleComponentManager lccm = ncApplicationContext.getLifeCycleComponentManager();
        lccm.configure(lifecycleMgmtConfiguration);
        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info("Configured:" + lccm);
        }
        ncApplicationContext.setStateDumpHandler(
                new AsterixStateDumpHandler(ncApplicationContext.getNodeId(), lccm.getDumpPath(), lccm));

        lccm.startAll();

        IRecoveryManager recoveryMgr = runtimeContext.getTransactionSubsystem().getRecoveryManager();
        recoveryMgr.checkpoint(true, RecoveryManager.NON_SHARP_CHECKPOINT_TARGET_LSN);

        if (isMetadataNode) {
            IMetadataNode stub = null;
            stub = (IMetadataNode) UnicastRemoteObject.exportObject(MetadataNode.INSTANCE, metadataRmiPort);
            proxy.setMetadataNode(stub);
        }

        //clean any temporary files
        performLocalCleanUp();
    }

    private void performLocalCleanUp() {
        //delete working area files from failed jobs
        runtimeContext.getIOManager().deleteWorkspaceFiles();

        //reclaim storage for temporary datasets.
        String storageDirName = AsterixClusterProperties.INSTANCE.getStorageDirectoryName();
        String[] ioDevices = ((PersistentLocalResourceRepository) runtimeContext.getLocalResourceRepository())
                .getStorageMountingPoints();
        for (String ioDevice : ioDevices) {
            String tempDatasetsDir = ioDevice + storageDirName + File.separator
                    + SplitsAndConstraintsUtil.TEMP_DATASETS_STORAGE_FOLDER;
            FileUtils.deleteQuietly(new File(tempDatasetsDir));
        }

        // TODO
        //reclaim storage for orphaned index artifacts in NCs.
        //Note: currently LSM indexes invalid components are deleted when an index is activated.
    }

    private void updateOnNodeJoin() {
        AsterixMetadataProperties metadataProperties = ((IAsterixPropertiesProvider) runtimeContext)
                .getMetadataProperties();
        if (!metadataProperties.getNodeNames().contains(nodeId)) {
            metadataProperties.getNodeNames().add(nodeId);
            Cluster cluster = AsterixClusterProperties.INSTANCE.getCluster();
            if (cluster == null) {
                throw new IllegalStateException("No cluster configuration found for this instance");
            }
            String asterixInstanceName = metadataProperties.getInstanceName();
            AsterixTransactionProperties txnProperties = ((IAsterixPropertiesProvider) runtimeContext)
                    .getTransactionProperties();
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
                    String storeDir = AsterixClusterProperties.INSTANCE.getStorageDirectoryName();
                    String nodeIoDevices = node.getIodevices() == null ? cluster.getIodevices() : node.getIodevices();
                    String[] ioDevicePaths = nodeIoDevices.trim().split(",");
                    for (int i = 0; i < ioDevicePaths.length; i++) {
                        //construct full store path
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
