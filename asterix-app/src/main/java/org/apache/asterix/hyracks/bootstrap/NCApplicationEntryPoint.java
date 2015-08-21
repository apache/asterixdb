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
package edu.uci.ics.asterix.hyracks.bootstrap;

import java.io.File;
import java.rmi.server.UnicastRemoteObject;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

import edu.uci.ics.asterix.api.common.AsterixAppRuntimeContext;
import edu.uci.ics.asterix.common.api.AsterixThreadFactory;
import edu.uci.ics.asterix.common.api.IAsterixAppRuntimeContext;
import edu.uci.ics.asterix.common.config.AsterixMetadataProperties;
import edu.uci.ics.asterix.common.config.AsterixTransactionProperties;
import edu.uci.ics.asterix.common.config.IAsterixPropertiesProvider;
import edu.uci.ics.asterix.common.transactions.IRecoveryManager;
import edu.uci.ics.asterix.common.transactions.IRecoveryManager.SystemState;
import edu.uci.ics.asterix.event.schema.cluster.Cluster;
import edu.uci.ics.asterix.event.schema.cluster.Node;
import edu.uci.ics.asterix.metadata.MetadataManager;
import edu.uci.ics.asterix.metadata.MetadataNode;
import edu.uci.ics.asterix.metadata.api.IAsterixStateProxy;
import edu.uci.ics.asterix.metadata.api.IMetadataNode;
import edu.uci.ics.asterix.metadata.bootstrap.MetadataBootstrap;
import edu.uci.ics.asterix.om.util.AsterixClusterProperties;
import edu.uci.ics.asterix.transaction.management.resource.PersistentLocalResourceRepository;
import edu.uci.ics.hyracks.api.application.INCApplicationContext;
import edu.uci.ics.hyracks.api.application.INCApplicationEntryPoint;
import edu.uci.ics.hyracks.api.lifecycle.ILifeCycleComponentManager;
import edu.uci.ics.hyracks.api.lifecycle.LifeCycleComponentManager;

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
    private final long NON_SHARP_CHECKPOINT_TARGET_LSN = -1;

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
        runtimeContext.initialize();
        ncApplicationContext.setApplicationObject(runtimeContext);

        if (initialRun) {
            LOGGER.info("System is being initialized. (first run)");
            systemState = SystemState.NEW_UNIVERSE;
        } else {
            // #. recover if the system is corrupted by checking system state.
            IRecoveryManager recoveryMgr = runtimeContext.getTransactionSubsystem().getRecoveryManager();
            systemState = recoveryMgr.getSystemState();

            if (LOGGER.isLoggable(Level.INFO)) {
                LOGGER.info("System is in a state: " + systemState);
            }

            if (systemState != SystemState.NEW_UNIVERSE) {
                PersistentLocalResourceRepository localResourceRepository = (PersistentLocalResourceRepository) runtimeContext
                        .getLocalResourceRepository();
                localResourceRepository.initialize(nodeId, null, false, runtimeContext.getResourceIdFactory());
            }
            
            if (systemState == SystemState.CORRUPTED) {
                recoveryMgr.startRecovery(true);
            }
        }

    }

    @Override
    public void stop() throws Exception {
        if (!stopInitiated) {
            runtimeContext.setShuttingdown(true);
            stopInitiated = true;
            if (LOGGER.isLoggable(Level.INFO)) {
                LOGGER.info("Stopping Asterix node controller: " + nodeId);
            }

            IRecoveryManager recoveryMgr = runtimeContext.getTransactionSubsystem().getRecoveryManager();
            recoveryMgr.checkpoint(true, NON_SHARP_CHECKPOINT_TARGET_LSN);

            if (isMetadataNode) {
                MetadataBootstrap.stopUniverse();
            }

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
        AsterixMetadataProperties metadataProperties = ((IAsterixPropertiesProvider) runtimeContext)
                .getMetadataProperties();

        if (systemState == SystemState.NEW_UNIVERSE) {
            if (LOGGER.isLoggable(Level.INFO)) {
                LOGGER.info("System state: " + SystemState.NEW_UNIVERSE);
                LOGGER.info("Node ID: " + nodeId);
                LOGGER.info("Stores: " + metadataProperties.getStores());
                LOGGER.info("Root Metadata Store: " + metadataProperties.getStores().get(nodeId)[0]);
            }

            PersistentLocalResourceRepository localResourceRepository = (PersistentLocalResourceRepository) runtimeContext
                    .getLocalResourceRepository();
            localResourceRepository.initialize(nodeId, metadataProperties.getStores().get(nodeId)[0], true, null);
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
        ncApplicationContext.setStateDumpHandler(new AsterixStateDumpHandler(ncApplicationContext.getNodeId(), lccm
                .getDumpPath(), lccm));

        lccm.startAll();

        IRecoveryManager recoveryMgr = runtimeContext.getTransactionSubsystem().getRecoveryManager();
        recoveryMgr.checkpoint(true, NON_SHARP_CHECKPOINT_TARGET_LSN);

        if (isMetadataNode) {
            IMetadataNode stub = null;
            stub = (IMetadataNode) UnicastRemoteObject.exportObject(MetadataNode.INSTANCE, metadataRmiPort);
            proxy.setMetadataNode(stub);
        }

        // Reclaim storage for temporary datasets.
        String[] ioDevices = AsterixClusterProperties.INSTANCE.getIODevices(nodeId);
        String[] nodeStores = metadataProperties.getStores().get(nodeId);
        int numIoDevices = AsterixClusterProperties.INSTANCE.getNumberOfIODevices(nodeId);
        for (int j = 0; j < nodeStores.length; j++) {
            for (int k = 0; k < numIoDevices; k++) {
                File f = new File(ioDevices[k] + File.separator + nodeStores[j] + File.separator + "temp");
                f.delete();
            }
        }

        // TODO
        // reclaim storage for orphaned index artifacts in NCs.

    }

    private void updateOnNodeJoin() {
        AsterixMetadataProperties metadataProperties = ((IAsterixPropertiesProvider) runtimeContext)
                .getMetadataProperties();
        if (!metadataProperties.getNodeNames().contains(nodeId)) {
            metadataProperties.getNodeNames().add(nodeId);
            Cluster cluster = AsterixClusterProperties.INSTANCE.getCluster();
            String asterixInstanceName = cluster.getInstanceName();
            AsterixTransactionProperties txnProperties = ((IAsterixPropertiesProvider) runtimeContext)
                    .getTransactionProperties();
            Node self = null;
            for (Node node : cluster.getSubstituteNodes().getNode()) {
                String ncId = asterixInstanceName + "_" + node.getId();
                if (ncId.equalsIgnoreCase(nodeId)) {
                    String storeDir = node.getStore() == null ? cluster.getStore() : node.getStore();
                    metadataProperties.getStores().put(nodeId, storeDir.split(","));

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
