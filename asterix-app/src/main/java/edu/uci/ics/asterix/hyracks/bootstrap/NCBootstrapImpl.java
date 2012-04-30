/*
 * Copyright 2009-2011 by The Regents of the University of California
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

import java.rmi.server.UnicastRemoteObject;
import java.util.logging.Logger;

import edu.uci.ics.asterix.api.aqlj.server.NodeDataClientThreadFactory;
import edu.uci.ics.asterix.api.aqlj.server.ThreadedServer;
import edu.uci.ics.asterix.common.api.AsterixAppContextInfoImpl;
import edu.uci.ics.asterix.common.exceptions.AsterixException;
import edu.uci.ics.asterix.common.context.AsterixAppRuntimeContext;
import edu.uci.ics.asterix.metadata.MetadataManager;
import edu.uci.ics.asterix.metadata.MetadataNode;
import edu.uci.ics.asterix.metadata.api.IAsterixStateProxy;
import edu.uci.ics.asterix.metadata.api.IMetadataNode;
import edu.uci.ics.asterix.metadata.bootstrap.AsterixProperties;
import edu.uci.ics.asterix.metadata.bootstrap.MetadataBootstrap;
import edu.uci.ics.asterix.transaction.management.exception.ACIDException;
import edu.uci.ics.asterix.transaction.management.service.transaction.TransactionProvider;
import edu.uci.ics.hyracks.api.application.INCApplicationContext;
import edu.uci.ics.hyracks.api.application.INCBootstrap;

public class NCBootstrapImpl implements INCBootstrap {
    private static final Logger LOGGER = Logger.getLogger(NCBootstrapImpl.class.getName());

    public static final int DEFAULT_AQLJ_NODE_DATA_SERVER_PORT = 6061;

    private INCApplicationContext ncAppContext = null;

    private static IMetadataNode metadataNode;
    private String nodeId;

    private ThreadedServer apiNodeDataServer;

    @Override
    public void start() throws Exception {

        LOGGER.info("Starting Asterix NC " + nodeId + " Bootstrap");
        IAsterixStateProxy p = (IAsterixStateProxy) ncAppContext.getDistributedState();
        LOGGER.info("\nMetadata node " + p.getAsterixProperties().getMetadataNodeName());
        initializeTransactionSupport(ncAppContext, nodeId);
        if (nodeId.equals(p.getAsterixProperties().getMetadataNodeName())) {
            AsterixAppRuntimeContext.initialize(ncAppContext);
            LOGGER.info("Initialized AsterixRuntimeContext: " + AsterixAppRuntimeContext.getInstance());
            metadataNode = registerRemoteObject(ncAppContext, p.getAsterixProperties());
            p.setMetadataNode(metadataNode);
            MetadataManager.INSTANCE = new MetadataManager(p);
            LOGGER.info("Bootstrapping Metadata");
            MetadataManager.INSTANCE.init();
            MetadataBootstrap.startUniverse(p.getAsterixProperties(), AsterixAppContextInfoImpl.INSTANCE);
        } else {
            Thread.sleep(5000);
            AsterixAppRuntimeContext.initialize(ncAppContext);
            LOGGER.info("Initialized AsterixRuntimeContext: " + AsterixAppRuntimeContext.getInstance());
        }

        IAsterixStateProxy proxy = (IAsterixStateProxy) ncAppContext.getDistributedState();
        AsterixNodeState ns = (AsterixNodeState) proxy.getAsterixNodeState(ncAppContext.getNodeId());
        apiNodeDataServer = new ThreadedServer(ns.getAPINodeDataServerPort(), new NodeDataClientThreadFactory());
        apiNodeDataServer.start();
    }

    public static IMetadataNode registerRemoteObject(INCApplicationContext ncAppContext,
            AsterixProperties asterixProperties) throws AsterixException {
        try {
            TransactionProvider factory = (TransactionProvider) ncAppContext.getApplicationObject();
            MetadataNode.INSTANCE = new MetadataNode(asterixProperties, AsterixAppContextInfoImpl.INSTANCE, factory);
            IMetadataNode stub = (IMetadataNode) UnicastRemoteObject.exportObject(MetadataNode.INSTANCE, 0);
            LOGGER.info("MetadataNode bound.");
            return stub;
        } catch (Exception e) {
            LOGGER.info("MetadataNode exception.");
            throw new AsterixException(e);
        }
    }

    @Override
    public void stop() throws Exception {
        LOGGER.info("Stopping Asterix NC Bootstrap");
        IAsterixStateProxy p = (IAsterixStateProxy) ncAppContext.getDistributedState();
        if (nodeId.equals(p.getAsterixProperties().getMetadataNodeName())) {
            MetadataBootstrap.stopUniverse();
        }
        AsterixAppRuntimeContext.deinitialize();
        apiNodeDataServer.shutdown();
    }

    @Override
    public void setApplicationContext(INCApplicationContext appCtx) {
        this.ncAppContext = appCtx;
        this.nodeId = ncAppContext.getNodeId();
    }

    private void initializeTransactionSupport(INCApplicationContext ncAppContext, String nodeId) {
        try {
            TransactionProvider factory = new TransactionProvider(nodeId);
            ncAppContext.setApplicationObject(factory);
        } catch (ACIDException e) {
            e.printStackTrace();
            LOGGER.severe(" Could not initialize transaction support ");
        }
    }
}