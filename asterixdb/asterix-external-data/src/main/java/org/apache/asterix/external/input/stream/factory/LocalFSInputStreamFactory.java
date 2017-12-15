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
package org.apache.asterix.external.input.stream.factory;

import java.net.InetAddress;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

import org.apache.asterix.common.dataflow.ICcApplicationContext;
import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.external.api.AsterixInputStream;
import org.apache.asterix.external.api.IInputStreamFactory;
import org.apache.asterix.external.api.INodeResolver;
import org.apache.asterix.external.api.INodeResolverFactory;
import org.apache.asterix.external.input.stream.LocalFSInputStream;
import org.apache.asterix.external.util.ExternalDataConstants;
import org.apache.asterix.external.util.ExternalDataUtils;
import org.apache.asterix.external.util.FileSystemWatcher;
import org.apache.asterix.external.util.NodeResolverFactory;
import org.apache.asterix.runtime.utils.RuntimeUtils;
import org.apache.hyracks.algebricks.common.constraints.AlgebricksAbsolutePartitionConstraint;
import org.apache.hyracks.api.application.IServiceContext;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.UnmanagedFileSplit;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class LocalFSInputStreamFactory implements IInputStreamFactory {

    private static final long serialVersionUID = 1L;

    protected static final INodeResolver DEFAULT_NODE_RESOLVER = new NodeResolverFactory().createNodeResolver();
    protected static final Logger LOGGER = LogManager.getLogger();
    protected static INodeResolver nodeResolver;
    protected Map<String, String> configuration;
    protected UnmanagedFileSplit[] inputFileSplits;
    protected boolean isFeed;
    protected String expression;
    // transient fields (They don't need to be serialized and transferred)
    private transient AlgebricksAbsolutePartitionConstraint constraints;
    private transient FileSystemWatcher watcher;

    @Override
    public synchronized AsterixInputStream createInputStream(IHyracksTaskContext ctx, int partition)
            throws HyracksDataException {
        if (watcher == null) {
            String nodeName = ctx.getJobletContext().getServiceContext().getNodeId();
            ArrayList<Path> inputResources = new ArrayList<>();
            for (int i = 0; i < inputFileSplits.length; i++) {
                if (inputFileSplits[i].getNodeName().equals(nodeName)) {
                    inputResources.add(inputFileSplits[i].getFile().toPath());
                }
            }
            watcher = new FileSystemWatcher(inputResources, expression, isFeed);
        }
        return new LocalFSInputStream(watcher);
    }

    @Override
    public DataSourceType getDataSourceType() {
        return DataSourceType.STREAM;
    }

    @Override
    public boolean isIndexible() {
        return false;
    }

    @Override
    public void configure(IServiceContext serviceCtx, Map<String, String> configuration) throws AsterixException {
        this.configuration = configuration;
        String[] splits = configuration.get(ExternalDataConstants.KEY_PATH).split(",");
        if (inputFileSplits == null) {
            configureFileSplits((ICcApplicationContext) serviceCtx.getApplicationContext(), splits);
        }
        configurePartitionConstraint();
        this.isFeed = ExternalDataUtils.isFeed(configuration) && ExternalDataUtils.keepDataSourceOpen(configuration);
        this.expression = configuration.get(ExternalDataConstants.KEY_EXPRESSION);
    }

    @Override
    public AlgebricksAbsolutePartitionConstraint getPartitionConstraint() {
        return constraints;
    }

    private void configureFileSplits(ICcApplicationContext appCtx, String[] splits) throws AsterixException {
        INodeResolver resolver = getNodeResolver();
        Map<InetAddress, Set<String>> ncMap = RuntimeUtils.getForcedNodeControllerMap(appCtx);
        Set<String> ncs = ncMap.values().stream().flatMap(Collection::stream).collect(Collectors.toSet());
        inputFileSplits = new UnmanagedFileSplit[splits.length];
        String node;
        String path;
        int count = 0;
        String trimmedValue;
        for (String splitPath : splits) {
            trimmedValue = splitPath.trim();
            if (!trimmedValue.contains("://")) {
                throw new AsterixException(
                        "Invalid path: " + splitPath + "\nUsage- path=\"Host://Absolute File Path\"");
            }
            node = resolver.resolveNode(appCtx, trimmedValue.split(":")[0], ncMap, ncs);
            path = trimmedValue.split("://")[1];
            inputFileSplits[count++] = new UnmanagedFileSplit(node, path);
        }

    }

    private void configurePartitionConstraint() throws AsterixException {
        Set<String> locs = new TreeSet<>();
        for (int i = 0; i < inputFileSplits.length; i++) {
            locs.add(inputFileSplits[i].getNodeName());
        }
        constraints = new AlgebricksAbsolutePartitionConstraint(locs.toArray(new String[locs.size()]));
    }

    protected INodeResolver getNodeResolver() {
        if (nodeResolver == null) {
            synchronized (DEFAULT_NODE_RESOLVER) {
                if (nodeResolver == null) {
                    nodeResolver = initializeNodeResolver();
                }
            }
        }
        return nodeResolver;
    }

    private static INodeResolver initializeNodeResolver() {
        INodeResolver nodeResolver = null;
        String configuredNodeResolverFactory = System.getProperty(ExternalDataConstants.NODE_RESOLVER_FACTORY_PROPERTY);
        if (configuredNodeResolverFactory != null) {
            try {
                nodeResolver = ((INodeResolverFactory) (Class.forName(configuredNodeResolverFactory).newInstance()))
                        .createNodeResolver();

            } catch (Exception e) {
                if (LOGGER.isWarnEnabled()) {
                    LOGGER.log(Level.WARN, "Unable to create node resolver from the configured classname "
                            + configuredNodeResolverFactory + "\n" + e.getMessage());
                }
                nodeResolver = DEFAULT_NODE_RESOLVER;
            }
        } else {
            nodeResolver = DEFAULT_NODE_RESOLVER;
        }
        return nodeResolver;
    }
}
