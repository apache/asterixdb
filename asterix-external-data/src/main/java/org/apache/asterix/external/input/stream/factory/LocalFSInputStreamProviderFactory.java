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

import java.io.File;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.external.api.IInputStreamProvider;
import org.apache.asterix.external.api.IInputStreamProviderFactory;
import org.apache.asterix.external.api.INodeResolver;
import org.apache.asterix.external.api.INodeResolverFactory;
import org.apache.asterix.external.input.stream.LocalFSInputStreamProvider;
import org.apache.asterix.external.util.DNSResolverFactory;
import org.apache.asterix.external.util.ExternalDataConstants;
import org.apache.hyracks.algebricks.common.constraints.AlgebricksAbsolutePartitionConstraint;
import org.apache.hyracks.algebricks.common.constraints.AlgebricksPartitionConstraint;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.dataflow.std.file.FileSplit;

public class LocalFSInputStreamProviderFactory implements IInputStreamProviderFactory {

    private static final long serialVersionUID = 1L;

    protected static final INodeResolver DEFAULT_NODE_RESOLVER = new DNSResolverFactory().createNodeResolver();
    protected static final Logger LOGGER = Logger.getLogger(LocalFSInputStreamProviderFactory.class.getName());
    protected static INodeResolver nodeResolver;
    protected Map<String, String> configuration;
    protected FileSplit[] fileSplits;

    @Override
    public IInputStreamProvider createInputStreamProvider(IHyracksTaskContext ctx, int partition) throws Exception {
        return new LocalFSInputStreamProvider(fileSplits, ctx, configuration, partition);
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
    public void configure(Map<String, String> configuration) throws Exception {
        this.configuration = configuration;
        String[] splits = configuration.get(ExternalDataConstants.KEY_PATH).split(",");
        configureFileSplits(splits);
    }

    @Override
    public AlgebricksPartitionConstraint getPartitionConstraint() throws Exception {
        return configurePartitionConstraint();
    }

    private void configureFileSplits(String[] splits) throws AsterixException {
        if (fileSplits == null) {
            fileSplits = new FileSplit[splits.length];
            String nodeName;
            String nodeLocalPath;
            int count = 0;
            String trimmedValue;
            for (String splitPath : splits) {
                trimmedValue = splitPath.trim();
                if (!trimmedValue.contains("://")) {
                    throw new AsterixException(
                            "Invalid path: " + splitPath + "\nUsage- path=\"Host://Absolute File Path\"");
                }
                nodeName = trimmedValue.split(":")[0];
                nodeLocalPath = trimmedValue.split("://")[1];
                FileSplit fileSplit = new FileSplit(nodeName, new FileReference(new File(nodeLocalPath)));
                fileSplits[count++] = fileSplit;
            }
        }
    }

    private AlgebricksPartitionConstraint configurePartitionConstraint() throws AsterixException {
        String[] locs = new String[fileSplits.length];
        String location;
        for (int i = 0; i < fileSplits.length; i++) {
            location = getNodeResolver().resolveNode(fileSplits[i].getNodeName());
            locs[i] = location;
        }
        return new AlgebricksAbsolutePartitionConstraint(locs);
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
                if (LOGGER.isLoggable(Level.WARNING)) {
                    LOGGER.log(Level.WARNING, "Unable to create node resolver from the configured classname "
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
