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
package edu.uci.ics.asterix.external.adapter.factory;

import java.io.File;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;

import edu.uci.ics.asterix.common.exceptions.AsterixException;
import edu.uci.ics.asterix.common.feeds.api.IDatasourceAdapter;
import edu.uci.ics.asterix.external.dataset.adapter.NCFileSystemAdapter;
import edu.uci.ics.asterix.external.util.DNSResolverFactory;
import edu.uci.ics.asterix.external.util.INodeResolver;
import edu.uci.ics.asterix.external.util.INodeResolverFactory;
import edu.uci.ics.asterix.metadata.entities.ExternalFile;
import edu.uci.ics.asterix.metadata.external.IAdapterFactory;
import edu.uci.ics.asterix.om.types.ARecordType;
import edu.uci.ics.asterix.om.types.IAType;
import edu.uci.ics.asterix.runtime.operators.file.AsterixTupleParserFactory;
import edu.uci.ics.asterix.runtime.operators.file.AsterixTupleParserFactory.InputDataFormat;
import edu.uci.ics.hyracks.algebricks.common.constraints.AlgebricksAbsolutePartitionConstraint;
import edu.uci.ics.hyracks.algebricks.common.constraints.AlgebricksPartitionConstraint;
import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.io.FileReference;
import edu.uci.ics.hyracks.dataflow.std.file.FileSplit;

/**
 * Factory class for creating an instance of NCFileSystemAdapter. An
 * NCFileSystemAdapter reads external data residing on the local file system of
 * an NC.
 */
public class NCFileSystemAdapterFactory extends StreamBasedAdapterFactory implements IAdapterFactory {
    private static final long serialVersionUID = 1L;

    public static final String NC_FILE_SYSTEM_ADAPTER_NAME = "localfs";

    private static final INodeResolver DEFAULT_NODE_RESOLVER = new DNSResolverFactory().createNodeResolver();

    private IAType sourceDatatype;
    private FileSplit[] fileSplits;
    private ARecordType outputType;


    @Override
    public IDatasourceAdapter createAdapter(IHyracksTaskContext ctx, int partition) throws Exception {
        NCFileSystemAdapter fsAdapter = new NCFileSystemAdapter(fileSplits, parserFactory, sourceDatatype, ctx);
        return fsAdapter;
    }

    @Override
    public String getName() {
        return NC_FILE_SYSTEM_ADAPTER_NAME;
    }


    @Override
    public SupportedOperation getSupportedOperations() {
        return SupportedOperation.READ;
    }

    @Override
    public void configure(Map<String, String> configuration, ARecordType outputType) throws Exception {
        this.configuration = configuration;
        this.outputType = outputType;
        String[] splits = ((String) configuration.get(AsterixTupleParserFactory.KEY_PATH)).split(",");
        IAType sourceDatatype = (IAType) outputType;
        configureFileSplits(splits);
        configureFormat(sourceDatatype);

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
                    throw new AsterixException("Invalid path: " + splitPath
                            + "\nUsage- path=\"Host://Absolute File Path\"");
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
            nodeResolver = initializeNodeResolver();
        }
        return nodeResolver;
    }

    private static INodeResolver initializeNodeResolver() {
        INodeResolver nodeResolver = null;
        String configuredNodeResolverFactory = System.getProperty(AsterixTupleParserFactory.NODE_RESOLVER_FACTORY_PROPERTY);
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
    
    @Override
    public ARecordType getAdapterOutputType() {
        return outputType;
    }
    
    @Override
    public InputDataFormat getInputDataFormat() {
        return InputDataFormat.UNKNOWN;
    }

    public void setFiles(List<ExternalFile> files) throws AlgebricksException {
        throw new AlgebricksException("can't set files for this Adapter");
    }

}
