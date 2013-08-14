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
package edu.uci.ics.asterix.external.dataset.adapter;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.uci.ics.asterix.common.exceptions.AsterixException;
import edu.uci.ics.asterix.external.util.DNSResolverFactory;
import edu.uci.ics.asterix.external.util.INodeResolver;
import edu.uci.ics.asterix.external.util.INodeResolverFactory;
import edu.uci.ics.asterix.om.types.ARecordType;
import edu.uci.ics.asterix.om.types.ATypeTag;
import edu.uci.ics.asterix.om.types.AUnionType;
import edu.uci.ics.asterix.om.types.IAType;
import edu.uci.ics.asterix.runtime.operators.file.AdmSchemafullRecordParserFactory;
import edu.uci.ics.asterix.runtime.operators.file.NtDelimitedDataTupleParserFactory;
import edu.uci.ics.hyracks.algebricks.common.constraints.AlgebricksPartitionConstraint;
import edu.uci.ics.hyracks.algebricks.common.exceptions.NotImplementedException;
import edu.uci.ics.hyracks.api.comm.IFrameWriter;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.dataflow.common.data.parsers.IValueParserFactory;
import edu.uci.ics.hyracks.dataflow.std.file.ITupleParser;
import edu.uci.ics.hyracks.dataflow.std.file.ITupleParserFactory;

public abstract class FileSystemBasedAdapter extends AbstractDatasourceAdapter {

    private static final long serialVersionUID = 1L;

    public static final String NODE_RESOLVER_FACTORY_PROPERTY = "node.Resolver";
    public static final String KEY_DELIMITER = "delimiter";
    public static final String KEY_PATH = "path";

    protected ITupleParserFactory parserFactory;
    protected ITupleParser parser;
    protected static INodeResolver nodeResolver;

    private static final INodeResolver DEFAULT_NODE_RESOLVER = new DNSResolverFactory().createNodeResolver();
    private static final Logger LOGGER = Logger.getLogger(FileSystemBasedAdapter.class.getName());

    public abstract InputStream getInputStream(int partition) throws IOException;

    public FileSystemBasedAdapter(IAType atype) {
        this.atype = atype;
    }

    @Override
    public void start(int partition, IFrameWriter writer) throws Exception {
        InputStream in = getInputStream(partition);
        parser = getTupleParser();
        parser.parse(in, writer);
    }

    @Override
    public abstract void initialize(IHyracksTaskContext ctx) throws Exception;

    @Override
    public abstract void configure(Map<String, Object> arguments) throws Exception;

    @Override
    public abstract AdapterType getAdapterType();

    @Override
    public abstract AlgebricksPartitionConstraint getPartitionConstraint() throws Exception;

    protected ITupleParser getTupleParser() throws Exception {
        return parserFactory.createTupleParser(ctx);
    }

    protected void configureFormat() throws Exception {
        String parserFactoryClassname = (String) configuration.get(KEY_PARSER_FACTORY);
        if (parserFactoryClassname == null) {
            String specifiedFormat = (String) configuration.get(KEY_FORMAT);
            if (specifiedFormat == null) {
                throw new IllegalArgumentException(" Unspecified data format");
            } else if (FORMAT_DELIMITED_TEXT.equalsIgnoreCase(specifiedFormat)) {
                parserFactory = getDelimitedDataTupleParserFactory((ARecordType) atype);
            } else if (FORMAT_ADM.equalsIgnoreCase((String) configuration.get(KEY_FORMAT))) {
                parserFactory = getADMDataTupleParserFactory((ARecordType) atype);
            } else {
                throw new IllegalArgumentException(" format " + configuration.get(KEY_FORMAT) + " not supported");
            }
        } else {
            parserFactory = (ITupleParserFactory) Class.forName(parserFactoryClassname).newInstance();
        }

    }

    protected ITupleParserFactory getDelimitedDataTupleParserFactory(ARecordType recordType) throws AsterixException {
        int n = recordType.getFieldTypes().length;
        IValueParserFactory[] fieldParserFactories = new IValueParserFactory[n];
        for (int i = 0; i < n; i++) {
            ATypeTag tag = null;
            if (recordType.getFieldTypes()[i].getTypeTag() == ATypeTag.UNION) {
                List<IAType> unionTypes = ((AUnionType) recordType.getFieldTypes()[i]).getUnionList();
                if (unionTypes.size() != 2 && unionTypes.get(0).getTypeTag() != ATypeTag.NULL) {
                    throw new NotImplementedException("Non-optional UNION type is not supported.");
                }
                tag = unionTypes.get(1).getTypeTag();
            } else {
                tag = recordType.getFieldTypes()[i].getTypeTag();
            }
            if (tag == null) {
                throw new NotImplementedException("Failed to get the type information for field " + i + ".");
            }
            IValueParserFactory vpf = typeToValueParserFactMap.get(tag);
            if (vpf == null) {
                throw new NotImplementedException("No value parser factory for delimited fields of type " + tag);
            }
            fieldParserFactories[i] = vpf;
        }
        String delimiterValue = (String) configuration.get(KEY_DELIMITER);
        if (delimiterValue != null && delimiterValue.length() > 1) {
            throw new AsterixException("improper delimiter");
        }

        Character delimiter = delimiterValue.charAt(0);
        return new NtDelimitedDataTupleParserFactory(recordType, fieldParserFactories, delimiter);
    }

    protected ITupleParserFactory getADMDataTupleParserFactory(ARecordType recordType) throws AsterixException {
        try {
            return new AdmSchemafullRecordParserFactory(recordType);
        } catch (Exception e) {
            throw new AsterixException(e);
        }

    }

    protected INodeResolver getNodeResolver() {
        if (nodeResolver == null) {
            nodeResolver = initNodeResolver();
        }
        return nodeResolver;
    }

    private static INodeResolver initNodeResolver() {
        INodeResolver nodeResolver = null;
        String configuredNodeResolverFactory = System.getProperty(NODE_RESOLVER_FACTORY_PROPERTY);
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
