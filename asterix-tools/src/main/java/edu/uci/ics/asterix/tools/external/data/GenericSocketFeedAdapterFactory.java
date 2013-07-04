/*
x * Copyright 2009-2012 by The Regents of the University of California
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
package edu.uci.ics.asterix.tools.external.data;

import java.util.Map;

import edu.uci.ics.asterix.external.adapter.factory.StreamBasedAdapterFactory;
import edu.uci.ics.asterix.metadata.feeds.IDatasourceAdapter;
import edu.uci.ics.asterix.metadata.feeds.ITypedAdapterFactory;
import edu.uci.ics.asterix.om.types.ARecordType;
import edu.uci.ics.hyracks.algebricks.common.constraints.AlgebricksCountPartitionConstraint;
import edu.uci.ics.hyracks.algebricks.common.constraints.AlgebricksPartitionConstraint;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;

/**
 * Factory class for creating @see{GenericSocketFeedAdapter} The
 * adapter listens at a port for receiving data (from external world).
 * Data received is transformed into Asterix Data Format (ADM) and stored into
 * a dataset a configured in the Adapter configuration.
 */
public class GenericSocketFeedAdapterFactory extends StreamBasedAdapterFactory implements ITypedAdapterFactory {

    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    private ARecordType outputType;

    @Override
    public String getName() {
        return "generic_socket_feed";
    }

    @Override
    public AdapterType getAdapterType() {
        return AdapterType.GENERIC;
    }

    @Override
    public SupportedOperation getSupportedOperations() {
        return SupportedOperation.READ;
    }

    @Override
    public void configure(Map<String, Object> configuration) throws Exception {
        this.configuration = configuration;
        outputType = (ARecordType) configuration.get(KEY_SOURCE_DATATYPE);
        this.configureFormat(outputType);
    }

    @Override
    public AlgebricksPartitionConstraint getPartitionConstraint() throws Exception {
        return new AlgebricksCountPartitionConstraint(1);
    }

    @Override
    public IDatasourceAdapter createAdapter(IHyracksTaskContext ctx) throws Exception {
        return new GenericSocketFeedAdapter(configuration, parserFactory, outputType, ctx);
    }

    @Override
    public ARecordType getAdapterOutputType() {
        return outputType;
    }

}