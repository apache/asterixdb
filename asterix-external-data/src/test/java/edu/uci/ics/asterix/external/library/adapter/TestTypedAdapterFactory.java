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
package edu.uci.ics.asterix.external.library.adapter;

import java.util.Map;

import edu.uci.ics.asterix.common.exceptions.AsterixException;
import edu.uci.ics.asterix.metadata.feeds.IDatasourceAdapter;
import edu.uci.ics.asterix.metadata.feeds.ITypedAdapterFactory;
import edu.uci.ics.asterix.om.types.ARecordType;
import edu.uci.ics.asterix.om.types.BuiltinType;
import edu.uci.ics.asterix.om.types.IAType;
import edu.uci.ics.asterix.runtime.operators.file.AdmSchemafullRecordParserFactory;
import edu.uci.ics.hyracks.algebricks.common.constraints.AlgebricksCountPartitionConstraint;
import edu.uci.ics.hyracks.algebricks.common.constraints.AlgebricksPartitionConstraint;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.dataflow.std.file.ITupleParserFactory;

public class TestTypedAdapterFactory implements ITypedAdapterFactory {

    public static final String NAME = "test_typed_adapter";

    private static ARecordType adapterOutputType = initOutputType();

    public static final String KEY_NUM_OUTPUT_RECORDS = "num_output_records";

    private Map<String, String> configuration;

    @Override
    public SupportedOperation getSupportedOperations() {
        return SupportedOperation.READ;
    }

    private static ARecordType initOutputType() {
        String[] fieldNames = new String[] { "tweetid", "message-text" };
        IAType[] fieldTypes = new IAType[] { BuiltinType.AINT64, BuiltinType.ASTRING };
        ARecordType outputType = null;
        try {
            outputType = new ARecordType("TestTypedAdapterOutputType", fieldNames, fieldTypes, false);
        } catch (AsterixException exception) {
            throw new IllegalStateException("Unable to create output type for adapter " + NAME);
        }
        return outputType;
    }

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public AdapterType getAdapterType() {
        return AdapterType.TYPED;
    }

    @Override
    public AlgebricksPartitionConstraint getPartitionConstraint() throws Exception {
        return new AlgebricksCountPartitionConstraint(1);
    }

    @Override
    public IDatasourceAdapter createAdapter(IHyracksTaskContext ctx, int partition) throws Exception {
        ITupleParserFactory tupleParserFactory = new AdmSchemafullRecordParserFactory(adapterOutputType, false, -1, null);
        return new TestTypedAdapter(tupleParserFactory, adapterOutputType, ctx, configuration);
    }

    @Override
    public ARecordType getAdapterOutputType() {
        return adapterOutputType;
    }

    @Override
    public void configure(Map<String, String> configuration) throws Exception {
        this.configuration = configuration;
    }

}
