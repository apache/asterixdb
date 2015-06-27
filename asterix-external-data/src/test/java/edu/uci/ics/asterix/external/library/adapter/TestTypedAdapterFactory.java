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
import edu.uci.ics.asterix.common.feeds.api.IDatasourceAdapter;
import edu.uci.ics.asterix.common.feeds.api.IIntakeProgressTracker;
import edu.uci.ics.asterix.metadata.entities.DatasourceAdapter.AdapterType;
import edu.uci.ics.asterix.metadata.external.IAdapterFactory.SupportedOperation;
import edu.uci.ics.asterix.metadata.feeds.IFeedAdapterFactory;
import edu.uci.ics.asterix.om.types.ARecordType;
import edu.uci.ics.asterix.om.types.BuiltinType;
import edu.uci.ics.asterix.om.types.IAType;
import edu.uci.ics.asterix.runtime.operators.file.AsterixTupleParserFactory;
import edu.uci.ics.asterix.runtime.operators.file.AsterixTupleParserFactory.InputDataFormat;
import edu.uci.ics.hyracks.algebricks.common.constraints.AlgebricksCountPartitionConstraint;
import edu.uci.ics.hyracks.algebricks.common.constraints.AlgebricksPartitionConstraint;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.std.file.ITupleParserFactory;

public class TestTypedAdapterFactory implements IFeedAdapterFactory {

    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    public static final String NAME = "test_typed_adapter";

    private ARecordType outputType;

    public static final String KEY_NUM_OUTPUT_RECORDS = "num_output_records";

    private Map<String, String> configuration;

    @Override
    public SupportedOperation getSupportedOperations() {
        return SupportedOperation.READ;
    }

    private static ARecordType initOutputType() {
        String[] fieldNames = new String[] { "id", "message-text" };
        IAType[] fieldTypes = new IAType[] { BuiltinType.AINT64, BuiltinType.ASTRING };
        ARecordType outputType = null;
        try {
            outputType = new ARecordType("TestTypedAdapterOutputType", fieldNames, fieldTypes, false);
        } catch (AsterixException | HyracksDataException exception) {
            throw new IllegalStateException("Unable to create output type for adapter " + NAME);
        }
        return outputType;
    }

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public AlgebricksPartitionConstraint getPartitionConstraint() throws Exception {
        return new AlgebricksCountPartitionConstraint(1);
    }

    @Override
    public IDatasourceAdapter createAdapter(IHyracksTaskContext ctx, int partition) throws Exception {
        ITupleParserFactory tupleParserFactory = new AsterixTupleParserFactory(configuration, outputType,
                InputDataFormat.ADM);
        return new TestTypedAdapter(tupleParserFactory, outputType, ctx, configuration, partition);
    }

    @Override
    public ARecordType getAdapterOutputType() {
        return outputType;
    }

    @Override
    public void configure(Map<String, String> configuration, ARecordType outputType) throws Exception {
        this.configuration = configuration;
        this.outputType = outputType;
    }

    @Override
    public boolean isRecordTrackingEnabled() {
        return false;
    }

    @Override
    public IIntakeProgressTracker createIntakeProgressTracker() {
        return null;
    }

}
