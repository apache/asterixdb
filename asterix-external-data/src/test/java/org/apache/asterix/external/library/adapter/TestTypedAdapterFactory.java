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
package org.apache.asterix.external.library.adapter;

import java.util.Map;

import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.common.feeds.api.IDatasourceAdapter;
import org.apache.asterix.common.feeds.api.IIntakeProgressTracker;
import org.apache.asterix.metadata.entities.DatasourceAdapter.AdapterType;
import org.apache.asterix.metadata.external.IAdapterFactory.SupportedOperation;
import org.apache.asterix.metadata.feeds.IFeedAdapterFactory;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.runtime.operators.file.AsterixTupleParserFactory;
import org.apache.asterix.runtime.operators.file.AsterixTupleParserFactory.InputDataFormat;
import org.apache.hyracks.algebricks.common.constraints.AlgebricksCountPartitionConstraint;
import org.apache.hyracks.algebricks.common.constraints.AlgebricksPartitionConstraint;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.std.file.ITupleParserFactory;

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
