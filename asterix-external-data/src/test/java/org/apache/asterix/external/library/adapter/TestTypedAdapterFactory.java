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

import java.io.InputStream;
import java.util.Map;

import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.common.feeds.api.IDataSourceAdapter;
import org.apache.asterix.common.parse.ITupleForwarder;
import org.apache.asterix.external.api.IAdapterFactory;
import org.apache.asterix.external.parser.ADMDataParser;
import org.apache.asterix.external.util.DataflowUtils;
import org.apache.asterix.om.types.ARecordType;
import org.apache.hyracks.algebricks.common.constraints.AlgebricksCountPartitionConstraint;
import org.apache.hyracks.algebricks.common.constraints.AlgebricksPartitionConstraint;
import org.apache.hyracks.api.comm.IFrameWriter;
import org.apache.hyracks.api.context.IHyracksCommonContext;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import org.apache.hyracks.dataflow.std.file.ITupleParser;
import org.apache.hyracks.dataflow.std.file.ITupleParserFactory;

public class TestTypedAdapterFactory implements IAdapterFactory {

    private static final long serialVersionUID = 1L;

    private ARecordType outputType;

    public static final String KEY_NUM_OUTPUT_RECORDS = "num_output_records";

    private Map<String, String> configuration;

    @Override
    public String getAlias() {
        return "test_typed";
    }

    @Override
    public AlgebricksPartitionConstraint getPartitionConstraint() throws Exception {
        return new AlgebricksCountPartitionConstraint(1);
    }

    @Override
    public IDataSourceAdapter createAdapter(IHyracksTaskContext ctx, int partition) throws Exception {
        ITupleParserFactory tupleParserFactory = new ITupleParserFactory() {
            private static final long serialVersionUID = 1L;

            @Override
            public ITupleParser createTupleParser(final IHyracksCommonContext ctx) throws HyracksDataException {
                ADMDataParser parser;
                ITupleForwarder forwarder;
                ArrayTupleBuilder tb;
                try {
                    parser = new ADMDataParser();
                    forwarder = DataflowUtils.getTupleForwarder(configuration);
                    forwarder.configure(configuration);
                    tb = new ArrayTupleBuilder(1);
                } catch (AsterixException e) {
                    throw new HyracksDataException(e);
                }
                return new ITupleParser() {

                    @Override
                    public void parse(InputStream in, IFrameWriter writer) throws HyracksDataException {
                        try {
                            parser.configure(configuration, outputType);
                            parser.setInputStream(in);
                            forwarder.initialize(ctx, writer);
                            while (true) {
                                tb.reset();
                                if (!parser.parse(tb.getDataOutput())) {
                                    break;
                                }
                                tb.addFieldEndOffset();
                                forwarder.addTuple(tb);
                            }
                            forwarder.close();
                        } catch (Exception e) {
                            throw new HyracksDataException(e);
                        }
                    }
                };
            }
        };
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

}
