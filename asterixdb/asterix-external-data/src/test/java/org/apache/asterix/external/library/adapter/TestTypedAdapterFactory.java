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

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;

import org.apache.asterix.common.api.IApplicationContext;
import org.apache.asterix.common.cluster.ClusterPartition;
import org.apache.asterix.common.dataflow.ICcApplicationContext;
import org.apache.asterix.external.api.IAdapterFactory;
import org.apache.asterix.external.api.IDataSourceAdapter;
import org.apache.asterix.external.api.IExternalDataSourceFactory;
import org.apache.asterix.external.dataflow.TupleForwarder;
import org.apache.asterix.external.parser.ADMDataParser;
import org.apache.asterix.om.types.ARecordType;
import org.apache.hyracks.algebricks.common.constraints.AlgebricksAbsolutePartitionConstraint;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.api.application.IServiceContext;
import org.apache.hyracks.api.comm.IFrameWriter;
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

    private transient AlgebricksAbsolutePartitionConstraint clusterLocations;

    private transient IServiceContext serviceContext;

    @Override
    public String getAlias() {
        return "test_typed";
    }

    @Override
    public AlgebricksAbsolutePartitionConstraint getPartitionConstraint() throws AlgebricksException {
        clusterLocations = IExternalDataSourceFactory.getPartitionConstraints(
                (ICcApplicationContext) serviceContext.getApplicationContext(), clusterLocations, 1);
        return clusterLocations;
    }

    @Override
    public IDataSourceAdapter createAdapter(IHyracksTaskContext ctx, int partition) throws HyracksDataException {
        final String nodeId = ctx.getJobletContext().getServiceContext().getNodeId();
        final ITupleParserFactory tupleParserFactory = new ITupleParserFactory() {
            private static final long serialVersionUID = 1L;

            @Override
            public ITupleParser createTupleParser(IHyracksTaskContext ctx) throws HyracksDataException {
                ADMDataParser parser;

                ArrayTupleBuilder tb;
                IApplicationContext appCtx =
                        (IApplicationContext) ctx.getJobletContext().getServiceContext().getApplicationContext();
                ClusterPartition nodePartition = appCtx.getMetadataProperties().getNodePartitions().get(nodeId)[0];
                parser = new ADMDataParser(outputType, true);
                tb = new ArrayTupleBuilder(1);
                return new ITupleParser() {
                    @Override
                    public void parse(InputStream in, IFrameWriter writer) throws HyracksDataException {
                        try {
                            parser.setInputStream(in);
                            TupleForwarder forwarder = new TupleForwarder(ctx, writer);
                            while (true) {
                                tb.reset();
                                if (!parser.parse(tb.getDataOutput())) {
                                    break;
                                }
                                tb.addFieldEndOffset();
                                forwarder.addTuple(tb);
                            }
                            forwarder.complete();
                        } catch (Exception e) {
                            throw HyracksDataException.create(e);
                        }
                    }
                };
            }
        };
        try {
            return new TestTypedAdapter(tupleParserFactory, outputType, ctx, configuration, partition);
        } catch (IOException e) {
            throw HyracksDataException.create(e);
        }
    }

    @Override
    public void configure(IServiceContext serviceContext, Map<String, String> configuration) {
        this.serviceContext = serviceContext;
        this.configuration = configuration;
    }

    @Override
    public void setOutputType(ARecordType outputType) {
        this.outputType = outputType;
    }

    @Override
    public void setMetaType(ARecordType metaType) {
    }

    @Override
    public ARecordType getOutputType() {
        return outputType;
    }

    @Override
    public ARecordType getMetaType() {
        return null;
    }
}
