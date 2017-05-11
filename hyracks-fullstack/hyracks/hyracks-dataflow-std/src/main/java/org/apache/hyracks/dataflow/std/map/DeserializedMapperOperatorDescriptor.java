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
package org.apache.hyracks.dataflow.std.map;

import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.IOpenableDataWriter;
import org.apache.hyracks.api.dataflow.IOperatorNodePushable;
import org.apache.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.job.IOperatorDescriptorRegistry;
import org.apache.hyracks.dataflow.std.base.AbstractSingleActivityOperatorDescriptor;
import org.apache.hyracks.dataflow.std.base.IOpenableDataWriterOperator;
import org.apache.hyracks.dataflow.std.util.DeserializedOperatorNodePushable;

public class DeserializedMapperOperatorDescriptor extends AbstractSingleActivityOperatorDescriptor {
    private class MapperOperator implements IOpenableDataWriterOperator {
        private IDeserializedMapper mapper;
        private IOpenableDataWriter<Object[]> writer;

        @Override
        public void close() throws HyracksDataException {
            writer.close();
        }

        @Override
        public void fail() throws HyracksDataException {
            writer.fail();
        }

        @Override
        public void open() throws HyracksDataException {
            mapper = mapperFactory.createMapper();
            writer.open();
        }

        @Override
        public void setDataWriter(int index, IOpenableDataWriter<Object[]> writer) {
            if (index != 0) {
                throw new IllegalArgumentException();
            }
            this.writer = writer;
        }

        @Override
        public void writeData(Object[] data) throws HyracksDataException {
            mapper.map(data, writer);
        }

        @Override
        public void flush() throws HyracksDataException {
        }
    }

    private static final long serialVersionUID = 1L;

    private final IDeserializedMapperFactory mapperFactory;

    public DeserializedMapperOperatorDescriptor(IOperatorDescriptorRegistry spec,
            IDeserializedMapperFactory mapperFactory, RecordDescriptor recordDescriptor) {
        super(spec, 1, 1);
        this.mapperFactory = mapperFactory;
        outRecDescs[0] = recordDescriptor;
    }

    @Override
    public IOperatorNodePushable createPushRuntime(IHyracksTaskContext ctx,
            IRecordDescriptorProvider recordDescProvider, int partition, int nPartitions) {
        return new DeserializedOperatorNodePushable(ctx, new MapperOperator(),
                recordDescProvider.getInputRecordDescriptor(getActivityId(), 0));
    }
}
