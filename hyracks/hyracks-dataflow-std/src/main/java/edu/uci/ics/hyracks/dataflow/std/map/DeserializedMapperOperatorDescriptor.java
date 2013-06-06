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
package edu.uci.ics.hyracks.dataflow.std.map;

import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.IOpenableDataWriter;
import edu.uci.ics.hyracks.api.dataflow.IOperatorNodePushable;
import edu.uci.ics.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.job.IOperatorDescriptorRegistry;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractSingleActivityOperatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.base.IOpenableDataWriterOperator;
import edu.uci.ics.hyracks.dataflow.std.util.DeserializedOperatorNodePushable;

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
    }

    private static final long serialVersionUID = 1L;

    private final IDeserializedMapperFactory mapperFactory;

    public DeserializedMapperOperatorDescriptor(IOperatorDescriptorRegistry spec,
            IDeserializedMapperFactory mapperFactory, RecordDescriptor recordDescriptor) {
        super(spec, 1, 1);
        this.mapperFactory = mapperFactory;
        recordDescriptors[0] = recordDescriptor;
    }

    @Override
    public IOperatorNodePushable createPushRuntime(IHyracksTaskContext ctx,
            IRecordDescriptorProvider recordDescProvider, int partition, int nPartitions) {
        return new DeserializedOperatorNodePushable(ctx, new MapperOperator(),
                recordDescProvider.getInputRecordDescriptor(getActivityId(), 0));
    }
}