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
package org.apache.hyracks.dataflow.std.file;

import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.IOpenableDataWriter;
import org.apache.hyracks.api.dataflow.IOperatorNodePushable;
import org.apache.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.FileSplit;
import org.apache.hyracks.api.io.IIOManager;
import org.apache.hyracks.api.job.IOperatorDescriptorRegistry;
import org.apache.hyracks.dataflow.std.base.AbstractSingleActivityOperatorDescriptor;
import org.apache.hyracks.dataflow.std.base.IOpenableDataWriterOperator;
import org.apache.hyracks.dataflow.std.util.DeserializedOperatorNodePushable;

public abstract class AbstractFileWriteOperatorDescriptor extends AbstractSingleActivityOperatorDescriptor {
    protected class FileWriteOperator implements IOpenableDataWriterOperator {
        private final IIOManager ioManager;
        private int index;
        private IRecordWriter writer;

        FileWriteOperator(IIOManager ioManager, int index) {
            this.ioManager = ioManager;
            this.index = index;
        }

        @Override
        public void setDataWriter(int index, IOpenableDataWriter<Object[]> writer) {
            throw new IllegalArgumentException();
        }

        @Override
        public void open() throws HyracksDataException {
            FileSplit split = splits[index];
            writer = createRecordWriter(ioManager, split, index);
        }

        @Override
        public void close() throws HyracksDataException {
            writer.close();
        }

        @Override
        public void fail() throws HyracksDataException {
        }

        @Override
        public void writeData(Object[] data) throws HyracksDataException {
            writer.write(data);
        }

        @Override
        public void flush() throws HyracksDataException {
            // This is a kind of a sink operator and hence, flush() is a no op
        }
    }

    private static final long serialVersionUID = 1L;

    protected FileSplit[] splits;

    public FileSplit[] getSplits() {
        return splits;
    }

    public void setSplits(FileSplit[] splits) {
        this.splits = splits;
    }

    public AbstractFileWriteOperatorDescriptor(IOperatorDescriptorRegistry spec, FileSplit[] splits) {
        super(spec, 1, 0);
        this.splits = splits;
    }

    protected abstract IRecordWriter createRecordWriter(IIOManager ioManager, FileSplit fileSplit, int index)
            throws HyracksDataException;

    @Override
    public IOperatorNodePushable createPushRuntime(IHyracksTaskContext ctx,
            IRecordDescriptorProvider recordDescProvider, int partition, int nPartitions) {
        return new DeserializedOperatorNodePushable(ctx, new FileWriteOperator(ctx.getIoManager(), partition),
                recordDescProvider.getInputRecordDescriptor(getActivityId(), 0));
    }
}
