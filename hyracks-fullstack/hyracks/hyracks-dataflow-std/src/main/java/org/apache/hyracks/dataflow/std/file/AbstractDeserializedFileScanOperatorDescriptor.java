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

import java.io.File;

import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.IOpenableDataWriter;
import org.apache.hyracks.api.dataflow.IOperatorNodePushable;
import org.apache.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.FileSplit;
import org.apache.hyracks.api.job.IOperatorDescriptorRegistry;
import org.apache.hyracks.dataflow.std.base.AbstractSingleActivityOperatorDescriptor;
import org.apache.hyracks.dataflow.std.base.IOpenableDataWriterOperator;
import org.apache.hyracks.dataflow.std.util.DeserializedOperatorNodePushable;

public abstract class AbstractDeserializedFileScanOperatorDescriptor extends AbstractSingleActivityOperatorDescriptor {
    private static final long serialVersionUID = 1L;

    protected FileSplit[] splits;

    public AbstractDeserializedFileScanOperatorDescriptor(IOperatorDescriptorRegistry spec, FileSplit[] splits,
            RecordDescriptor recordDescriptor) {
        super(spec, 0, 1);
        outRecDescs[0] = recordDescriptor;
        this.splits = splits;
    }

    protected abstract IRecordReader createRecordReader(File file, RecordDescriptor desc) throws Exception;

    protected abstract void configure() throws Exception;

    protected class DeserializedFileScanOperator implements IOpenableDataWriterOperator {
        private IOpenableDataWriter<Object[]> writer;
        private int index;

        DeserializedFileScanOperator(int index) {
            this.index = index;
        }

        @Override
        public void setDataWriter(int index, IOpenableDataWriter<Object[]> writer) {
            if (index != 0) {
                throw new IndexOutOfBoundsException("Invalid index: " + index);
            }
            this.writer = writer;
        }

        @Override
        public void open() throws HyracksDataException {
            FileSplit split = splits[index];
            RecordDescriptor desc = outRecDescs[0];
            IRecordReader reader;
            try {
                reader = createRecordReader(split.getFile(null), desc);
            } catch (Exception e) {
                throw HyracksDataException.create(e);
            }
            if (desc == null) {
                desc = outRecDescs[0];
            }
            writer.open();
            try {
                while (true) {
                    Object[] record = new Object[desc.getFieldCount()];
                    if (!reader.read(record)) {
                        break;
                    }
                    writer.writeData(record);
                }
            } catch (Exception e) {
                writer.fail();
                throw HyracksDataException.create(e);
            } finally {
                reader.close();
                writer.close();
            }
        }

        @Override
        public void close() throws HyracksDataException {
            // do nothing
        }

        @Override
        public void writeData(Object[] data) throws HyracksDataException {
            throw new UnsupportedOperationException();
        }

        @Override
        public void fail() throws HyracksDataException {
            // do nothing
        }

        @Override
        public void flush() throws HyracksDataException {
            // do nothing
        }
    }

    @Override
    public IOperatorNodePushable createPushRuntime(IHyracksTaskContext ctx,
            IRecordDescriptorProvider recordDescProvider, int partition, int nPartitions) {
        return new DeserializedOperatorNodePushable(ctx, new DeserializedFileScanOperator(partition), null);
    }
}
