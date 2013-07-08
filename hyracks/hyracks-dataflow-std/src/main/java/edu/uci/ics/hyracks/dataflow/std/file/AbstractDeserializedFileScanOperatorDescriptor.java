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
package edu.uci.ics.hyracks.dataflow.std.file;

import java.io.File;

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

public abstract class AbstractDeserializedFileScanOperatorDescriptor extends AbstractSingleActivityOperatorDescriptor {
    private static final long serialVersionUID = 1L;

    protected FileSplit[] splits;

    public AbstractDeserializedFileScanOperatorDescriptor(IOperatorDescriptorRegistry spec, FileSplit[] splits,
            RecordDescriptor recordDescriptor) {
        super(spec, 0, 1);
        recordDescriptors[0] = recordDescriptor;
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
            RecordDescriptor desc = recordDescriptors[0];
            IRecordReader reader;
            try {
                reader = createRecordReader(split.getLocalFile().getFile(), desc);
            } catch (Exception e) {
                throw new HyracksDataException(e);
            }
            if (desc == null) {
                desc = recordDescriptors[0];
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
                throw new HyracksDataException(e);
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
    }

    @Override
    public IOperatorNodePushable createPushRuntime(IHyracksTaskContext ctx,
            IRecordDescriptorProvider recordDescProvider, int partition, int nPartitions) {
        return new DeserializedOperatorNodePushable(ctx, new DeserializedFileScanOperator(partition), null);
    }
}