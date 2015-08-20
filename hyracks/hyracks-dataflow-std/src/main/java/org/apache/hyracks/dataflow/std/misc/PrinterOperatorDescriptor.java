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
package edu.uci.ics.hyracks.dataflow.std.misc;

import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.IOpenableDataWriter;
import edu.uci.ics.hyracks.api.dataflow.IOperatorNodePushable;
import edu.uci.ics.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.job.IOperatorDescriptorRegistry;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractSingleActivityOperatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.base.IOpenableDataWriterOperator;
import edu.uci.ics.hyracks.dataflow.std.util.DeserializedOperatorNodePushable;
import edu.uci.ics.hyracks.dataflow.std.util.StringSerializationUtils;

public class PrinterOperatorDescriptor extends AbstractSingleActivityOperatorDescriptor {
    private static final long serialVersionUID = 1L;

    public PrinterOperatorDescriptor(IOperatorDescriptorRegistry spec) {
        super(spec, 1, 0);
    }

    private class PrinterOperator implements IOpenableDataWriterOperator {
        @Override
        public void open() throws HyracksDataException {
        }

        @Override
        public void close() throws HyracksDataException {
        }

        @Override
        public void fail() throws HyracksDataException {
        }

        @Override
        public void writeData(Object[] data) throws HyracksDataException {
            for (int i = 0; i < data.length; ++i) {
                System.err.print(StringSerializationUtils.toString(data[i]));
                System.err.print(", ");
            }
            System.err.println();
        }

        @Override
        public void setDataWriter(int index, IOpenableDataWriter<Object[]> writer) {
            throw new IllegalArgumentException();
        }
    }

    @Override
    public IOperatorNodePushable createPushRuntime(IHyracksTaskContext ctx,
            IRecordDescriptorProvider recordDescProvider, int partition, int nPartitions) {
        return new DeserializedOperatorNodePushable(ctx, new PrinterOperator(),
                recordDescProvider.getInputRecordDescriptor(getActivityId(), 0));
    }
}