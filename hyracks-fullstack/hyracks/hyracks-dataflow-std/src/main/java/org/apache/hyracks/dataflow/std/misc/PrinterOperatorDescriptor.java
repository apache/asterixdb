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
package org.apache.hyracks.dataflow.std.misc;

import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.IOpenableDataWriter;
import org.apache.hyracks.api.dataflow.IOperatorNodePushable;
import org.apache.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.job.IOperatorDescriptorRegistry;
import org.apache.hyracks.dataflow.std.base.AbstractSingleActivityOperatorDescriptor;
import org.apache.hyracks.dataflow.std.base.IOpenableDataWriterOperator;
import org.apache.hyracks.dataflow.std.util.DeserializedOperatorNodePushable;
import org.apache.hyracks.dataflow.std.util.StringSerializationUtils;

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

        @Override
        public void flush() throws HyracksDataException {
            System.err.flush();
        }
    }

    @Override
    public IOperatorNodePushable createPushRuntime(IHyracksTaskContext ctx,
            IRecordDescriptorProvider recordDescProvider, int partition, int nPartitions) {
        return new DeserializedOperatorNodePushable(ctx, new PrinterOperator(),
                recordDescProvider.getInputRecordDescriptor(getActivityId(), 0));
    }
}
