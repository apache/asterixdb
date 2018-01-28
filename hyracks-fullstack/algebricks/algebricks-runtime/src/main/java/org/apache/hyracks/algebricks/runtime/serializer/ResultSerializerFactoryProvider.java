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
package org.apache.hyracks.algebricks.runtime.serializer;

import java.io.PrintStream;
import java.nio.BufferOverflowException;

import org.apache.hyracks.algebricks.data.IAWriter;
import org.apache.hyracks.algebricks.data.IAWriterFactory;
import org.apache.hyracks.algebricks.data.IPrinterFactory;
import org.apache.hyracks.algebricks.data.IResultSerializerFactoryProvider;
import org.apache.hyracks.api.comm.IFrameTupleAccessor;
import org.apache.hyracks.api.dataflow.value.IResultSerializer;
import org.apache.hyracks.api.dataflow.value.IResultSerializerFactory;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;

public class ResultSerializerFactoryProvider implements IResultSerializerFactoryProvider {
    private static final long serialVersionUID = 1L;

    public static final ResultSerializerFactoryProvider INSTANCE = new ResultSerializerFactoryProvider();

    private ResultSerializerFactoryProvider() {
    }

    @Override
    public IResultSerializerFactory getAqlResultSerializerFactoryProvider(final int[] fields,
            final IPrinterFactory[] printerFactories, final IAWriterFactory writerFactory) {
        return new IResultSerializerFactory() {
            private static final long serialVersionUID = 1L;

            @Override
            public IResultSerializer createResultSerializer(RecordDescriptor inputRecordDesc, PrintStream printStream) {
                final IAWriter writer =
                        writerFactory.createWriter(fields, printStream, printerFactories, inputRecordDesc);

                return new IResultSerializer() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public void init() throws HyracksDataException {
                        writer.init();
                    }

                    @Override
                    public boolean appendTuple(IFrameTupleAccessor tAccess, int tIdx) throws HyracksDataException {
                        try {
                            writer.printTuple(tAccess, tIdx);
                        } catch (BufferOverflowException e) {
                            return false;
                        }
                        return true;
                    }
                };
            }
        };
    }
}
