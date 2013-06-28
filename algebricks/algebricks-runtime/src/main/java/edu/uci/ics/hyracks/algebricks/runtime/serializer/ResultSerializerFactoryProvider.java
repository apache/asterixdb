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
package edu.uci.ics.hyracks.algebricks.runtime.serializer;

import java.io.PrintStream;
import java.nio.BufferOverflowException;

import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.data.IAWriter;
import edu.uci.ics.hyracks.algebricks.data.IAWriterFactory;
import edu.uci.ics.hyracks.algebricks.data.IPrinterFactory;
import edu.uci.ics.hyracks.algebricks.data.IResultSerializerFactoryProvider;
import edu.uci.ics.hyracks.api.comm.IFrameTupleAccessor;
import edu.uci.ics.hyracks.api.dataflow.value.IResultSerializer;
import edu.uci.ics.hyracks.api.dataflow.value.IResultSerializerFactory;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;

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
                final IAWriter writer = writerFactory.createWriter(fields, printStream, printerFactories,
                        inputRecordDesc);

                return new IResultSerializer() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public void init() throws HyracksDataException {
                        try {
                            writer.init();
                        } catch (AlgebricksException e) {
                            throw new HyracksDataException(e);
                        }
                    }

                    @Override
                    public boolean appendTuple(IFrameTupleAccessor tAccess, int tIdx) throws HyracksDataException {
                        try {
                            writer.printTuple(tAccess, tIdx);
                        } catch (BufferOverflowException e) {
                            return false;
                        } catch (AlgebricksException e) {
                            throw new HyracksDataException(e);
                        }
                        return true;
                    }
                };
            }
        };
    }
}
