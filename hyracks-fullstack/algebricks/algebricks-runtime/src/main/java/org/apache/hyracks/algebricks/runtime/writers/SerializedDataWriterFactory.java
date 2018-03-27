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
package org.apache.hyracks.algebricks.runtime.writers;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.PrintStream;

import org.apache.hyracks.algebricks.data.IAWriter;
import org.apache.hyracks.algebricks.data.IAWriterFactory;
import org.apache.hyracks.algebricks.data.IPrinterFactory;
import org.apache.hyracks.api.comm.IFrameTupleAccessor;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;

public class SerializedDataWriterFactory implements IAWriterFactory {

    private static final long serialVersionUID = 1L;

    @Override
    public IAWriter createWriter(final int[] fields, final PrintStream ps, IPrinterFactory[] printerFactories,
            final RecordDescriptor inputRecordDescriptor) {
        return new IAWriter() {

            @Override
            public void init() throws HyracksDataException {
                // dump the SerializerDeserializers to disk
                try {
                    ByteArrayOutputStream baos = new ByteArrayOutputStream();
                    ObjectOutputStream oos = new ObjectOutputStream(baos);
                    oos.writeObject(inputRecordDescriptor);
                    baos.writeTo(ps);
                    oos.close();
                } catch (IOException e) {
                    throw HyracksDataException.create(e);
                }
            }

            @Override
            public void printTuple(IFrameTupleAccessor tAccess, int tIdx) throws HyracksDataException {
                for (int i = 0; i < fields.length; i++) {
                    int fldStart = tAccess.getTupleStartOffset(tIdx) + tAccess.getFieldSlotsLength()
                            + tAccess.getFieldStartOffset(tIdx, fields[i]);
                    int fldLen = tAccess.getFieldLength(tIdx, fields[i]);
                    ps.write(tAccess.getBuffer().array(), fldStart, fldLen);
                }
            }
        };
    }
}
