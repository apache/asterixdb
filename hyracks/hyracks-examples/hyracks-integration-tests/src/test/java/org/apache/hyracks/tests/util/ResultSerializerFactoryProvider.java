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
package org.apache.hyracks.tests.util;

import java.io.DataInputStream;
import java.io.PrintStream;
import java.io.Serializable;

import org.apache.hyracks.api.comm.IFrameTupleAccessor;
import org.apache.hyracks.api.dataflow.value.IResultSerializer;
import org.apache.hyracks.api.dataflow.value.IResultSerializerFactory;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.comm.util.ByteBufferInputStream;

public class ResultSerializerFactoryProvider implements Serializable {
    private static final long serialVersionUID = 1L;

    public static final ResultSerializerFactoryProvider INSTANCE = new ResultSerializerFactoryProvider();

    private ResultSerializerFactoryProvider() {
    }

    public IResultSerializerFactory getResultSerializerFactoryProvider() {
        return new IResultSerializerFactory() {
            private static final long serialVersionUID = 1L;

            @Override
            public IResultSerializer createResultSerializer(final RecordDescriptor recordDesc,
                    final PrintStream printStream) {
                return new IResultSerializer() {
                    private static final long serialVersionUID = 1L;

                    ByteBufferInputStream bbis = new ByteBufferInputStream();
                    DataInputStream di = new DataInputStream(bbis);

                    @Override
                    public void init() throws HyracksDataException {

                    }

                    @Override
                    public boolean appendTuple(IFrameTupleAccessor tAccess, int tIdx) throws HyracksDataException {
                        int start = tAccess.getTupleStartOffset(tIdx) + tAccess.getFieldSlotsLength();

                        bbis.setByteBuffer(tAccess.getBuffer(), start);

                        Object[] record = new Object[recordDesc.getFieldCount()];
                        for (int i = 0; i < record.length; ++i) {
                            Object instance = recordDesc.getFields()[i].deserialize(di);
                            if (i == 0) {
                                printStream.print(String.valueOf(instance));
                            } else {
                                printStream.print(", " + String.valueOf(instance));
                            }
                        }
                        printStream.println();
                        return true;
                    }
                };
            }
        };
    }
}
