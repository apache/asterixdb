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
package org.apache.hyracks.dataflow.hadoop.data;

import java.io.DataInputStream;

import org.apache.hadoop.io.Writable;
import org.apache.hyracks.api.comm.IFrameTupleAccessor;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.dataflow.value.ITuplePartitionComputer;
import org.apache.hyracks.api.dataflow.value.ITuplePartitionComputerFactory;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.comm.util.ByteBufferInputStream;

public class HadoopHashTuplePartitionComputerFactory<K extends Writable> implements ITuplePartitionComputerFactory {
    private static final long serialVersionUID = 1L;
    private final ISerializerDeserializer<K> keyIO;

    public HadoopHashTuplePartitionComputerFactory(ISerializerDeserializer<K> keyIO) {
        this.keyIO = keyIO;
    }

    @Override
    public ITuplePartitionComputer createPartitioner(IHyracksTaskContext ctx, int partition) {
        return new ITuplePartitionComputer() {
            private final ByteBufferInputStream bbis = new ByteBufferInputStream();
            private final DataInputStream dis = new DataInputStream(bbis);

            @Override
            public int partition(IFrameTupleAccessor accessor, int tIndex, int nParts) throws HyracksDataException {
                int keyStart = accessor.getTupleStartOffset(tIndex) + accessor.getFieldSlotsLength()
                        + accessor.getFieldStartOffset(tIndex, 0);
                bbis.setByteBuffer(accessor.getBuffer(), keyStart);
                K key = keyIO.deserialize(dis);
                int h = key.hashCode();
                if (h < 0) {
                    h = -h;
                }
                return h % nParts;
            }
        };
    }
}
