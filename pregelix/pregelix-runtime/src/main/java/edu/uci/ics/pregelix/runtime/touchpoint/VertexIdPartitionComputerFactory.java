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
package edu.uci.ics.pregelix.runtime.touchpoint;

import org.apache.hadoop.io.Writable;

import edu.uci.ics.hyracks.api.comm.IFrameTupleAccessor;
import edu.uci.ics.hyracks.api.dataflow.value.ITuplePartitionComputer;
import edu.uci.ics.hyracks.api.dataflow.value.ITuplePartitionComputerFactory;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.pregelix.dataflow.base.IConfigurationFactory;
import edu.uci.ics.pregelix.dataflow.std.base.ISerializerDeserializerFactory;

public class VertexIdPartitionComputerFactory<K extends Writable, V extends Writable> implements
        ITuplePartitionComputerFactory {
    private static final long serialVersionUID = 1L;

    public VertexIdPartitionComputerFactory(ISerializerDeserializerFactory<K> keyIOFactory,
            IConfigurationFactory confFactory) {
    }

    public ITuplePartitionComputer createPartitioner() {
        try {
            return new ITuplePartitionComputer() {

                public int partition(IFrameTupleAccessor accessor, int tIndex, int nParts) throws HyracksDataException {
                    int keyStart = accessor.getTupleStartOffset(tIndex) + accessor.getFieldSlotsLength()
                            + accessor.getFieldStartOffset(tIndex, 0);
                    int len = accessor.getFieldLength(tIndex, 0);
                    return Math.abs(hash(accessor.getBuffer().array(), keyStart, len) % nParts);
                }

                private int hash(byte[] bytes, int offset, int length) {
                    int value = 1;
                    int end = offset + length;
                    for (int i = offset; i < end; i++)
                        value = value * 31 + (int) bytes[i];
                    return value;
                }
            };
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

}