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

import java.io.DataInputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.WritableComparable;

import edu.uci.ics.hyracks.api.comm.IFrameTupleAccessor;
import edu.uci.ics.hyracks.api.dataflow.value.ITuplePartitionComputer;
import edu.uci.ics.hyracks.api.dataflow.value.ITuplePartitionComputerFactory;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.comm.util.ByteBufferInputStream;
import edu.uci.ics.pregelix.api.graph.VertexPartitioner;
import edu.uci.ics.pregelix.api.util.BspUtils;
import edu.uci.ics.pregelix.dataflow.base.IConfigurationFactory;

/**
 * The vertex-based partition computer factory.
 * It is used to support customized graph partitioning function.
 * 
 * @author yingyib
 */
public class VertexPartitionComputerFactory implements ITuplePartitionComputerFactory {

    private static final long serialVersionUID = 1L;
    private final IConfigurationFactory confFactory;

    public VertexPartitionComputerFactory(IConfigurationFactory confFactory) {
        this.confFactory = confFactory;
    }

    @SuppressWarnings("rawtypes")
    public ITuplePartitionComputer createPartitioner() {
        try {
            final Configuration conf = confFactory.createConfiguration();
            return new ITuplePartitionComputer() {
                private final ByteBufferInputStream bbis = new ByteBufferInputStream();
                private final DataInputStream dis = new DataInputStream(bbis);
                private final VertexPartitioner partitioner = BspUtils.createVertexPartitioner(conf);
                private final WritableComparable vertexId = BspUtils.createVertexIndex(conf);

                @SuppressWarnings("unchecked")
                public int partition(IFrameTupleAccessor accessor, int tIndex, int nParts) throws HyracksDataException {
                    try {
                        int keyStart = accessor.getTupleStartOffset(tIndex) + accessor.getFieldSlotsLength()
                                + accessor.getFieldStartOffset(tIndex, 0);
                        bbis.setByteBuffer(accessor.getBuffer(), keyStart);
                        vertexId.readFields(dis);
                        return Math.abs(partitioner.getPartitionId(vertexId, nParts) % nParts);
                    } catch (Exception e) {
                        throw new HyracksDataException(e);
                    }
                }
            };
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
