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
package edu.uci.ics.pregelix.api.util;

import org.apache.hadoop.io.WritableComparable;

import edu.uci.ics.pregelix.api.graph.VertexPartitioner;

/**
 * The deafult vertex partitioner which use the hashcode of the vertex id to determine the partition
 * of the vertex.
 * 
 * @author yingyib
 */
@SuppressWarnings("rawtypes")
public class DefaultVertexPartitioner<I extends WritableComparable> extends VertexPartitioner<I> {

    @Override
    public int getPartitionId(I vertexId, int nPartitions) {
        return vertexId.hashCode() % nPartitions;
    }

}
