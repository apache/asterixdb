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
package org.apache.hyracks.dataflow.common.data.partition;

import org.apache.hyracks.api.comm.IFrameTupleAccessor;
import org.apache.hyracks.api.dataflow.value.IBinaryHashFunction;
import org.apache.hyracks.api.dataflow.value.ITuplePartitionComputer;
import org.apache.hyracks.api.exceptions.HyracksDataException;

import it.unimi.dsi.fastutil.ints.Int2IntMap;

public class FieldHashPartitionComputer extends HashPartitioner implements ITuplePartitionComputer {

    public FieldHashPartitionComputer(int[] hashFields, IBinaryHashFunction[] hashFunctions,
            Int2IntMap storagePartition2Compute) {
        super(hashFields, hashFunctions, storagePartition2Compute);
    }

    @Override
    public int partition(IFrameTupleAccessor accessor, int tIndex, int nParts) throws HyracksDataException {
        return super.partition(accessor, tIndex, nParts);
    }
}
