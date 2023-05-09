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

import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.value.IBinaryHashFunction;
import org.apache.hyracks.api.dataflow.value.IBinaryHashFunctionFactory;
import org.apache.hyracks.api.dataflow.value.ITuplePartitionComputer;
import org.apache.hyracks.api.dataflow.value.ITuplePartitionComputerFactory;

import it.unimi.dsi.fastutil.ints.Int2IntMap;
import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;

public class FieldHashPartitionComputerFactory implements ITuplePartitionComputerFactory {

    private static final long serialVersionUID = 2L;
    private final int[] hashFields;
    private final IBinaryHashFunctionFactory[] hashFunctionFactories;
    private final int[][] partitionsMap;

    public static FieldHashPartitionComputerFactory of(int[] hashFields,
            IBinaryHashFunctionFactory[] hashFunctionFactories) {
        return new FieldHashPartitionComputerFactory(hashFields, hashFunctionFactories, null);
    }

    public static FieldHashPartitionComputerFactory withMap(int[] hashFields,
            IBinaryHashFunctionFactory[] hashFunctionFactories, int[][] partitionsMap) {
        return new FieldHashPartitionComputerFactory(hashFields, hashFunctionFactories, partitionsMap);
    }

    private FieldHashPartitionComputerFactory(int[] hashFields, IBinaryHashFunctionFactory[] hashFunctionFactories,
            int[][] partitionsMap) {
        this.hashFields = hashFields;
        this.hashFunctionFactories = hashFunctionFactories;
        this.partitionsMap = partitionsMap;
    }

    @Override
    public ITuplePartitionComputer createPartitioner(IHyracksTaskContext ctx) {
        final IBinaryHashFunction[] hashFunctions = new IBinaryHashFunction[hashFunctionFactories.length];
        for (int i = 0; i < hashFunctionFactories.length; ++i) {
            hashFunctions[i] = hashFunctionFactories[i].createBinaryHashFunction();
        }
        if (partitionsMap == null) {
            return new FieldHashPartitionComputer(hashFields, hashFunctions, null);
        } else {
            Int2IntMap storagePartition2Compute = new Int2IntOpenHashMap();
            for (int i = 0; i < partitionsMap.length; i++) {
                for (int storagePartition : partitionsMap[i]) {
                    storagePartition2Compute.put(storagePartition, i);
                }
            }
            return new FieldHashPartitionComputer(hashFields, hashFunctions, storagePartition2Compute);
        }
    }
}
