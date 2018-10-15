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
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.value.IBinaryHashFunction;
import org.apache.hyracks.api.dataflow.value.IBinaryHashFunctionFactory;
import org.apache.hyracks.api.dataflow.value.ITuplePartitionComputer;
import org.apache.hyracks.api.dataflow.value.ITuplePartitionComputerFactory;
import org.apache.hyracks.api.exceptions.HyracksDataException;

public class FieldHashPartitionComputerFactory implements ITuplePartitionComputerFactory {
    private static final long serialVersionUID = 1L;
    private final int[] hashFields;
    private final IBinaryHashFunctionFactory[] hashFunctionFactories;

    public FieldHashPartitionComputerFactory(int[] hashFields, IBinaryHashFunctionFactory[] hashFunctionFactories) {
        this.hashFields = hashFields;
        this.hashFunctionFactories = hashFunctionFactories;
    }

    @Override
    public ITuplePartitionComputer createPartitioner(IHyracksTaskContext hyracksTaskContext) {
        final IBinaryHashFunction[] hashFunctions = new IBinaryHashFunction[hashFunctionFactories.length];
        for (int i = 0; i < hashFunctionFactories.length; ++i) {
            hashFunctions[i] = hashFunctionFactories[i].createBinaryHashFunction();
        }
        return new ITuplePartitionComputer() {
            @Override
            public int partition(IFrameTupleAccessor accessor, int tIndex, int nParts) throws HyracksDataException {
                if (nParts == 1) {
                    return 0;
                }
                int h = 0;
                int startOffset = accessor.getTupleStartOffset(tIndex);
                int slotLength = accessor.getFieldSlotsLength();
                for (int j = 0; j < hashFields.length; ++j) {
                    int fIdx = hashFields[j];
                    IBinaryHashFunction hashFn = hashFunctions[j];
                    int fStart = accessor.getFieldStartOffset(tIndex, fIdx);
                    int fEnd = accessor.getFieldEndOffset(tIndex, fIdx);
                    int fh = hashFn.hash(accessor.getBuffer().array(), startOffset + slotLength + fStart,
                            fEnd - fStart);
                    h = h * 31 + fh;
                }
                if (h < 0) {
                    h = -(h + 1);
                }
                return h % nParts;
            }
        };
    }
}
