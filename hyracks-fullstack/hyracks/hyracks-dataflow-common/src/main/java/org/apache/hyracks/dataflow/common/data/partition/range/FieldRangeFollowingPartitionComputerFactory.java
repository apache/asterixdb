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

package org.apache.hyracks.dataflow.common.data.partition.range;

import org.apache.hyracks.api.comm.IFrameTupleAccessor;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.dataflow.value.ITupleMultiPartitionComputer;
import org.apache.hyracks.api.dataflow.value.ITupleMultiPartitionComputerFactory;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.exceptions.SourceLocation;

public final class FieldRangeFollowingPartitionComputerFactory extends AbstractFieldRangePartitionComputerFactory
        implements ITupleMultiPartitionComputerFactory {

    private static final long serialVersionUID = 1L;

    private final int[] rangeFields;

    public FieldRangeFollowingPartitionComputerFactory(int[] rangeFields,
            IBinaryComparatorFactory[] comparatorFactories, RangeMapSupplier rangeMapSupplier,
            SourceLocation sourceLocation) {
        super(rangeMapSupplier, comparatorFactories, sourceLocation);
        this.rangeFields = rangeFields;
    }

    @Override
    public ITupleMultiPartitionComputer createPartitioner(IHyracksTaskContext taskContext) {
        return new AbstractFieldRangeMultiPartitionComputer(taskContext) {
            @Override
            protected int computeStartPartition(IFrameTupleAccessor accessor, int tIndex, int nParts)
                    throws HyracksDataException {
                return rangeMapPartitionComputer.partition(accessor, tIndex, rangeFields, nParts);
            }

            @Override
            protected int computeEndPartition(IFrameTupleAccessor accessor, int tIndex, int nParts) {
                return nParts - 1;
            }
        };
    }
}
