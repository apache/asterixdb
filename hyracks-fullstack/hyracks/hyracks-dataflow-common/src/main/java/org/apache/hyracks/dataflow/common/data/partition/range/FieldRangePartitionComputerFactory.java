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
import org.apache.hyracks.api.dataflow.value.ITuplePartitionComputer;
import org.apache.hyracks.api.dataflow.value.ITuplePartitionComputerFactory;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.exceptions.SourceLocation;

public final class FieldRangePartitionComputerFactory extends AbstractFieldRangePartitionComputerFactory
        implements ITuplePartitionComputerFactory {

    private static final long serialVersionUID = 2L;

    private final int[] rangeFields;

    private final boolean usePercentage;

    public FieldRangePartitionComputerFactory(int[] rangeFields, IBinaryComparatorFactory[] comparatorFactories,
            RangeMapSupplier rangeMapSupplier, SourceLocation sourceLocation, boolean usePercentage) {
        super(rangeMapSupplier, comparatorFactories, sourceLocation);
        this.rangeFields = rangeFields;
        this.usePercentage = usePercentage;
    }

    @Override
    public ITuplePartitionComputer createPartitioner(IHyracksTaskContext taskContext) {
        return new AbstractFieldRangeSinglePartitionComputer(taskContext,
                usePercentage ? PercentageRangeMapPartitionComputer::new : RangeMapPartitionComputer::new) {
            @Override
            protected int computePartition(IFrameTupleAccessor accessor, int tIndex, int nParts)
                    throws HyracksDataException {
                return rangeMapPartitionComputer.partition(accessor, tIndex, rangeFields, nParts);
            }
        };
    }
}
