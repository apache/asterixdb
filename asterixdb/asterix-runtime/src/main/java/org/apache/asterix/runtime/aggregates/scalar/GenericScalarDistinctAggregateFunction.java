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

package org.apache.asterix.runtime.aggregates.scalar;

import java.util.List;

import org.apache.asterix.builders.AbvsBuilderFactory;
import org.apache.asterix.builders.ArrayListFactory;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.om.util.container.IObjectPool;
import org.apache.asterix.om.util.container.ListObjectPool;
import org.apache.asterix.runtime.aggregates.utils.PointableHashSet;
import org.apache.hyracks.algebricks.runtime.base.IAggregateEvaluator;
import org.apache.hyracks.algebricks.runtime.base.IEvaluatorContext;
import org.apache.hyracks.algebricks.runtime.base.IUnnestingEvaluatorFactory;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.exceptions.SourceLocation;
import org.apache.hyracks.data.std.api.IMutableValueStorage;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;

/**
 * Implements scalar distinct aggregates by iterating over a collection with the ScanCollection unnesting function,
 * and applying the corresponding aggregate function to each distinct collection-item.
 */
public class GenericScalarDistinctAggregateFunction extends GenericScalarAggregateFunction {

    private final IObjectPool<IMutableValueStorage, ATypeTag> storageAllocator;

    private final IObjectPool<List<IPointable>, ATypeTag> arrayListAllocator;

    private final PointableHashSet itemSet;

    public GenericScalarDistinctAggregateFunction(IAggregateEvaluator aggFunc,
            IUnnestingEvaluatorFactory scanCollectionFactory, IEvaluatorContext context, SourceLocation sourceLoc,
            IAType itemType) throws HyracksDataException {
        super(aggFunc, scanCollectionFactory, context, sourceLoc);
        storageAllocator = new ListObjectPool<>(new AbvsBuilderFactory());
        arrayListAllocator = new ListObjectPool<>(new ArrayListFactory<>());
        itemSet = new PointableHashSet(arrayListAllocator, itemType) {
            @Override
            protected IPointable makeStoredItem(IPointable item) throws HyracksDataException {
                ArrayBackedValueStorage abvs = (ArrayBackedValueStorage) storageAllocator.allocate(null);
                abvs.assign(item);
                return abvs;
            }
        };
    }

    @Override
    protected void aggInit() throws HyracksDataException {
        super.aggInit();

        storageAllocator.reset();
        arrayListAllocator.reset();
        itemSet.clear();
    }

    @Override
    protected void aggStep(IPointable item) throws HyracksDataException {
        if (itemSet.add(item)) {
            super.aggStep(item);
        }
    }
}
