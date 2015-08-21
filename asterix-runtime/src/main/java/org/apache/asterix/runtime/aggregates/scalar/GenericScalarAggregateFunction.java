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
package org.apache.asterix.runtime.aggregates.scalar;

import org.apache.asterix.runtime.aggregates.base.SingleFieldFrameTupleReference;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.runtime.base.ICopyAggregateFunction;
import org.apache.hyracks.algebricks.runtime.base.ICopyEvaluator;
import org.apache.hyracks.algebricks.runtime.base.ICopyUnnestingFunction;
import org.apache.hyracks.algebricks.runtime.base.ICopyUnnestingFunctionFactory;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.dataflow.common.data.accessors.IFrameTupleReference;

/**
 * Implements scalar aggregates by iterating over a collection with the ScanCollection unnesting function,
 * and applying the corresponding ICopyAggregateFunction to each collection-item.
 */
public class GenericScalarAggregateFunction implements ICopyEvaluator {

    private final ArrayBackedValueStorage listItemOut = new ArrayBackedValueStorage();
    private final ICopyAggregateFunction aggFunc;
    private final ICopyUnnestingFunction scanCollection;

    private final SingleFieldFrameTupleReference itemTuple = new SingleFieldFrameTupleReference();

    public GenericScalarAggregateFunction(ICopyAggregateFunction aggFunc,
            ICopyUnnestingFunctionFactory scanCollectionFactory) throws AlgebricksException {
        this.aggFunc = aggFunc;
        this.scanCollection = scanCollectionFactory.createUnnestingFunction(listItemOut);
        listItemOut.reset();
    }

    @Override
    public void evaluate(IFrameTupleReference tuple) throws AlgebricksException {
        scanCollection.init(tuple);
        aggFunc.init();
        while (scanCollection.step()) {
            itemTuple.reset(listItemOut.getByteArray(), 0, listItemOut.getLength());
            aggFunc.step(itemTuple);
            listItemOut.reset();
        }
        aggFunc.finish();
    }
}
