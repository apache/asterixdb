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
package org.apache.asterix.column.filter.iterable.evaluator;

import org.apache.asterix.column.filter.iterable.IColumnIterableFilterEvaluator;
import org.apache.asterix.formats.nontagged.BinaryBooleanInspector;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.primitive.VoidPointable;

abstract class AbstractIterableFilterEvaluator implements IColumnIterableFilterEvaluator {
    protected final IScalarEvaluator evaluator;
    private final VoidPointable booleanResult;
    protected int index;

    AbstractIterableFilterEvaluator(IScalarEvaluator evaluator) {
        this.evaluator = evaluator;
        this.booleanResult = new VoidPointable();
        reset();
    }

    @Override
    public final void reset() {
        index = -1;
    }

    @Override
    public final int getTupleIndex() {
        return index;
    }

    @Override
    public final void setAt(int index) throws HyracksDataException {
        int count = index - this.index;
        // count - 1 as we want to evaluate the value at 'index'
        skip(count - 1);
    }

    protected abstract void skip(int count) throws HyracksDataException;

    protected final boolean inspect() throws HyracksDataException {
        evaluator.evaluate(null, booleanResult);
        return BinaryBooleanInspector.getBooleanValue(booleanResult.getByteArray(), booleanResult.getStartOffset(),
                booleanResult.getLength());
    }
}
