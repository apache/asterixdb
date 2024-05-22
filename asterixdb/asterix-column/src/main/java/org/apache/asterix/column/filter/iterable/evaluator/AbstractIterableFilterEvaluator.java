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

import java.util.List;

import org.apache.asterix.column.filter.iterable.IColumnIterableFilterEvaluator;
import org.apache.asterix.column.values.IColumnValuesReader;
import org.apache.asterix.formats.nontagged.BinaryBooleanInspector;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.primitive.VoidPointable;

abstract class AbstractIterableFilterEvaluator implements IColumnIterableFilterEvaluator {
    protected final IScalarEvaluator evaluator;
    protected final IColumnValuesReader primaryKeyReader;
    protected final List<IColumnValuesReader> readers;
    private final VoidPointable booleanResult;
    protected int tupleIndex;
    protected int valueIndex;

    AbstractIterableFilterEvaluator(IScalarEvaluator evaluator, List<IColumnValuesReader> readers) {
        this.evaluator = evaluator;
        this.primaryKeyReader = readers.get(0);
        this.readers = readers;
        this.booleanResult = new VoidPointable();
        reset();
    }

    @Override
    public final void reset() {
        tupleIndex = -1;
        valueIndex = -1;
    }

    @Override
    public final int getTupleIndex() {
        return tupleIndex;
    }

    @Override
    public int getValueIndex() {
        return valueIndex;
    }

    @Override
    public final void setAt(int index) throws HyracksDataException {
        // -1 as we want to evaluate the value at 'index'
        int count = index - this.tupleIndex - 1;
        if (count > 0) {
            tupleIndex += count - 1;
            // skip(int) returns the number of skipped values (i.e., without anti-matters)
            valueIndex += skip(count - 1);
        }
    }

    protected final boolean next() throws HyracksDataException {
        // start from 1 as 0 is reserved for the primary key level reader
        boolean advance = true;
        while (advance) {
            advance = primaryKeyReader.next() && primaryKeyReader.isMissing();
            // Advance tuple index
            tupleIndex++;
        }

        // Advance value index
        valueIndex++;
        for (int i = 1; i < readers.size(); i++) {
            if (!readers.get(i).next()) {
                return false;
            }
        }
        return true;
    }

    protected final int skip(int count) throws HyracksDataException {
        // Count non-anti-matter tuples
        int nonAntiMatterCount = 0;
        for (int i = 0; i < count; i++) {
            primaryKeyReader.next();
            nonAntiMatterCount += primaryKeyReader.isValue() ? 0 : 1;
        }
        for (int i = 1; nonAntiMatterCount > 0 && i < readers.size(); i++) {
            readers.get(i).skip(nonAntiMatterCount);
        }
        return nonAntiMatterCount;
    }

    protected final boolean inspect() throws HyracksDataException {
        evaluator.evaluate(null, booleanResult);
        return BinaryBooleanInspector.getBooleanValue(booleanResult.getByteArray(), booleanResult.getStartOffset(),
                booleanResult.getLength());
    }
}
