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

import java.util.ArrayList;
import java.util.List;

import org.apache.asterix.column.values.IColumnValuesReader;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.api.exceptions.HyracksDataException;

public class ColumnarRepeatedIterableFilterEvaluator extends AbstractIterableFilterEvaluator {
    private final List<IColumnValuesReader> repeatedReaders;

    ColumnarRepeatedIterableFilterEvaluator(IScalarEvaluator evaluator, List<IColumnValuesReader> readers) {
        super(evaluator, readers);
        repeatedReaders = new ArrayList<>();
        for (IColumnValuesReader reader : readers) {
            if (reader.isRepeated()) {
                repeatedReaders.add(reader);
            }
        }
    }

    @Override
    public boolean evaluate() throws HyracksDataException {
        boolean result = false;
        while (!result && next()) {
            // TODO handle nested repetition (x = unnest --> y = unnest --> select (x = 1 AND y = 3))
            // TODO we need a way to 'rewind' y for each x
            result = evaluateRepeated();
        }
        if (!result) {
            // Last tuple does not satisfy the condition
            tupleIndex++;
            valueIndex++;
        }
        return result;
    }

    private boolean evaluateRepeated() throws HyracksDataException {
        boolean result = false;
        boolean doNext;
        do {
            doNext = false;
            result |= inspect();
            for (int i = 0; i < repeatedReaders.size(); i++) {
                IColumnValuesReader reader = repeatedReaders.get(i);
                boolean repeatedValue = reader.isRepeatedValue() && !reader.isLastDelimiter();
                doNext |= repeatedValue;
                if (repeatedValue) {
                    reader.next();
                }
            }
        } while (doNext);
        return result;
    }
}
