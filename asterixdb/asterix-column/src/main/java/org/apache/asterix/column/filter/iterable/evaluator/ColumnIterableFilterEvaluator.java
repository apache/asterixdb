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

import org.apache.asterix.column.values.IColumnValuesReader;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.api.exceptions.HyracksDataException;

public class ColumnIterableFilterEvaluator extends AbstractIterableFilterEvaluator {

    public ColumnIterableFilterEvaluator(IScalarEvaluator evaluator, List<IColumnValuesReader> readers) {
        super(evaluator, readers);
    }

    @Override
    public boolean evaluate() throws HyracksDataException {
        boolean result = false;
        while (!result && next()) {
            result = inspect();
        }
        if (!result) {
            // Last tuple does not satisfy the condition
            tupleIndex++;
            valueIndex++;
        }
        return result;
    }
}
