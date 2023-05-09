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
package org.apache.asterix.column.filter.iterable.accessor;

import org.apache.asterix.column.assembler.value.IValueGetter;
import org.apache.asterix.column.values.IColumnValuesReader;
import org.apache.asterix.runtime.evaluators.functions.PointableHelper;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.dataflow.common.data.accessors.IFrameTupleReference;

public class ColumnFilterValueAccessorEvaluator implements IScalarEvaluator {
    private final IColumnValuesReader reader;
    private final IValueGetter getter;

    public ColumnFilterValueAccessorEvaluator(IColumnValuesReader reader, IValueGetter getter) {
        this.reader = reader;
        this.getter = getter;
    }

    @Override
    public void evaluate(IFrameTupleReference tuple, IPointable result) throws HyracksDataException {
        if (reader.isRepeated() && !reader.isRepeatedValue() || reader.isMissing()) {
            PointableHelper.setMissing(result);
        } else if (reader.isNull()) {
            PointableHelper.setNull(result);
        } else {
            result.set(getter.getValue(reader));
        }
    }
}
