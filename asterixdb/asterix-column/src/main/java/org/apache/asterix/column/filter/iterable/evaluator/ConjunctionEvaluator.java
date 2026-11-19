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

import org.apache.asterix.formats.nontagged.BinaryBooleanInspector;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.dataflow.common.data.accessors.IFrameTupleReference;

/**
 * True when every conjunct is, as the filter reads each one ({@link BinaryBooleanInspector}): a conjunct that is null
 * or missing keeps no row, as the AND it was split from would not.
 */
final class ConjunctionEvaluator implements IScalarEvaluator {
    private static final byte[] TRUE = { ATypeTag.SERIALIZED_BOOLEAN_TYPE_TAG, 1 };
    private static final byte[] FALSE = { ATypeTag.SERIALIZED_BOOLEAN_TYPE_TAG, 0 };
    private final List<IScalarEvaluator> conjuncts;

    ConjunctionEvaluator(List<IScalarEvaluator> conjuncts) {
        this.conjuncts = conjuncts;
    }

    @Override
    public void evaluate(IFrameTupleReference tuple, IPointable result) throws HyracksDataException {
        for (IScalarEvaluator conjunct : conjuncts) {
            conjunct.evaluate(tuple, result);
            if (!BinaryBooleanInspector.getBooleanValue(result.getByteArray(), result.getStartOffset(),
                    result.getLength())) {
                result.set(FALSE, 0, FALSE.length);
                return;
            }
        }
        result.set(TRUE, 0, TRUE.length);
    }
}
