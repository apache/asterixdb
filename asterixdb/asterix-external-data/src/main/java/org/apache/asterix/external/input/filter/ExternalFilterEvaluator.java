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
package org.apache.asterix.external.input.filter;

import org.apache.asterix.common.external.IExternalFilterEvaluator;
import org.apache.asterix.formats.nontagged.BinaryBooleanInspector;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.primitive.VoidPointable;

class ExternalFilterEvaluator implements IExternalFilterEvaluator {
    private final IScalarEvaluator evaluator;
    private final IExternalFilterValueEvaluator[] valueEvaluators;
    private final VoidPointable booleanResult;

    ExternalFilterEvaluator(IScalarEvaluator evaluator, IExternalFilterValueEvaluator[] valueEvaluators) {
        this.evaluator = evaluator;
        this.valueEvaluators = valueEvaluators;
        booleanResult = new VoidPointable();
    }

    @Override
    public boolean isEmpty() {
        return valueEvaluators.length == 0;
    }

    @Override
    public boolean isComputedFieldUsed(int index) {
        return valueEvaluators[index] != NoOpExternalFilterValueEvaluator.INSTANCE;
    }

    @Override
    public void setValue(int index, String stringValue) throws HyracksDataException {
        valueEvaluators[index].setValue(stringValue);
    }

    @Override
    public boolean evaluate() throws HyracksDataException {
        evaluator.evaluate(null, booleanResult);
        return BinaryBooleanInspector.getBooleanValue(booleanResult.getByteArray(), booleanResult.getStartOffset(),
                booleanResult.getLength());
    }
}
