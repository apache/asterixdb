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
package org.apache.asterix.runtime.evaluators.functions;

import org.apache.asterix.common.annotations.MissingNullInOutFunction;
import org.apache.asterix.om.base.AMutableDouble;
import org.apache.asterix.om.base.AMutableInt64;
import org.apache.asterix.om.base.temporal.DurationArithmeticOperations;
import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.asterix.om.functions.IFunctionDescriptor;
import org.apache.asterix.om.functions.IFunctionDescriptorFactory;
import org.apache.asterix.runtime.exceptions.OverflowException;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.api.exceptions.HyracksDataException;

@MissingNullInOutFunction
public class NumericSubDescriptor extends AbstractNumericArithmeticEval {

    private static final long serialVersionUID = 1L;
    public static final IFunctionDescriptorFactory FACTORY = new IFunctionDescriptorFactory() {
        public IFunctionDescriptor createFunctionDescriptor() {
            return new NumericSubDescriptor();
        }
    };

    @Override
    public FunctionIdentifier getIdentifier() {
        return BuiltinFunctions.NUMERIC_SUBTRACT;
    }

    /* (non-Javadoc)
     * @see org.apache.asterix.runtime.evaluators.functions.AbstractNumericArithmeticEval#evaluateInteger(long, long)
     */
    @Override
    protected boolean evaluateInteger(long lhs, long rhs, AMutableInt64 result) throws HyracksDataException {
        try {
            long res = Math.subtractExact(lhs, rhs);
            result.setValue(res);
            return true;
        } catch (ArithmeticException e) {
            throw new OverflowException(sourceLoc, getIdentifier());
        }
    }

    /* (non-Javadoc)
     * @see org.apache.asterix.runtime.evaluators.functions.AbstractNumericArithmeticEval#evaluateDouble(double, double)
     */
    @Override
    protected boolean evaluateDouble(double lhs, double rhs, AMutableDouble result) throws HyracksDataException {
        double res = lhs - rhs;
        result.setValue(res);
        return true;
    }

    /* (non-Javadoc)
     * @see org.apache.asterix.runtime.evaluators.functions.AbstractNumericArithmeticEval#evaluateTimeDurationArithmetic(long, int, long, boolean)
     */
    @Override
    protected boolean evaluateTimeDurationArithmetic(long chronon, int yearMonth, long dayTime, boolean isTimeOnly,
            AMutableInt64 result) throws HyracksDataException {
        long res = DurationArithmeticOperations.addDuration(chronon, -1 * yearMonth, -1 * dayTime, isTimeOnly);
        result.setValue(res);
        return true;
    }

    /* (non-Javadoc)
     * @see org.apache.asterix.runtime.evaluators.functions.AbstractNumericArithmeticEval#evaluateTimeInstanceArithmetic(long, long)
     */
    @Override
    protected boolean evaluateTimeInstanceArithmetic(long chronon0, long chronon1, AMutableInt64 result)
            throws HyracksDataException {
        return evaluateInteger(chronon0, chronon1, result);
    }
}
