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
package edu.uci.ics.asterix.runtime.evaluators.functions;

import edu.uci.ics.asterix.om.functions.AsterixBuiltinFunctions;
import edu.uci.ics.asterix.om.functions.IFunctionDescriptor;
import edu.uci.ics.asterix.om.functions.IFunctionDescriptorFactory;
import edu.uci.ics.hyracks.algebricks.common.exceptions.NotImplementedException;
import edu.uci.ics.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;

public class NumericMultiplyDescriptor extends AbstractNumericArithmeticEval {

    private static final long serialVersionUID = 1L;
    public static final IFunctionDescriptorFactory FACTORY = new IFunctionDescriptorFactory() {
        public IFunctionDescriptor createFunctionDescriptor() {
            return new NumericMultiplyDescriptor();
        }
    };

    @Override
    public FunctionIdentifier getIdentifier() {
        return AsterixBuiltinFunctions.NUMERIC_MULTIPLY;
    }

    @Override
    protected long evaluateInteger(long lhs, long rhs) throws HyracksDataException {
        int signLhs = lhs > 0 ? 1 : (lhs < 0 ? -1 : 0);
        int signRhs = rhs > 0 ? 1 : (rhs < 0 ? -1 : 0);
        long maximum = signLhs == signRhs ? Long.MAX_VALUE : Long.MIN_VALUE;

        if (lhs != 0 && (rhs > 0 && rhs > maximum / lhs || rhs < 0 && rhs < maximum / lhs))
            throw new HyracksDataException("Overflow Happened.");

        return lhs * rhs;
    }

    @Override
    protected double evaluateDouble(double lhs, double rhs) throws HyracksDataException {
        int signLhs = lhs > 0 ? 1 : (lhs < 0 ? -1 : 0);
        int signRhs = rhs > 0 ? 1 : (rhs < 0 ? -1 : 0);
        double maximum = signLhs == signRhs ? Double.MAX_VALUE : -Double.MAX_VALUE;

        if (lhs != 0 && (rhs > 0 && rhs > maximum / lhs || rhs < 0 && rhs < maximum / lhs))
            throw new HyracksDataException("Overflow Happened.");

        return lhs * rhs;
    }

    @Override
    protected long evaluateTimeDurationArithmetic(long chronon, int yearMonth, long dayTime, boolean isTimeOnly)
            throws HyracksDataException {
        throw new NotImplementedException("Multiply operation is not defined for temporal types");
    }

    @Override
    protected long evaluateTimeInstanceArithmetic(long chronon0, long chronon1) throws HyracksDataException {
        throw new NotImplementedException("Multiply operation is not defined for temporal types");
    }
}
