package edu.uci.ics.asterix.runtime.evaluators.functions;

import edu.uci.ics.asterix.om.functions.AsterixBuiltinFunctions;
import edu.uci.ics.asterix.om.functions.IFunctionDescriptor;
import edu.uci.ics.asterix.om.functions.IFunctionDescriptorFactory;
import edu.uci.ics.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;

public class NumericAddDescriptor extends AbstractNumericArithmeticEval {

    private static final long serialVersionUID = 1L;
    public static final IFunctionDescriptorFactory FACTORY = new IFunctionDescriptorFactory() {
        public IFunctionDescriptor createFunctionDescriptor() {
            return new NumericAddDescriptor();
        }
    };

    @Override
    public FunctionIdentifier getIdentifier() {
        return AsterixBuiltinFunctions.NUMERIC_ADD;
    }

    @Override
    protected long evaluateInteger(long x, long y) throws HyracksDataException {
        long z = x + y;
        if (x > 0) {
            if (y > 0 && z < 0)
                throw new ArithmeticException("Overflow adding " + x + " + " + y);
        } else if (y < 0 && z > 0)
            throw new ArithmeticException("Overflow adding " + x + " + " + y);
        return z;
    }

    @Override
    protected double evaluateDouble(double lhs, double rhs) throws HyracksDataException {
        return lhs + rhs;
    }
}
