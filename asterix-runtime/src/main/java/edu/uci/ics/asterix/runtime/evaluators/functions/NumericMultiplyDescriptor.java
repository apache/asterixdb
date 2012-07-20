package edu.uci.ics.asterix.runtime.evaluators.functions;

import edu.uci.ics.asterix.common.functions.FunctionConstants;
import edu.uci.ics.asterix.om.functions.IFunctionDescriptor;
import edu.uci.ics.asterix.om.functions.IFunctionDescriptorFactory;
import edu.uci.ics.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;

public class NumericMultiplyDescriptor extends AbstractNumericArithmeticEval {

    private static final long serialVersionUID = 1L;
    public final static FunctionIdentifier FID = new FunctionIdentifier(FunctionConstants.ASTERIX_NS,
            "numeric-multiply", 2);
    public static final IFunctionDescriptorFactory FACTORY = new IFunctionDescriptorFactory() {
        public IFunctionDescriptor createFunctionDescriptor() {
            return new NumericMultiplyDescriptor();
        }
    };

    @Override
    public FunctionIdentifier getIdentifier() {
        return FID;
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
}
