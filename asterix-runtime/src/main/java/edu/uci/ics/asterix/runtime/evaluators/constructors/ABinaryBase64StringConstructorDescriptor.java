package edu.uci.ics.asterix.runtime.evaluators.constructors;

import edu.uci.ics.asterix.om.functions.AsterixBuiltinFunctions;
import edu.uci.ics.asterix.om.functions.IFunctionDescriptor;
import edu.uci.ics.asterix.om.functions.IFunctionDescriptorFactory;
import edu.uci.ics.asterix.runtime.evaluators.base.AbstractScalarFunctionDynamicDescriptor;
import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import edu.uci.ics.hyracks.algebricks.runtime.base.ICopyEvaluator;
import edu.uci.ics.hyracks.algebricks.runtime.base.ICopyEvaluatorFactory;
import edu.uci.ics.hyracks.data.std.api.IDataOutputProvider;
import edu.uci.ics.hyracks.dataflow.common.data.parsers.ByteArrayBase64ParserFactory;

public class ABinaryBase64StringConstructorDescriptor extends AbstractScalarFunctionDynamicDescriptor {

    public static final IFunctionDescriptorFactory FACTORY = new IFunctionDescriptorFactory() {
        @Override public IFunctionDescriptor createFunctionDescriptor() {
            return new ABinaryBase64StringConstructorDescriptor();
        }
    };

    @Override public ICopyEvaluatorFactory createEvaluatorFactory(final ICopyEvaluatorFactory[] args)
            throws AlgebricksException {
        return new ICopyEvaluatorFactory() {
            @Override public ICopyEvaluator createEvaluator(IDataOutputProvider output) throws AlgebricksException {
                return new ABinaryHexStringConstructorDescriptor.ABinaryConstructorEvaluator(output, args[0],
                        ByteArrayBase64ParserFactory.INSTANCE);
            }
        };
    }

    @Override public FunctionIdentifier getIdentifier() {
        return AsterixBuiltinFunctions.BINARY_BASE64_CONSTRUCTOR;
    }
}
