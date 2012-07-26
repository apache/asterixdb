package edu.uci.ics.asterix.runtime.evaluators.functions;

import edu.uci.ics.asterix.common.functions.FunctionConstants;
import edu.uci.ics.asterix.om.functions.IFunctionDescriptor;
import edu.uci.ics.asterix.om.functions.IFunctionDescriptorFactory;
import edu.uci.ics.asterix.om.types.IAType;
import edu.uci.ics.asterix.runtime.evaluators.base.AbstractScalarFunctionDynamicDescriptor;
import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import edu.uci.ics.hyracks.algebricks.runtime.base.ICopyEvaluator;
import edu.uci.ics.hyracks.algebricks.runtime.base.ICopyEvaluatorFactory;
import edu.uci.ics.hyracks.data.std.api.IDataOutputProvider;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.IFrameTupleReference;

public class EmbedTypeDescriptor extends AbstractScalarFunctionDynamicDescriptor {

    private static final long serialVersionUID = 1L;
    private final static FunctionIdentifier FID = new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "embed-type", 1);
    public static final IFunctionDescriptorFactory FACTORY = new IFunctionDescriptorFactory() {
        public IFunctionDescriptor createFunctionDescriptor() {
            return new EmbedTypeDescriptor();
        }
    };

    @SuppressWarnings("unused")
    private IAType fieldType;

    public void reset(IAType fieldType) {
        this.fieldType = fieldType;
    }

    @Override
    public ICopyEvaluatorFactory createEvaluatorFactory(final ICopyEvaluatorFactory[] args) {
        return new ICopyEvaluatorFactory() {

            private static final long serialVersionUID = 1L;

            @Override
            public ICopyEvaluator createEvaluator(IDataOutputProvider output) throws AlgebricksException {
                // TODO Auto-generated method stub
                return new ICopyEvaluator() {

                    @Override
                    public void evaluate(IFrameTupleReference tuple) throws AlgebricksException {
                        // TODO Auto-generated method stub

                    }
                };
            }
        };
    }

    @Override
    public FunctionIdentifier getIdentifier() {
        return FID;
    }

}
