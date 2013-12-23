package edu.uci.ics.asterix.runtime.evaluators.functions;

import java.io.DataOutput;

import edu.uci.ics.asterix.om.functions.AsterixBuiltinFunctions;
import edu.uci.ics.asterix.om.functions.IFunctionDescriptor;
import edu.uci.ics.asterix.om.functions.IFunctionDescriptorFactory;
import edu.uci.ics.asterix.om.pointables.PointableAllocator;
import edu.uci.ics.asterix.om.pointables.base.IVisitablePointable;
import edu.uci.ics.asterix.om.types.ARecordType;
import edu.uci.ics.asterix.runtime.evaluators.base.AbstractScalarFunctionDynamicDescriptor;
import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import edu.uci.ics.hyracks.algebricks.runtime.base.ICopyEvaluator;
import edu.uci.ics.hyracks.algebricks.runtime.base.ICopyEvaluatorFactory;
import edu.uci.ics.hyracks.data.std.api.IDataOutputProvider;
import edu.uci.ics.hyracks.data.std.util.ArrayBackedValueStorage;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.IFrameTupleReference;

public class FlowRecordDescriptor extends AbstractScalarFunctionDynamicDescriptor {

    public static final IFunctionDescriptorFactory FACTORY = new IFunctionDescriptorFactory() {
        public IFunctionDescriptor createFunctionDescriptor() {
            return new FlowRecordDescriptor();
        }
    };

    private static final long serialVersionUID = 1L;
    private ARecordType inputType;

    public void reset(ARecordType inputType) {
        this.inputType = inputType;
    }

    @Override
    public FunctionIdentifier getIdentifier() {
        return AsterixBuiltinFunctions.FLOW_RECORD;
    }

    @Override
    public ICopyEvaluatorFactory createEvaluatorFactory(final ICopyEvaluatorFactory[] args) {
        final ICopyEvaluatorFactory recordEvalFactory = args[0];

        return new ICopyEvaluatorFactory() {
            private static final long serialVersionUID = 1L;

            @Override
            public ICopyEvaluator createEvaluator(final IDataOutputProvider output) throws AlgebricksException {
                final DataOutput out = output.getDataOutput();
                final ArrayBackedValueStorage recordBuffer = new ArrayBackedValueStorage();
                final ICopyEvaluator recEvaluator = recordEvalFactory.createEvaluator(recordBuffer);

                return new ICopyEvaluator() {
                    // pointable allocator
                    private PointableAllocator allocator = new PointableAllocator();
                    final IVisitablePointable recAccessor = allocator.allocateRecordValue(inputType);

                    @Override
                    public void evaluate(IFrameTupleReference tuple) throws AlgebricksException {
                        try {
                            recordBuffer.reset();
                            recEvaluator.evaluate(tuple);
                            recAccessor.set(recordBuffer);
                            out.write(recAccessor.getByteArray(), recAccessor.getStartOffset(), recAccessor.getLength());
                        } catch (Exception ioe) {
                            throw new AlgebricksException(ioe);
                        }
                    }
                };
            }
        };
    }

}
