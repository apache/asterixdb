package edu.uci.ics.asterix.runtime.evaluators.functions;

import java.io.DataOutput;

import edu.uci.ics.asterix.common.functions.FunctionConstants;
import edu.uci.ics.asterix.om.functions.IFunctionDescriptor;
import edu.uci.ics.asterix.om.functions.IFunctionDescriptorFactory;
import edu.uci.ics.asterix.om.types.ARecordType;
import edu.uci.ics.asterix.om.types.IAType;
import edu.uci.ics.asterix.runtime.accessors.ARecordAccessor;
import edu.uci.ics.asterix.runtime.accessors.base.IBinaryAccessor;
import edu.uci.ics.asterix.runtime.accessors.cast.ACastVisitor;
import edu.uci.ics.asterix.runtime.evaluators.base.AbstractScalarFunctionDynamicDescriptor;
import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.common.utils.Triple;
import edu.uci.ics.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import edu.uci.ics.hyracks.algebricks.runtime.base.IEvaluator;
import edu.uci.ics.hyracks.algebricks.runtime.base.IEvaluatorFactory;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.ArrayBackedValueStorage;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.IDataOutputProvider;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.IFrameTupleReference;

public class CastRecordDescriptor extends AbstractScalarFunctionDynamicDescriptor {

    protected static final FunctionIdentifier FID_CAST = new FunctionIdentifier(FunctionConstants.ASTERIX_NS,
            "cast-record", 1, true);
    public static final IFunctionDescriptorFactory FACTORY = new IFunctionDescriptorFactory() {
        public IFunctionDescriptor createFunctionDescriptor() {
            return new CastRecordDescriptor();
        }
    };

    private static final long serialVersionUID = 1L;
    private ARecordType reqType;
    private ARecordType inputType;

    public void reset(ARecordType reqType, ARecordType inputType) {
        this.reqType = reqType;
        this.inputType = inputType;
    }

    @Override
    public FunctionIdentifier getIdentifier() {
        return FID_CAST;
    }

    @Override
    public IEvaluatorFactory createEvaluatorFactory(final IEvaluatorFactory[] args) {
        final IEvaluatorFactory recordEvalFactory = args[0];

        return new IEvaluatorFactory() {
            private static final long serialVersionUID = 1L;

            @Override
            public IEvaluator createEvaluator(final IDataOutputProvider output) throws AlgebricksException {
                final DataOutput out = output.getDataOutput();
                final ArrayBackedValueStorage recordBuffer = new ArrayBackedValueStorage();
                final IEvaluator recEvaluator = recordEvalFactory.createEvaluator(recordBuffer);

                return new IEvaluator() {
                    final ARecordAccessor recAccessor = new ARecordAccessor(inputType);
                    final ARecordAccessor resultAccessor = new ARecordAccessor(reqType);
                    final ACastVisitor castVisitor = new ACastVisitor();
                    final Triple<IBinaryAccessor, IAType, Boolean> arg = new Triple<IBinaryAccessor, IAType, Boolean>(
                            resultAccessor, reqType, Boolean.FALSE);

                    @Override
                    public void evaluate(IFrameTupleReference tuple) throws AlgebricksException {
                        try {
                            recordBuffer.reset();
                            recEvaluator.evaluate(tuple);
                            recAccessor.reset(recordBuffer.getBytes(), recordBuffer.getStartIndex(),
                                    recordBuffer.getLength());
                            recAccessor.accept(castVisitor, arg);
                            out.write(resultAccessor.getBytes(), resultAccessor.getStartIndex(),
                                    resultAccessor.getLength());
                        } catch (Exception ioe) {
                            throw new AlgebricksException(ioe);
                        }
                    }
                };
            }
        };
    }
}
