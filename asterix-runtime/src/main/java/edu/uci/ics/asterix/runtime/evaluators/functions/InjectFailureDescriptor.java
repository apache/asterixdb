package edu.uci.ics.asterix.runtime.evaluators.functions;

import java.io.IOException;

import edu.uci.ics.asterix.common.functions.FunctionConstants;
import edu.uci.ics.asterix.dataflow.data.nontagged.serde.ABooleanSerializerDeserializer;
import edu.uci.ics.asterix.om.types.ATypeTag;
import edu.uci.ics.asterix.om.types.EnumDeserializer;
import edu.uci.ics.asterix.runtime.evaluators.base.AbstractScalarFunctionDynamicDescriptor;
import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import edu.uci.ics.hyracks.algebricks.runtime.base.IEvaluator;
import edu.uci.ics.hyracks.algebricks.runtime.base.IEvaluatorFactory;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.ArrayBackedValueStorage;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.IDataOutputProvider;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.IFrameTupleReference;

public class InjectFailureDescriptor extends AbstractScalarFunctionDynamicDescriptor {

    private static final long serialVersionUID = 1L;
    private final static FunctionIdentifier FID = new FunctionIdentifier(FunctionConstants.ASTERIX_NS,
            "inject-failure", 2, true);

    @Override
    public FunctionIdentifier getIdentifier() {
        return FID;
    }

    @Override
    public IEvaluatorFactory createEvaluatorFactory(final IEvaluatorFactory[] args) throws AlgebricksException {

        return new IEvaluatorFactory() {
            private static final long serialVersionUID = 1L;

            @Override
            public IEvaluator createEvaluator(final IDataOutputProvider output) throws AlgebricksException {

                final ArrayBackedValueStorage argOut = new ArrayBackedValueStorage();
                final IEvaluator[] evals = new IEvaluator[args.length];
                evals[0] = args[0].createEvaluator(argOut);
                evals[1] = args[1].createEvaluator(argOut);

                return new IEvaluator() {

                    @Override
                    public void evaluate(IFrameTupleReference tuple) throws AlgebricksException {
                        try {
                            // evaluator the failure condition
                            argOut.reset();
                            evals[1].evaluate(tuple);
                            ATypeTag typeTag = EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(argOut.getBytes()[0]);
                            if (typeTag == ATypeTag.BOOLEAN) {
                                boolean argResult = ABooleanSerializerDeserializer.getBoolean(argOut.getBytes(), 1);
                                if (argResult)
                                    throw new AlgebricksException("Injecting a intended failure");
                            }

                            // evaluate the real evaluator
                            argOut.reset();
                            evals[0].evaluate(tuple);
                            output.getDataOutput().write(argOut.getBytes(), argOut.getStartIndex(), argOut.getLength());
                        } catch (IOException e) {
                            throw new AlgebricksException(e);
                        }
                    }

                };
            }
        };
    }
}
