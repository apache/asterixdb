package edu.uci.ics.asterix.runtime.evaluators.functions;

import java.io.IOException;

import edu.uci.ics.asterix.common.functions.FunctionConstants;
import edu.uci.ics.asterix.runtime.evaluators.base.AbstractScalarFunctionDynamicDescriptor;
import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import edu.uci.ics.hyracks.algebricks.runtime.base.IEvaluator;
import edu.uci.ics.hyracks.algebricks.runtime.base.IEvaluatorFactory;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.ArrayBackedValueStorage;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.IDataOutputProvider;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.IFrameTupleReference;

public class SwitchCaseDescriptor extends AbstractScalarFunctionDynamicDescriptor {

    private static final long serialVersionUID = 1L;
    public final static FunctionIdentifier FID = new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "switch-case",
            FunctionIdentifier.VARARGS, true);

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
                final ArrayBackedValueStorage condOut = new ArrayBackedValueStorage();
                final ArrayBackedValueStorage caseOut = new ArrayBackedValueStorage();
                final ArrayBackedValueStorage argOut = new ArrayBackedValueStorage();

                final IEvaluator[] evals = new IEvaluator[args.length];
                // condition
                evals[0] = args[0].createEvaluator(condOut);
                // case value
                for (int i = 1; i < evals.length - 1; i += 2) {
                    evals[i] = args[i].createEvaluator(caseOut);
                }
                // case expression
                for (int i = 2; i < evals.length - 1; i += 2) {
                    evals[i] = args[i].createEvaluator(argOut);
                }
                // default expression
                evals[evals.length - 1] = args[evals.length - 1].createEvaluator(argOut);

                return new IEvaluator() {

                    @Override
                    public void evaluate(IFrameTupleReference tuple) throws AlgebricksException {
                        try {
                            int n = args.length;
                            condOut.reset();
                            evals[0].evaluate(tuple);
                            for (int i = 1; i < n; i += 2) {
                                caseOut.reset();
                                evals[i].evaluate(tuple);
                                if (equals(condOut, caseOut)) {
                                    argOut.reset();
                                    evals[i + 1].evaluate(tuple);
                                    output.getDataOutput().write(argOut.getBytes(), argOut.getStartIndex(),
                                            argOut.getLength());
                                    return;
                                }
                            }
                            // the default case
                            argOut.reset();
                            evals[n - 1].evaluate(tuple);
                            output.getDataOutput().write(argOut.getBytes(), argOut.getStartIndex(), argOut.getLength());
                        } catch (HyracksDataException hde) {
                            throw new AlgebricksException(hde);
                        } catch (IOException ioe) {
                            throw new AlgebricksException(ioe);
                        }
                    }

                    private boolean equals(ArrayBackedValueStorage out1, ArrayBackedValueStorage out2) {
                        if (out1.getStartIndex() != out2.getStartIndex() || out1.getLength() != out2.getLength())
                            return false;
                        byte[] data1 = out1.getBytes();
                        byte[] data2 = out2.getBytes();
                        for (int i = out1.getStartIndex(); i < out1.getLength(); i++) {
                            if (data1[i] != data2[i]) {
                                return false;
                            }
                        }
                        return true;
                    }
                };
            }
        };
    }

}
