package edu.uci.ics.asterix.runtime.evaluators.functions;

import java.io.DataOutput;
import java.io.IOException;

import edu.uci.ics.asterix.builders.UnorderedListBuilder;
import edu.uci.ics.asterix.common.functions.FunctionConstants;
import edu.uci.ics.asterix.om.types.ATypeTag;
import edu.uci.ics.asterix.om.types.AUnorderedListType;
import edu.uci.ics.asterix.runtime.evaluators.base.AbstractScalarFunctionDynamicDescriptor;
import edu.uci.ics.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import edu.uci.ics.hyracks.algebricks.core.algebra.runtime.base.IEvaluator;
import edu.uci.ics.hyracks.algebricks.core.algebra.runtime.base.IEvaluatorFactory;
import edu.uci.ics.hyracks.algebricks.core.api.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.ArrayBackedValueStorage;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.IDataOutputProvider;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.IFrameTupleReference;

public class UnorderedListConstructorDescriptor extends AbstractScalarFunctionDynamicDescriptor {

    private static final long serialVersionUID = 1L;
    private final static FunctionIdentifier FID = new FunctionIdentifier(FunctionConstants.ASTERIX_NS,
            "unordered-list-constructor", FunctionIdentifier.VARARGS, true);

    private AUnorderedListType ultype;

    public void reset(AUnorderedListType unorderedListType) {
        this.ultype = unorderedListType;
    }

    @Override
    public FunctionIdentifier getIdentifier() {
        return FID;
    }

    @Override
    public IEvaluatorFactory createEvaluatorFactory(final IEvaluatorFactory[] args) {
        return new UnorderedListConstructorEvaluatorFactory(args, ultype);
    }

    private static class UnorderedListConstructorEvaluatorFactory implements IEvaluatorFactory {

        private static final long serialVersionUID = 1L;
        private IEvaluatorFactory[] args;

        private boolean selfDescList = false;
        private boolean homoList = false;
        private AUnorderedListType unorderedlistType;

        public UnorderedListConstructorEvaluatorFactory(IEvaluatorFactory[] args, AUnorderedListType type) {
            this.args = args;
            this.unorderedlistType = type;
            if (type == null || type.getItemType() == null || type.getItemType().getTypeTag() == ATypeTag.ANY)
                this.selfDescList = true;
            else
                this.homoList = true;
        }

        @Override
        public IEvaluator createEvaluator(IDataOutputProvider output) throws AlgebricksException {
            final DataOutput out = output.getDataOutput();
            final ArrayBackedValueStorage inputVal = new ArrayBackedValueStorage();
            final IEvaluator[] argEvals = new IEvaluator[args.length];
            for (int i = 0; i < args.length; i++) {
                argEvals[i] = args[i].createEvaluator(inputVal);
            }

            return new IEvaluator() {

                private UnorderedListBuilder builder = new UnorderedListBuilder();

                @Override
                public void evaluate(IFrameTupleReference tuple) throws AlgebricksException {
                    try {
                        builder.reset(unorderedlistType);
                        if (selfDescList) {
                            this.writeUntypedItems(tuple);
                        }
                        if (homoList) {
                            this.writeTypedItems(tuple);
                        }
                        builder.write(out, true);
                    } catch (IOException ioe) {
                        throw new AlgebricksException(ioe);
                    }
                }

                private void writeUntypedItems(IFrameTupleReference tuple) throws AlgebricksException {

                    try {
                        for (int i = 0; i < argEvals.length; i++) {
                            inputVal.reset();
                            argEvals[i].evaluate(tuple);
                            builder.addItem(inputVal);
                        }
                    } catch (IOException ioe) {
                        throw new AlgebricksException(ioe);
                    }
                }

                private void writeTypedItems(IFrameTupleReference tuple) throws AlgebricksException {

                    try {
                        for (int i = 0; i < argEvals.length; i++) {
                            inputVal.reset();
                            argEvals[i].evaluate(tuple);
                            builder.addItem(inputVal);
                        }
                    } catch (IOException ioe) {
                        throw new AlgebricksException(ioe);
                    }
                }

            };

        }
    }

}
