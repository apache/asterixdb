package edu.uci.ics.asterix.runtime.evaluators.functions;

import java.io.DataOutput;
import java.io.IOException;

import edu.uci.ics.asterix.builders.OrderedListBuilder;
import edu.uci.ics.asterix.common.functions.FunctionConstants;
import edu.uci.ics.asterix.om.functions.IFunctionDescriptor;
import edu.uci.ics.asterix.om.functions.IFunctionDescriptorFactory;
import edu.uci.ics.asterix.om.types.AOrderedListType;
import edu.uci.ics.asterix.om.types.ATypeTag;
import edu.uci.ics.asterix.runtime.evaluators.base.AbstractScalarFunctionDynamicDescriptor;
import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import edu.uci.ics.hyracks.algebricks.runtime.base.ICopyEvaluator;
import edu.uci.ics.hyracks.algebricks.runtime.base.ICopyEvaluatorFactory;
import edu.uci.ics.hyracks.data.std.api.IDataOutputProvider;
import edu.uci.ics.hyracks.data.std.util.ArrayBackedValueStorage;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.IFrameTupleReference;

public class OrderedListConstructorDescriptor extends AbstractScalarFunctionDynamicDescriptor {

    private static final long serialVersionUID = 1L;
    private final static FunctionIdentifier FID = new FunctionIdentifier(FunctionConstants.ASTERIX_NS,
            "ordered-list-constructor", FunctionIdentifier.VARARGS);
    public static final IFunctionDescriptorFactory FACTORY = new IFunctionDescriptorFactory() {
        public IFunctionDescriptor createFunctionDescriptor() {
            return new OrderedListConstructorDescriptor();
        }
    };

    private AOrderedListType oltype;

    public void reset(AOrderedListType orderedListType) {
        this.oltype = orderedListType;
    }

    @Override
    public FunctionIdentifier getIdentifier() {
        return FID;
    }

    @Override
    public ICopyEvaluatorFactory createEvaluatorFactory(final ICopyEvaluatorFactory[] args) {
        return new OrderedListConstructorEvaluatorFactory(args, oltype);
    }

    private static class OrderedListConstructorEvaluatorFactory implements ICopyEvaluatorFactory {

        private static final long serialVersionUID = 1L;
        private ICopyEvaluatorFactory[] args;

        private boolean selfDescList = false;

        private AOrderedListType orderedlistType;

        public OrderedListConstructorEvaluatorFactory(ICopyEvaluatorFactory[] args, AOrderedListType type) {
            this.args = args;

            this.orderedlistType = type;
            if (type == null || type.getItemType() == null || type.getItemType().getTypeTag() == ATypeTag.ANY) {
                this.selfDescList = true;
            }
        }

        @Override
        public ICopyEvaluator createEvaluator(IDataOutputProvider output) throws AlgebricksException {
            final DataOutput out = output.getDataOutput();
            final ArrayBackedValueStorage inputVal = new ArrayBackedValueStorage();
            final ICopyEvaluator[] argEvals = new ICopyEvaluator[args.length];
            for (int i = 0; i < args.length; i++) {
                argEvals[i] = args[i].createEvaluator(inputVal);
            }

            return new ICopyEvaluator() {

                private OrderedListBuilder builder = new OrderedListBuilder();

                @Override
                public void evaluate(IFrameTupleReference tuple) throws AlgebricksException {
                    try {
                        builder.reset(orderedlistType);
                        if (selfDescList) {
                            this.writeUntypedItems(tuple);
                        } else {
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
