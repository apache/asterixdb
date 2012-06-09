package edu.uci.ics.asterix.runtime.evaluators.functions;

import java.io.IOException;

import edu.uci.ics.asterix.builders.IAOrderedListBuilder;
import edu.uci.ics.asterix.builders.OrderedListBuilder;
import edu.uci.ics.asterix.common.functions.FunctionConstants;
import edu.uci.ics.asterix.formats.nontagged.AqlSerializerDeserializerProvider;
import edu.uci.ics.asterix.om.base.ABoolean;
import edu.uci.ics.asterix.om.functions.IFunctionDescriptor;
import edu.uci.ics.asterix.om.functions.IFunctionDescriptorFactory;
import edu.uci.ics.asterix.om.types.AOrderedListType;
import edu.uci.ics.asterix.om.types.ATypeTag;
import edu.uci.ics.asterix.om.types.BuiltinType;
import edu.uci.ics.asterix.runtime.evaluators.base.AbstractScalarFunctionDynamicDescriptor;
import edu.uci.ics.asterix.runtime.evaluators.common.EditDistanceEvaluator;
import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import edu.uci.ics.hyracks.algebricks.runtime.base.IEvaluator;
import edu.uci.ics.hyracks.algebricks.runtime.base.IEvaluatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.ArrayBackedValueStorage;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.IDataOutputProvider;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.IFrameTupleReference;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.IntegerSerializerDeserializer;

public class EditDistanceCheckDescriptor extends AbstractScalarFunctionDynamicDescriptor {

    private static final long serialVersionUID = 1L;
    private final static FunctionIdentifier FID = new FunctionIdentifier(FunctionConstants.ASTERIX_NS,
            "edit-distance-check", 3, true);
    public static final IFunctionDescriptorFactory FACTORY = new IFunctionDescriptorFactory() {
        public IFunctionDescriptor createFunctionDescriptor() {
            return new EditDistanceCheckDescriptor();
        }
    };

    @Override
    public IEvaluatorFactory createEvaluatorFactory(final IEvaluatorFactory[] args) throws AlgebricksException {
        return new IEvaluatorFactory() {
            private static final long serialVersionUID = 1L;

            @Override
            public IEvaluator createEvaluator(IDataOutputProvider output) throws AlgebricksException {
                return new EditDistanceCheckEvaluator(args, output);
            }
        };
    }

    @Override
    public FunctionIdentifier getIdentifier() {
        return FID;
    }

    private static class EditDistanceCheckEvaluator extends EditDistanceEvaluator {

        private final IEvaluator edThreshEval;
        private int edThresh = -1;
        private IAOrderedListBuilder listBuilder;
        private ArrayBackedValueStorage inputVal;
        @SuppressWarnings("unchecked")
        private final ISerializerDeserializer<ABoolean> booleanSerde = AqlSerializerDeserializerProvider.INSTANCE
                .getSerializerDeserializer(BuiltinType.ABOOLEAN);

        public EditDistanceCheckEvaluator(IEvaluatorFactory[] args, IDataOutputProvider output)
                throws AlgebricksException {
            super(args, output);
            edThreshEval = args[2].createEvaluator(argOut);
            listBuilder = new OrderedListBuilder();
            inputVal = new ArrayBackedValueStorage();
        }

        @Override
        protected void runArgEvals(IFrameTupleReference tuple) throws AlgebricksException {
            super.runArgEvals(tuple);
            int edThreshStart = argOut.getLength();
            edThreshEval.evaluate(tuple);
            edThresh = IntegerSerializerDeserializer.getInt(argOut.getByteArray(), edThreshStart + typeIndicatorSize);
        }

        @Override
        protected int computeResult(byte[] bytes, int firstStart, int secondStart, ATypeTag argType)
                throws AlgebricksException {
            switch (argType) {

                case STRING: {
                    return ed.UTF8StringEditDistance(bytes, firstStart + typeIndicatorSize, secondStart
                            + typeIndicatorSize, edThresh);
                }

                case ORDEREDLIST: {
                    firstOrdListIter.reset(bytes, firstStart);
                    secondOrdListIter.reset(bytes, secondStart);
                    return (int) ed.getSimilarity(firstOrdListIter, secondOrdListIter, edThresh);
                }

                default: {
                    throw new AlgebricksException("Invalid type " + argType
                            + " passed as argument to edit distance function.");
                }

            }
        }

        @Override
        protected void writeResult(int ed) throws IOException {

            listBuilder.reset(new AOrderedListType(BuiltinType.ANY, "list"));
            boolean matches = (ed < 0) ? false : true;
            inputVal.reset();
            booleanSerde.serialize(matches ? ABoolean.TRUE : ABoolean.FALSE, inputVal.getDataOutput());
            listBuilder.addItem(inputVal);

            inputVal.reset();
            aInt32.setValue((matches) ? ed : Integer.MAX_VALUE);
            int32Serde.serialize(aInt32, inputVal.getDataOutput());
            listBuilder.addItem(inputVal);

            listBuilder.write(out, true);
        }
    }

}
