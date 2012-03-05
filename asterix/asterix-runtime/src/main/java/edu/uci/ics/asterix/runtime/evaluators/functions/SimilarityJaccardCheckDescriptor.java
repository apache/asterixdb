package edu.uci.ics.asterix.runtime.evaluators.functions;

import java.io.IOException;

import edu.uci.ics.asterix.builders.IAOrderedListBuilder;
import edu.uci.ics.asterix.builders.OrderedListBuilder;
import edu.uci.ics.asterix.common.functions.FunctionConstants;
import edu.uci.ics.asterix.dataflow.data.nontagged.serde.AFloatSerializerDeserializer;
import edu.uci.ics.asterix.formats.nontagged.AqlSerializerDeserializerProvider;
import edu.uci.ics.asterix.om.base.ABoolean;
import edu.uci.ics.asterix.om.types.AOrderedListType;
import edu.uci.ics.asterix.om.types.ATypeTag;
import edu.uci.ics.asterix.om.types.BuiltinType;
import edu.uci.ics.asterix.runtime.evaluators.base.AbstractScalarFunctionDynamicDescriptor;
import edu.uci.ics.asterix.runtime.evaluators.common.SimilarityJaccardEvaluator;
import edu.uci.ics.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import edu.uci.ics.hyracks.algebricks.core.algebra.runtime.base.IEvaluator;
import edu.uci.ics.hyracks.algebricks.core.algebra.runtime.base.IEvaluatorFactory;
import edu.uci.ics.hyracks.algebricks.core.api.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.ArrayBackedValueStorage;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.IDataOutputProvider;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.IFrameTupleReference;

//assumes that both arguments are sorted by the same ordering

public class SimilarityJaccardCheckDescriptor extends AbstractScalarFunctionDynamicDescriptor {

    private static final long serialVersionUID = 1L;
    private final static FunctionIdentifier FID = new FunctionIdentifier(FunctionConstants.ASTERIX_NS,
            "similarity-jaccard-check", 3, true);

    @Override
    public IEvaluatorFactory createEvaluatorFactory(final IEvaluatorFactory[] args) throws AlgebricksException {
        return new IEvaluatorFactory() {
            private static final long serialVersionUID = 1L;

            @Override
            public IEvaluator createEvaluator(IDataOutputProvider output) throws AlgebricksException {
                return new SimilarityJaccardCheckEvaluator(args, output);
            }
        };
    }

    @Override
    public FunctionIdentifier getIdentifier() {
        return FID;
    }

    private static class SimilarityJaccardCheckEvaluator extends SimilarityJaccardEvaluator {

        private final IEvaluator jaccThreshEval;
        private float jaccThresh = -1f;

        private IAOrderedListBuilder listBuilder;
        private ArrayBackedValueStorage inputVal;
        @SuppressWarnings("unchecked")
        private final ISerializerDeserializer<ABoolean> booleanSerde = AqlSerializerDeserializerProvider.INSTANCE
                .getSerializerDeserializer(BuiltinType.ABOOLEAN);
        private final AOrderedListType listType = new AOrderedListType(BuiltinType.ANY, "list");

        public SimilarityJaccardCheckEvaluator(IEvaluatorFactory[] args, IDataOutputProvider output)
                throws AlgebricksException {
            super(args, output);
            jaccThreshEval = args[2].createEvaluator(argOut);
            listBuilder = new OrderedListBuilder();
            inputVal = new ArrayBackedValueStorage();
        }

        @Override
        protected void runArgEvals(IFrameTupleReference tuple) throws AlgebricksException {
            super.runArgEvals(tuple);
            int jaccThreshStart = argOut.getLength();
            jaccThreshEval.evaluate(tuple);
            jaccThresh = (float) AFloatSerializerDeserializer.getFloat(argOut.getBytes(), jaccThreshStart
                    + typeIndicatorSize);
        }

        @Override
        protected float computeResult(byte[] bytes, int firstStart, int secondStart, ATypeTag argType)
                throws AlgebricksException {
            firstListIter.reset(bytes, firstStart);
            secondListIter.reset(bytes, secondStart);
            // Check for special case where one of the lists is empty, since
            // list types won't match.
            if (firstListIter.size() == 0 || secondListIter.size() == 0) {
                return (jaccThresh == 0.0f) ? 0.0f : -1.0f;
            }
            if (firstTypeTag == ATypeTag.ANY || secondTypeTag == ATypeTag.ANY)
                throw new AlgebricksException("\n Jaccard can only be called on homogenous lists");
            return jaccard.getSimilarity(firstListIter, secondListIter, jaccThresh);
        }

        @Override
        protected void writeResult(float jacc) throws IOException {
            listBuilder.reset(listType);
            boolean matches = (jacc < 0) ? false : true;
            inputVal.reset();
            booleanSerde.serialize(matches ? ABoolean.TRUE : ABoolean.FALSE, inputVal.getDataOutput());
            listBuilder.addItem(inputVal);

            inputVal.reset();
            aFloat.setValue((matches) ? jacc : 0.0f);
            floatSerde.serialize(aFloat, inputVal.getDataOutput());
            listBuilder.addItem(inputVal);

            listBuilder.write(out, true);
        }
    }

}
