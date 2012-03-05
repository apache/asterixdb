package edu.uci.ics.asterix.runtime.evaluators.functions;

import java.io.IOException;

import edu.uci.ics.asterix.builders.IAOrderedListBuilder;
import edu.uci.ics.asterix.builders.OrderedListBuilder;
import edu.uci.ics.asterix.common.functions.FunctionConstants;
import edu.uci.ics.asterix.formats.nontagged.AqlSerializerDeserializerProvider;
import edu.uci.ics.asterix.om.base.ABoolean;
import edu.uci.ics.asterix.om.base.AFloat;
import edu.uci.ics.asterix.om.base.AMutableFloat;
import edu.uci.ics.asterix.om.types.AOrderedListType;
import edu.uci.ics.asterix.om.types.BuiltinType;
import edu.uci.ics.asterix.runtime.evaluators.base.AbstractScalarFunctionDynamicDescriptor;
import edu.uci.ics.asterix.runtime.evaluators.common.SimilarityJaccardPrefixEvaluator;
import edu.uci.ics.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import edu.uci.ics.hyracks.algebricks.core.algebra.runtime.base.IEvaluator;
import edu.uci.ics.hyracks.algebricks.core.algebra.runtime.base.IEvaluatorFactory;
import edu.uci.ics.hyracks.algebricks.core.api.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.ArrayBackedValueStorage;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.IDataOutputProvider;

public class SimilarityJaccardPrefixCheckDescriptor extends AbstractScalarFunctionDynamicDescriptor {

    private static final long serialVersionUID = 1L;

    private final static FunctionIdentifier FID = new FunctionIdentifier(FunctionConstants.ASTERIX_NS,
            "similarity-jaccard-prefix-check", 6, true);

    @Override
    public IEvaluatorFactory createEvaluatorFactory(final IEvaluatorFactory[] args) throws AlgebricksException {
        return new IEvaluatorFactory() {
            private static final long serialVersionUID = 1L;

            @Override
            public IEvaluator createEvaluator(final IDataOutputProvider output) throws AlgebricksException {
                return new SimilarityJaccardPrefixCheckEvaluator(args, output);
            }
        };
    }

    @Override
    public FunctionIdentifier getIdentifier() {
        return FID;
    }

    private static class SimilarityJaccardPrefixCheckEvaluator extends SimilarityJaccardPrefixEvaluator {

        private final IAOrderedListBuilder listBuilder;
        private ArrayBackedValueStorage inputVal;
        @SuppressWarnings("unchecked")
        private final ISerializerDeserializer<ABoolean> booleanSerde = AqlSerializerDeserializerProvider.INSTANCE
                .getSerializerDeserializer(BuiltinType.ABOOLEAN);
        @SuppressWarnings("unchecked")
        private final ISerializerDeserializer<AFloat> floatSerde = AqlSerializerDeserializerProvider.INSTANCE
                .getSerializerDeserializer(BuiltinType.AFLOAT);
        private final AMutableFloat aFloat = new AMutableFloat(0);

        private final AOrderedListType listType = new AOrderedListType(BuiltinType.ANY, "list");

        public SimilarityJaccardPrefixCheckEvaluator(IEvaluatorFactory[] args, IDataOutputProvider output)
                throws AlgebricksException {
            super(args, output);
            listBuilder = new OrderedListBuilder();
            inputVal = new ArrayBackedValueStorage();
        }

        @Override
        public void writeResult() throws AlgebricksException, IOException {
            listBuilder.reset(listType);
            boolean matches = (sim <= 0) ? false : true;
            float jaccSim = (matches) ? sim : 0.0f;

            inputVal.reset();
            booleanSerde.serialize(matches ? ABoolean.TRUE : ABoolean.FALSE, inputVal.getDataOutput());
            listBuilder.addItem(inputVal);

            inputVal.reset();
            aFloat.setValue(jaccSim);
            floatSerde.serialize(aFloat, inputVal.getDataOutput());
            listBuilder.addItem(inputVal);

            listBuilder.write(out, true);

        }
    }

}
