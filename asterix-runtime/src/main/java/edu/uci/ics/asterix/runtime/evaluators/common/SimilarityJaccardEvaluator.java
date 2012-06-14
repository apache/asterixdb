package edu.uci.ics.asterix.runtime.evaluators.common;

import java.io.DataOutput;
import java.io.IOException;

import edu.uci.ics.asterix.formats.nontagged.AqlSerializerDeserializerProvider;
import edu.uci.ics.asterix.om.base.AFloat;
import edu.uci.ics.asterix.om.base.AMutableFloat;
import edu.uci.ics.asterix.om.types.ATypeTag;
import edu.uci.ics.asterix.om.types.BuiltinType;
import edu.uci.ics.asterix.om.types.EnumDeserializer;
import edu.uci.ics.fuzzyjoin.similarity.SimilarityMetricJaccard;
import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.runtime.base.ICopyEvaluator;
import edu.uci.ics.hyracks.algebricks.runtime.base.ICopyEvaluatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.ArrayBackedValueStorage;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.IDataOutputProvider;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.IFrameTupleReference;

// assumes that both arguments are sorted by the same ordering

public class SimilarityJaccardEvaluator implements ICopyEvaluator {

    // assuming type indicator in serde format
    protected final int typeIndicatorSize = 1;

    protected final DataOutput out;
    protected final ArrayBackedValueStorage argOut = new ArrayBackedValueStorage();
    protected final ICopyEvaluator firstOrdListEval;
    protected final ICopyEvaluator secondOrdListEval;

    protected final AsterixOrderedListIterator fstOrdListIter = new AsterixOrderedListIterator();
    protected final AsterixOrderedListIterator sndOrdListIter = new AsterixOrderedListIterator();
    protected final AsterixUnorderedListIterator fstUnordListIter = new AsterixUnorderedListIterator();
    protected final AsterixUnorderedListIterator sndUnordListIter = new AsterixUnorderedListIterator();

    protected AbstractAsterixListIterator firstListIter;
    protected AbstractAsterixListIterator secondListIter;

    protected final SimilarityMetricJaccard jaccard = new SimilarityMetricJaccard();
    protected final AMutableFloat aFloat = new AMutableFloat(0);
    @SuppressWarnings("unchecked")
    protected final ISerializerDeserializer<AFloat> floatSerde = AqlSerializerDeserializerProvider.INSTANCE
            .getSerializerDeserializer(BuiltinType.AFLOAT);

    protected ATypeTag itemTypeTag;

    protected ATypeTag firstTypeTag;
    protected ATypeTag secondTypeTag;
    protected int firstStart = -1;
    protected int secondStart = -1;
    protected float jaccSim = 0.0f;

    public SimilarityJaccardEvaluator(ICopyEvaluatorFactory[] args, IDataOutputProvider output) throws AlgebricksException {
        out = output.getDataOutput();
        firstOrdListEval = args[0].createEvaluator(argOut);
        secondOrdListEval = args[1].createEvaluator(argOut);
    }

    @Override
    public void evaluate(IFrameTupleReference tuple) throws AlgebricksException {
        runArgEvals(tuple);
        if (!checkArgTypes(firstTypeTag, secondTypeTag))
            return;
        jaccSim = computeResult(argOut.getByteArray(), firstStart, secondStart, firstTypeTag);
        try {
            writeResult(jaccSim);
        } catch (IOException e) {
            throw new AlgebricksException(e);
        }
    }

    protected void runArgEvals(IFrameTupleReference tuple) throws AlgebricksException {
        argOut.reset();

        firstStart = argOut.getLength();
        firstOrdListEval.evaluate(tuple);
        secondStart = argOut.getLength();
        secondOrdListEval.evaluate(tuple);

        firstTypeTag = EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(argOut.getByteArray()[firstStart]);
        secondTypeTag = EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(argOut.getByteArray()[secondStart]);
    }

    protected float computeResult(byte[] bytes, int firstStart, int secondStart, ATypeTag argType)
            throws AlgebricksException {
        firstListIter.reset(bytes, firstStart);
        secondListIter.reset(bytes, secondStart);
        // Check for special case where one of the lists is empty, since list
        // types won't match.
        if (firstListIter.size() == 0 || secondListIter.size() == 0) {
            try {
                writeResult(0.0f);
            } catch (IOException e) {
                throw new AlgebricksException(e);
            }
        }
        if (firstTypeTag == ATypeTag.ANY || secondTypeTag == ATypeTag.ANY)
            throw new AlgebricksException("\n Jaccard can only be called on homogenous lists");
        return jaccard.getSimilarity(firstListIter, secondListIter);
    }

    protected boolean checkArgTypes(ATypeTag typeTag1, ATypeTag typeTag2) throws AlgebricksException {
        // jaccard between null and anything else is 0
        if (typeTag1 == ATypeTag.NULL || typeTag2 == ATypeTag.NULL) {
            try {
                writeResult(0.0f);
            } catch (IOException e) {
                throw new AlgebricksException(e);
            }
            return false;
        }
        switch (typeTag1) {
            case ORDEREDLIST: {
                firstListIter = fstOrdListIter;
                break;
            }
            case UNORDEREDLIST: {
                firstListIter = fstUnordListIter;
                break;
            }
            default: {
                throw new AlgebricksException("Invalid types " + typeTag1 + " given as arguments to jaccard.");
            }
        }
        switch (typeTag2) {
            case ORDEREDLIST: {
                secondListIter = sndOrdListIter;
                break;
            }
            case UNORDEREDLIST: {
                secondListIter = sndUnordListIter;
                break;
            }
            default: {
                throw new AlgebricksException("Invalid types " + typeTag2 + " given as arguments to jaccard.");
            }
        }
        return true;
    }

    protected void writeResult(float jacc) throws IOException {
        aFloat.setValue(jacc);
        floatSerde.serialize(aFloat, out);
    }
}