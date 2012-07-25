package edu.uci.ics.asterix.runtime.evaluators.common;

import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;

import edu.uci.ics.asterix.formats.nontagged.AqlBinaryComparatorFactoryProvider;
import edu.uci.ics.asterix.formats.nontagged.AqlBinaryHashFunctionFactoryProvider;
import edu.uci.ics.asterix.formats.nontagged.AqlSerializerDeserializerProvider;
import edu.uci.ics.asterix.om.base.AFloat;
import edu.uci.ics.asterix.om.base.AMutableFloat;
import edu.uci.ics.asterix.om.types.ATypeTag;
import edu.uci.ics.asterix.om.types.BuiltinType;
import edu.uci.ics.asterix.om.types.EnumDeserializer;
import edu.uci.ics.asterix.runtime.evaluators.functions.BinaryHashMap;
import edu.uci.ics.asterix.runtime.evaluators.functions.BinaryHashMap.BinaryEntry;
import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.runtime.base.ICopyEvaluator;
import edu.uci.ics.hyracks.algebricks.runtime.base.ICopyEvaluatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparator;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryHashFunction;
import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.data.std.api.IDataOutputProvider;
import edu.uci.ics.hyracks.data.std.primitive.IntegerPointable;
import edu.uci.ics.hyracks.data.std.primitive.UTF8StringPointable;
import edu.uci.ics.hyracks.data.std.util.ArrayBackedValueStorage;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.IFrameTupleReference;

public class SimilarityJaccardEvaluator implements ICopyEvaluator {

    // Parameters for hash table.
    protected final int TABLE_SIZE = 100;
    protected final int TABLE_FRAME_SIZE = 32768;

    // Assuming type indicator in serde format.
    protected final int TYPE_INDICATOR_SIZE = 1;

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

    protected final AMutableFloat aFloat = new AMutableFloat(0);
    @SuppressWarnings("unchecked")
    protected final ISerializerDeserializer<AFloat> floatSerde = AqlSerializerDeserializerProvider.INSTANCE
            .getSerializerDeserializer(BuiltinType.AFLOAT);

    protected ATypeTag firstTypeTag;
    protected ATypeTag secondTypeTag;
    protected int firstStart = -1;
    protected int secondStart = -1;
    protected float jaccSim = 0.0f;
    protected ATypeTag itemTypeTag;

    protected BinaryHashMap hashMap;
    protected BinaryEntry keyEntry = new BinaryEntry();
    protected BinaryEntry valEntry = new BinaryEntry();

    // Ignore case for strings. Defaults to true.
    protected final boolean ignoreCase = true;

    public SimilarityJaccardEvaluator(ICopyEvaluatorFactory[] args, IDataOutputProvider output)
            throws AlgebricksException {
        out = output.getDataOutput();
        firstOrdListEval = args[0].createEvaluator(argOut);
        secondOrdListEval = args[1].createEvaluator(argOut);
        byte[] emptyValBuf = new byte[8];
        Arrays.fill(emptyValBuf, (byte) 0);
        valEntry.set(emptyValBuf, 0, 8);
    }

    @Override
    public void evaluate(IFrameTupleReference tuple) throws AlgebricksException {
        runArgEvals(tuple);
        if (!checkArgTypes(firstTypeTag, secondTypeTag)) {
            return;
        }
        if (prepareLists(argOut.getByteArray(), firstStart, secondStart, firstTypeTag)) {
            jaccSim = computeResult(argOut.getByteArray(), firstStart, secondStart, firstTypeTag);
        } else {
            jaccSim = 0.0f;
        }
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

    protected boolean prepareLists(byte[] bytes, int firstStart, int secondStart, ATypeTag argType)
            throws AlgebricksException {
        firstListIter.reset(bytes, firstStart);
        secondListIter.reset(bytes, secondStart);
        // Check for special case where one of the lists is empty, since list
        // types won't match.
        if (firstListIter.size() == 0 || secondListIter.size() == 0) {
            return false;
        }
        if (firstTypeTag == ATypeTag.ANY || secondTypeTag == ATypeTag.ANY) {
            throw new AlgebricksException("\n Jaccard can only be called on homogenous lists");
        }
        // TODO: Check item types are compatible.
        itemTypeTag = EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(bytes[firstStart + 1]);
        return true;
    }

    protected float computeResult(byte[] bytes, int firstStart, int secondStart, ATypeTag argType)
            throws AlgebricksException {
        setHashMap(bytes, firstStart, secondStart);
        // We will subtract the intersection size later to get the real union size.
        int firstListSize = firstListIter.size();
        int secondListSize = secondListIter.size();
        int unionSize = firstListSize + secondListSize;
        // Choose smaller list as build, and larger one as probe.
        AbstractAsterixListIterator buildList = (firstListSize < secondListSize) ? firstListIter : secondListIter;
        AbstractAsterixListIterator probeList = (buildList == firstListIter) ? secondListIter : firstListIter;
        int buildListSize = (buildList == firstListIter) ? firstListSize : secondListSize;
        int probeListSize = (probeList == firstListIter) ? firstListSize : secondListSize;

        buildHashMap(buildList);
        int intersectionSize = probeHashMap(probeList, buildListSize, probeListSize);
        // Special indicator for the "check" version of jaccard.
        if (intersectionSize < 0) {
            return -1;
        }
        unionSize -= intersectionSize;
        return (float) intersectionSize / (float) unionSize;
    }

    protected void buildHashMap(AbstractAsterixListIterator buildIter) {
        // Build phase: Add items into hash map, starting with first list.
        // Value in map is a pair of integers. Set first integer to 1.
        IntegerPointable.setInteger(valEntry.buf, 0, 1);
        while (buildIter.hasNext()) {
            byte[] buf = buildIter.getData();
            int off = buildIter.getPos();
            int len = getItemLen(buf, off);
            keyEntry.set(buf, off, len);
            BinaryEntry entry = hashMap.put(keyEntry, valEntry);
            if (entry != null) {
                // Increment value.
                int firstValInt = IntegerPointable.getInteger(entry.buf, entry.off);
                IntegerPointable.setInteger(entry.buf, entry.off, firstValInt + 1);
            }
            buildIter.next();
        }
    }

    protected int probeHashMap(AbstractAsterixListIterator probeIter, int probeListSize, int buildListSize) {
        // Probe phase: Probe items from second list, and compute intersection size.
        int intersectionSize = 0;
        while (probeIter.hasNext()) {
            byte[] buf = probeIter.getData();
            int off = probeIter.getPos();
            int len = getItemLen(buf, off);
            keyEntry.set(buf, off, len);
            BinaryEntry entry = hashMap.get(keyEntry);
            if (entry != null) {
                // Increment second value.
                int firstValInt = IntegerPointable.getInteger(entry.buf, entry.off);
                // Irrelevant for the intersection size.
                if (firstValInt == 0) {
                    continue;
                }
                int secondValInt = IntegerPointable.getInteger(entry.buf, entry.off + 4);
                // Subtract old min value.
                intersectionSize -= (firstValInt < secondValInt) ? firstValInt : secondValInt;
                secondValInt++;
                // Add new min value.
                intersectionSize += (firstValInt < secondValInt) ? firstValInt : secondValInt;
                IntegerPointable.setInteger(entry.buf, entry.off + 4, secondValInt);
            }
            probeIter.next();
        }
        return intersectionSize;
    }

    protected void setHashMap(byte[] bytes, int firstStart, int secondStart) {
        if (hashMap != null) {
            hashMap.clear();
            return;
        }
        IBinaryHashFunction hashFunc = null;
        IBinaryComparator cmp = null;
        switch (itemTypeTag) {
            case INT32: {
                hashFunc = AqlBinaryHashFunctionFactoryProvider.INTEGER_POINTABLE_INSTANCE.createBinaryHashFunction();
                cmp = AqlBinaryComparatorFactoryProvider.INTEGER_POINTABLE_INSTANCE.createBinaryComparator();
                break;
            }
            case FLOAT: {
                hashFunc = AqlBinaryHashFunctionFactoryProvider.FLOAT_POINTABLE_INSTANCE.createBinaryHashFunction();
                cmp = AqlBinaryComparatorFactoryProvider.FLOAT_POINTABLE_INSTANCE.createBinaryComparator();
                break;
            }
            case DOUBLE: {
                hashFunc = AqlBinaryHashFunctionFactoryProvider.DOUBLE_POINTABLE_INSTANCE.createBinaryHashFunction();
                cmp = AqlBinaryComparatorFactoryProvider.DOUBLE_POINTABLE_INSTANCE.createBinaryComparator();
                break;
            }
            case STRING: {
                if (ignoreCase) {
                    // Ignore case in comparisons and hashing.
                    hashFunc = AqlBinaryHashFunctionFactoryProvider.UTF8STRING_LOWERCASE_POINTABLE_INSTANCE
                            .createBinaryHashFunction();
                    cmp = AqlBinaryComparatorFactoryProvider.UTF8STRING_LOWERCASE_POINTABLE_INSTANCE
                            .createBinaryComparator();
                } else {
                    hashFunc = AqlBinaryHashFunctionFactoryProvider.UTF8STRING_POINTABLE_INSTANCE
                            .createBinaryHashFunction();
                    cmp = AqlBinaryComparatorFactoryProvider.UTF8STRING_POINTABLE_INSTANCE.createBinaryComparator();
                }
                break;
            }
            default: {
                break;
            }
        }
        hashMap = new BinaryHashMap(TABLE_SIZE, TABLE_FRAME_SIZE, hashFunc, cmp);
    }

    protected int getItemLen(byte[] bytes, int itemOff) {
        switch (itemTypeTag) {
            case INT32: {
                return 4;
            }
            case FLOAT: {
                return 4;
            }
            case DOUBLE: {
                return 8;
            }
            case STRING: {
                // 2 bytes for the UTF8 len, plus the string data.
                return 2 + UTF8StringPointable.getUTFLength(bytes, itemOff);
            }
            default: {
                return -1;
            }
        }
    }

    protected boolean checkArgTypes(ATypeTag typeTag1, ATypeTag typeTag2) throws AlgebricksException {
        // Jaccard between null and anything else is 0
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