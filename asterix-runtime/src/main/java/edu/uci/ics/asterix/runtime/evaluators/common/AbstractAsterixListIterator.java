package edu.uci.ics.asterix.runtime.evaluators.common;

import edu.uci.ics.asterix.common.exceptions.AsterixException;
import edu.uci.ics.asterix.common.exceptions.AsterixRuntimeException;
import edu.uci.ics.asterix.formats.nontagged.AqlBinaryComparatorFactoryProvider;
import edu.uci.ics.asterix.om.types.ATypeTag;
import edu.uci.ics.asterix.om.types.EnumDeserializer;
import edu.uci.ics.fuzzyjoin.similarity.IListIterator;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparator;

public abstract class AbstractAsterixListIterator implements IListIterator {

    protected byte[] data;
    protected int count = 0;
    protected int pos = -1;
    protected int size = -1;
    protected int startOff = -1;
    protected IBinaryComparator cmp;

    // Ignore case for strings. Defaults to true.
    protected final boolean ignoreCase = true;

    @Override
    public int compare(IListIterator cmpIter) {
        return cmp.compare(data, pos, -1, cmpIter.getData(), cmpIter.getPos(), -1);
    }

    @Override
    public boolean hasNext() {
        return count < size;
    }

    @Override
    public int size() {
        return size;
    }

    @Override
    public byte[] getData() {
        return data;
    }

    @Override
    public int getPos() {
        return pos;
    }

    @Override
    public void next() {
        try {
            pos = getItemOffset(data, startOff, ++count);
        } catch (AsterixException e) {
            throw new AsterixRuntimeException(e);
        }
    }

    @Override
    public void reset() {
        count = 0;
        try {
            pos = getItemOffset(data, startOff, count);
        } catch (AsterixException e) {
            throw new AsterixRuntimeException(e);
        }
    }

    public void reset(byte[] data, int startOff) {
        this.data = data;
        this.startOff = startOff;
        size = getNumberOfItems(data, startOff);
        ATypeTag tag = EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(data[startOff + 1]);
        switch (tag) {
            case INT32: {
                cmp = AqlBinaryComparatorFactoryProvider.INTEGER_POINTABLE_INSTANCE.createBinaryComparator();
                break;
            }
            case FLOAT: {
                cmp = AqlBinaryComparatorFactoryProvider.FLOAT_POINTABLE_INSTANCE.createBinaryComparator();
                break;
            }
            case DOUBLE: {
                cmp = AqlBinaryComparatorFactoryProvider.DOUBLE_POINTABLE_INSTANCE.createBinaryComparator();
                break;
            }
            case STRING: {
                if (ignoreCase) {
                    cmp = AqlBinaryComparatorFactoryProvider.UTF8STRING_LOWERCASE_POINTABLE_INSTANCE
                            .createBinaryComparator();
                } else {
                    cmp = AqlBinaryComparatorFactoryProvider.UTF8STRING_POINTABLE_INSTANCE.createBinaryComparator();
                }
                break;
            }
            default: {
                cmp = null;
                break;
            }
        }
        reset();
    }

    protected abstract int getItemOffset(byte[] serOrderedList, int offset, int itemIndex) throws AsterixException;

    protected abstract int getNumberOfItems(byte[] serOrderedList, int offset);
}
