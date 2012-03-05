package edu.uci.ics.asterix.runtime.evaluators.common;

import edu.uci.ics.asterix.common.exceptions.AsterixException;
import edu.uci.ics.asterix.dataflow.data.nontagged.serde.AOrderedListSerializerDeserializer;

public final class AsterixOrderedListIterator extends AbstractAsterixListIterator {

    @Override
    protected int getItemOffset(byte[] serOrderedList, int offset, int itemIndex) throws AsterixException {
        return AOrderedListSerializerDeserializer.getItemOffset(serOrderedList, offset, itemIndex);
    }

    @Override
    protected int getNumberOfItems(byte[] serOrderedList, int offset) {
        return AOrderedListSerializerDeserializer.getNumberOfItems(serOrderedList, offset);
    }
}
