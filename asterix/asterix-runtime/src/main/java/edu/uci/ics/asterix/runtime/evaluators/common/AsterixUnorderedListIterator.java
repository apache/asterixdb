package edu.uci.ics.asterix.runtime.evaluators.common;

import edu.uci.ics.asterix.common.exceptions.AsterixException;
import edu.uci.ics.asterix.dataflow.data.nontagged.serde.AUnorderedListSerializerDeserializer;

public final class AsterixUnorderedListIterator extends AbstractAsterixListIterator {

    @Override
    protected int getItemOffset(byte[] serOrderedList, int offset, int itemIndex) throws AsterixException {
        return AUnorderedListSerializerDeserializer.getItemOffset(serOrderedList, offset, itemIndex);
    }

    @Override
    protected int getNumberOfItems(byte[] serOrderedList, int offset) {
        return AUnorderedListSerializerDeserializer.getNumberOfItems(serOrderedList, offset);
    }
}
