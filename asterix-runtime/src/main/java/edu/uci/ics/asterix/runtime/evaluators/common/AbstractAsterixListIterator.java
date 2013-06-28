/*
 * Copyright 2009-2013 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
    protected int nextPos = -1;
    protected int itemLen = -1;
    protected int numberOfItems = -1;
    protected int listLength = -1;
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
        return count < numberOfItems;
    }

    @Override
    public int size() {
        return numberOfItems;
    }

    @Override
    public byte[] getData() {
        return data;
    }

    @Override
    public int getPos() {
        return pos;
    }

    public int getItemLen() {
        return itemLen;
    }

    @Override
    public void next() {
        try {
            pos = nextPos;
            ++count;
            nextPos = startOff + listLength;
            if (count + 1 < numberOfItems) {
                nextPos = getItemOffset(data, startOff, count + 1);
            }
            itemLen = nextPos - pos;
        } catch (AsterixException e) {
            throw new AsterixRuntimeException(e);
        }
    }

    @Override
    public void reset() {
        count = 0;
        try {
            pos = getItemOffset(data, startOff, count);
            nextPos = startOff + listLength;
            if (count + 1 < numberOfItems) {
                nextPos = getItemOffset(data, startOff, count + 1);
            }
            itemLen = nextPos - pos;
        } catch (AsterixException e) {
            throw new AsterixRuntimeException(e);
        }
    }

    public void reset(byte[] data, int startOff) {
        this.data = data;
        this.startOff = startOff;
        this.numberOfItems = getNumberOfItems(data, startOff);
        this.listLength = getListLength(data, startOff);
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

    protected abstract int getListLength(byte[] serOrderedList, int offset);
}
