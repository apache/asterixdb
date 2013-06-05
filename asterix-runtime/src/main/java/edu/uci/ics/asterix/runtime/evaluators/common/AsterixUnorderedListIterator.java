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

    @Override
    protected int getListLength(byte[] serOrderedList, int offset) {
        return AUnorderedListSerializerDeserializer.getUnorderedListLength(serOrderedList, offset + 1);
    }
}
