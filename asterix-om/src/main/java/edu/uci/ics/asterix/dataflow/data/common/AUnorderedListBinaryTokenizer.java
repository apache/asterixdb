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
package edu.uci.ics.asterix.dataflow.data.common;

import edu.uci.ics.asterix.common.exceptions.AsterixException;
import edu.uci.ics.asterix.dataflow.data.nontagged.serde.AUnorderedListSerializerDeserializer;
import edu.uci.ics.hyracks.storage.am.lsm.invertedindex.tokenizers.ITokenFactory;

public class AUnorderedListBinaryTokenizer extends AOrderedListBinaryTokenizer {

    public AUnorderedListBinaryTokenizer(ITokenFactory tokenFactory) {
        super(tokenFactory);
    }

    @Override
    protected int getItemOffset(byte[] data, int start, int itemIndex) throws AsterixException {
        return AUnorderedListSerializerDeserializer.getItemOffset(data, start, itemIndex);
    }

    @Override
    protected int getNumberOfItems(byte[] data, int start) {
        return AUnorderedListSerializerDeserializer.getNumberOfItems(data, start);
    }
}
