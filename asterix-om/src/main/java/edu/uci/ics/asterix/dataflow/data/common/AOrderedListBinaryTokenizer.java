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
import edu.uci.ics.asterix.dataflow.data.nontagged.serde.AOrderedListSerializerDeserializer;
import edu.uci.ics.asterix.om.types.ATypeTag;
import edu.uci.ics.asterix.om.types.EnumDeserializer;
import edu.uci.ics.asterix.om.util.NonTaggedFormatUtil;
import edu.uci.ics.hyracks.storage.am.lsm.invertedindex.tokenizers.IBinaryTokenizer;
import edu.uci.ics.hyracks.storage.am.lsm.invertedindex.tokenizers.IToken;
import edu.uci.ics.hyracks.storage.am.lsm.invertedindex.tokenizers.ITokenFactory;

public class AOrderedListBinaryTokenizer implements IBinaryTokenizer {

    protected byte[] data;
    protected int start;
    protected int length;
    protected int listLength;
    protected int itemIndex;

    protected final IToken token;

    public AOrderedListBinaryTokenizer(ITokenFactory tokenFactory) {
        token = tokenFactory.createToken();
    }

    @Override
    public IToken getToken() {
        return token;
    }

    @Override
    public boolean hasNext() {
        return itemIndex < listLength;
    }

    @Override
    public void next() {
        int itemOffset = -1;
        int length = -1;
        try {
            itemOffset = getItemOffset(data, start, itemIndex);
            // Assuming homogeneous list.
            ATypeTag typeTag = EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(data[start + 1]);
            length = NonTaggedFormatUtil.getFieldValueLength(data, itemOffset, typeTag, false);
            // Last param is a hack to pass the type tag.
            token.reset(data, itemOffset, length, length, data[start + 1]);
        } catch (AsterixException e) {
            throw new IllegalStateException(e);
        }
        itemIndex++;
    }

    @Override
    public void reset(byte[] data, int start, int length) {
        this.data = data;
        this.start = start;
        this.length = length;
        this.listLength = getNumberOfItems(data, start);
        this.itemIndex = 0;
    }

    protected int getItemOffset(byte[] data, int start, int itemIndex) throws AsterixException {
        return AOrderedListSerializerDeserializer.getItemOffset(data, start, itemIndex);
    }

    protected int getNumberOfItems(byte[] data, int start) {
        return AOrderedListSerializerDeserializer.getNumberOfItems(data, start);
    }
}
