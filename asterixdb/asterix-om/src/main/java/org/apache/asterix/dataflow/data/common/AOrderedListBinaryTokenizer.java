/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.asterix.dataflow.data.common;

import org.apache.asterix.dataflow.data.nontagged.serde.AOrderedListSerializerDeserializer;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.EnumDeserializer;
import org.apache.asterix.om.utils.NonTaggedFormatUtil;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.storage.am.lsm.invertedindex.tokenizers.IBinaryTokenizer;
import org.apache.hyracks.storage.am.lsm.invertedindex.tokenizers.IToken;
import org.apache.hyracks.storage.am.lsm.invertedindex.tokenizers.ITokenFactory;
import org.apache.hyracks.storage.am.lsm.invertedindex.tokenizers.TokenizerInfo.TokenizerType;

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
            // ? Can we handle the non-string type ?
            length = NonTaggedFormatUtil.getFieldValueLength(data, itemOffset, typeTag, false);
            // Last param is a hack to pass the type tag.
            token.reset(data, itemOffset, itemOffset + length, length, data[start + 1]);
        } catch (HyracksDataException e) {
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

    protected int getItemOffset(byte[] data, int start, int itemIndex) throws HyracksDataException {
        return AOrderedListSerializerDeserializer.getItemOffset(data, start, itemIndex);
    }

    protected int getNumberOfItems(byte[] data, int start) {
        return AOrderedListSerializerDeserializer.getNumberOfItems(data, start);
    }

    @Override
    public short getTokensCount() {
        return (short) listLength;
    }

    @Override
    public TokenizerType getTokenizerType() {
        return TokenizerType.LIST;
    }
}
