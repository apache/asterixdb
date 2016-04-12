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

import java.io.IOException;

import org.apache.hyracks.data.std.util.GrowableArray;
import org.apache.hyracks.storage.am.lsm.invertedindex.tokenizers.IToken;

public class AListElementToken implements IToken {

    protected byte[] data;
    protected int startOffset;
    protected int endOffset;
    protected int tokenLength;
    protected int typeTag;

    @Override
    public byte[] getData() {
        return data;
    }

    @Override
    public int getEndOffset() {
        return endOffset;
    }

    @Override
    public int getStartOffset() {
        return startOffset;
    }

    @Override
    public int getTokenLength() {
        return tokenLength;
    }

    @Override
    public void reset(byte[] data, int startOffset, int endOffset, int tokenLength, int tokenCount) {
        this.data = data;
        this.startOffset = startOffset;
        this.endOffset = endOffset;
        this.tokenLength = tokenLength;
        // We abuse the last param, tokenCount, to pass the type tag.
        typeTag = tokenCount;
    }

    @Override
    public void serializeToken(GrowableArray out) throws IOException {
        out.getDataOutput().writeByte(typeTag);
        out.getDataOutput().write(data, startOffset, endOffset - startOffset);
    }

    @Override
    public void serializeTokenCount(GrowableArray out) throws IOException {
        throw new UnsupportedOperationException("Token count not implemented.");
    }
}
