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

import java.io.IOException;

import edu.uci.ics.hyracks.data.std.util.GrowableArray;
import edu.uci.ics.hyracks.storage.am.lsm.invertedindex.tokenizers.IToken;

public class AListElementToken implements IToken {

    protected byte[] data;
    protected int start;
    protected int length;
    protected int tokenLength;
    protected int typeTag;
    
    @Override
    public byte[] getData() {
        return data;
    }

    @Override
    public int getLength() {
        return length;
    }

    @Override
    public int getStart() {
        return start;
    }

    @Override
    public int getTokenLength() {
        return tokenLength;
    }

    @Override
    public void reset(byte[] data, int start, int length, int tokenLength, int tokenCount) {
        this.data = data;
        this.start = start;
        this.length = length;
        this.tokenLength = tokenLength;
        // We abuse the last param, tokenCount, to pass the type tag.
        typeTag = tokenCount;
    }

    @Override
    public void serializeToken(GrowableArray out) throws IOException {
        out.getDataOutput().writeByte(typeTag);
        out.getDataOutput().write(data, start, length);
    }

    @Override
    public void serializeTokenCount(GrowableArray out) throws IOException {
        throw new UnsupportedOperationException("Token count not implemented.");
    }
}
