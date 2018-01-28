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
package org.apache.hyracks.storage.am.lsm.invertedindex.tokenizers;

import java.io.DataOutput;
import java.io.IOException;

import org.apache.hyracks.data.std.util.GrowableArray;
import org.apache.hyracks.data.std.util.UTF8StringBuilder;
import org.apache.hyracks.util.string.UTF8StringUtil;

public abstract class AbstractUTF8Token implements IToken {
    public static final int GOLDEN_RATIO_32 = 0x09e3779b9;

    protected byte[] data;
    protected int startOffset;
    protected int endOffset;
    protected int tokenLength;
    protected int tokenCount;
    protected final byte tokenTypeTag;
    protected final byte countTypeTag;

    public AbstractUTF8Token() {
        tokenTypeTag = -1;
        countTypeTag = -1;
    }

    public AbstractUTF8Token(byte tokenTypeTag, byte countTypeTag) {
        this.tokenTypeTag = tokenTypeTag;
        this.countTypeTag = countTypeTag;
    }

    @Override
    public byte[] getData() {
        return data;
    }

    @Override
    public int getEndOffset() {
        return endOffset;
    }

    public int getLowerCaseUTF8Len(int limit) {
        int lowerCaseUTF8Len = 0;
        int pos = startOffset;
        for (int i = 0; i < limit; i++) {
            char c = Character.toLowerCase(UTF8StringUtil.charAt(data, pos));
            lowerCaseUTF8Len += UTF8StringUtil.getModifiedUTF8Len(c);
            pos += UTF8StringUtil.charSize(data, pos);
        }
        return lowerCaseUTF8Len;
    }

    @Override
    public int getStartOffset() {
        return startOffset;
    }

    @Override
    public int getTokenLength() {
        return tokenLength;
    }

    public void handleCountTypeTag(DataOutput dos) throws IOException {
        if (countTypeTag > 0) {
            dos.write(countTypeTag);
        }
    }

    public void handleTokenTypeTag(DataOutput dos) throws IOException {
        if (tokenTypeTag > 0) {
            dos.write(tokenTypeTag);
        }
    }

    /**
     * Note: the {@code startOffset} is the offset of first character, not the string length offset
     *
     * @param data
     * @param startOffset
     * @param endOffset
     * @param tokenLength
     * @param tokenCount  the count of this token in a document , or a record, or something else.
     */
    @Override
    public void reset(byte[] data, int startOffset, int endOffset, int tokenLength, int tokenCount) {
        this.data = data;
        this.startOffset = startOffset;
        this.endOffset = endOffset;
        this.tokenLength = tokenLength;
        this.tokenCount = tokenCount;
    }

    @Override
    public void serializeTokenCount(GrowableArray out) throws IOException {
        handleCountTypeTag(out.getDataOutput());
        out.getDataOutput().writeInt(tokenCount);
    }

    // The preChar and postChar are required to be a single byte utf8 char, e.g. ASCII char.
    protected void serializeToken(UTF8StringBuilder builder, GrowableArray out, int numPreChars, int numPostChars,
            char preChar, char postChar) throws IOException {

        handleTokenTypeTag(out.getDataOutput());

        assert UTF8StringUtil.getModifiedUTF8Len(preChar) == 1 && UTF8StringUtil.getModifiedUTF8Len(postChar) == 1;
        int actualUtfLen = endOffset - startOffset;

        builder.reset(out, actualUtfLen + numPreChars + numPostChars);
        // pre chars
        for (int i = 0; i < numPreChars; i++) {
            builder.appendChar(preChar);
        }

        /// regular chars
        int numRegChars = tokenLength - numPreChars - numPostChars;
        int pos = startOffset;
        for (int i = 0; i < numRegChars; i++) {
            char c = Character.toLowerCase(UTF8StringUtil.charAt(data, pos));
            builder.appendChar(c);
            pos += UTF8StringUtil.charSize(data, pos);
        }

        // post chars
        for (int i = 0; i < numPostChars; i++) {
            builder.appendChar(postChar);
        }

        builder.finish();
    }

}
