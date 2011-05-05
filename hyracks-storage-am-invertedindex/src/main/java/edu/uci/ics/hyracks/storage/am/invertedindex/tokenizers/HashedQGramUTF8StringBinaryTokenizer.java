/*
 * Copyright 2009-2010 by The Regents of the University of California
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

package edu.uci.ics.hyracks.storage.am.invertedindex.tokenizers;

import java.io.DataOutput;
import java.io.IOException;

import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.IntegerSerializerDeserializer;
import edu.uci.ics.hyracks.dataflow.common.data.util.StringUtils;
import edu.uci.ics.hyracks.storage.am.invertedindex.api.IBinaryTokenizer;

public class HashedQGramUTF8StringBinaryTokenizer implements IBinaryTokenizer {

    private static final RecordDescriptor tokenSchema = new RecordDescriptor(
            new ISerializerDeserializer[] { IntegerSerializerDeserializer.INSTANCE });

    private final boolean prePost;
    private final int q;
    private byte[] data;
    private int start;
    private int length;
    private int gramNum;
    private int utflen;

    private final char PRECHAR = '#';
    private final char POSTCHAR = '$';

    private int charPos;
    private int pos;
    private int hashedGram;

    HashedQGramUTF8StringBinaryTokenizer(int q, boolean prePost) {
        this.prePost = prePost;
        this.q = q;
    }

    @Override
    public int getTokenLength() {
        // the produced token (hashed q-gram) is derived from data
        // but not contained in it
        // therefore this call does not make sense
        return -1;
    }

    @Override
    public int getTokenStartOff() {
        // the produced token (hashed q-gram) is derived from data
        // but not contained in it
        // therefore this call does not make sense
        return -1;
    }

    @Override
    public boolean hasNext() {
        if ((prePost && pos >= start + length) || (!prePost && pos >= start + length - q))
            return false;
        else
            return true;
    }

    @Override
    public void next() {
        hashedGram = 0;
        if (prePost) {
            if (gramNum < q) {
                for (int i = 0; i < q - gramNum; i++) {
                    hashedGram = 31 * hashedGram + PRECHAR;
                }

                int tmpPos = pos;
                for (int i = 0; i < gramNum; i++) {
                    hashedGram = 31 * hashedGram + StringUtils.charAt(data, tmpPos);
                    tmpPos += StringUtils.charSize(data, tmpPos);
                }
            } else {
                int stopStr = Math.min(charPos + q, utflen);
                int tmpPos = pos;
                for (int i = charPos; i < stopStr; i++) {
                    hashedGram = 31 * hashedGram + StringUtils.charAt(data, tmpPos);
                    tmpPos += StringUtils.charSize(data, tmpPos);
                }

                int stopPost = (charPos + q) - (utflen);
                for (int i = 0; i < stopPost; i++) {
                    hashedGram = 31 * hashedGram + POSTCHAR;
                }
                pos += StringUtils.charSize(data, pos);
                charPos++;
            }
            gramNum++;
        } else {
            int tmpPos = pos;
            for (int i = charPos; i < charPos + q; i++) {
                hashedGram = 31 * hashedGram + StringUtils.charAt(data, tmpPos);
                tmpPos += StringUtils.charSize(data, tmpPos);
            }
            pos += StringUtils.charSize(data, pos);
            charPos++;
        }
    }

    @Override
    public void reset(byte[] data, int start, int length) {
        this.data = data;
        this.start = start;
        this.length = length;
        this.utflen = StringUtils.getUTFLen(data, start);
        this.pos = start + 2; // UTF-8 specific
        this.gramNum = 1;
        this.charPos = 0;
    }

    @Override
    public void writeToken(DataOutput dos) throws IOException {
        dos.writeInt(hashedGram);
    }

    public char getPreChar() {
        return PRECHAR;
    }

    public char getPostChar() {
        return POSTCHAR;
    }

    @Override
    public RecordDescriptor getTokenSchema() {
        return tokenSchema;
    }

    @Override
    public int getNumTokens() {
        return 0;
    }
}