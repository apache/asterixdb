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

package edu.uci.ics.hyracks.storage.am.lsm.invertedindex.tokenizers;

import java.io.IOException;

import edu.uci.ics.hyracks.data.std.primitive.UTF8StringPointable;
import edu.uci.ics.hyracks.data.std.util.GrowableArray;
import edu.uci.ics.hyracks.dataflow.common.data.util.StringUtils;

public class UTF8NGramToken extends AbstractUTF8Token implements INGramToken {

    public final static char PRECHAR = '#';

    public final static char POSTCHAR = '$';

    protected int numPreChars;
    protected int numPostChars;

    public UTF8NGramToken(byte tokenTypeTag, byte countTypeTag) {
        super(tokenTypeTag, countTypeTag);
    }

    @Override
    public int getNumPostChars() {
        return numPreChars;
    }

    @Override
    public int getNumPreChars() {
        return numPostChars;
    }

    @Override
    public void serializeToken(GrowableArray out) throws IOException {
        handleTokenTypeTag(out.getDataOutput());
        int tokenUTF8LenOff = out.getLength();

        // regular chars
        int numRegChars = tokenLength - numPreChars - numPostChars;

        // assuming pre and post char need 1-byte each in utf8
        int tokenUTF8Len = numPreChars + numPostChars;

        // Write dummy UTF length which will be correctly set later.
        out.getDataOutput().writeShort(0);

        // pre chars
        for (int i = 0; i < numPreChars; i++) {
            StringUtils.writeCharAsModifiedUTF8(PRECHAR, out.getDataOutput());
        }

        int pos = start;
        for (int i = 0; i < numRegChars; i++) {
            char c = Character.toLowerCase(UTF8StringPointable.charAt(data, pos));
            tokenUTF8Len += StringUtils.writeCharAsModifiedUTF8(c, out.getDataOutput());
            pos += UTF8StringPointable.charSize(data, pos);
        }

        // post chars
        for (int i = 0; i < numPostChars; i++) {
            StringUtils.writeCharAsModifiedUTF8(POSTCHAR, out.getDataOutput());
        }

        // Set UTF length of token.
        out.getByteArray()[tokenUTF8LenOff] = (byte) ((tokenUTF8Len >>> 8) & 0xFF);
        out.getByteArray()[tokenUTF8LenOff + 1] = (byte) ((tokenUTF8Len >>> 0) & 0xFF);
    }

    public void setNumPrePostChars(int numPreChars, int numPostChars) {
        this.numPreChars = numPreChars;
        this.numPostChars = numPostChars;
    }
}
