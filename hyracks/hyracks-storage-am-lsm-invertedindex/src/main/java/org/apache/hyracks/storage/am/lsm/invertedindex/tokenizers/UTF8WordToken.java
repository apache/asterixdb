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

import java.io.IOException;

import org.apache.hyracks.data.std.primitive.UTF8StringPointable;
import org.apache.hyracks.data.std.util.GrowableArray;
import org.apache.hyracks.dataflow.common.data.util.StringUtils;

public class UTF8WordToken extends AbstractUTF8Token {

    public UTF8WordToken(byte tokenTypeTag, byte countTypeTag) {
        super(tokenTypeTag, countTypeTag);
    }

    @Override
    public void serializeToken(GrowableArray out) throws IOException {
        handleTokenTypeTag(out.getDataOutput());
        int tokenUTF8LenOff = out.getLength();
        int tokenUTF8Len = 0;
        // Write dummy UTF length which will be correctly set later.
        out.getDataOutput().writeShort(0);
        int pos = start;
        for (int i = 0; i < tokenLength; i++) {
            char c = Character.toLowerCase(UTF8StringPointable.charAt(data, pos));
            tokenUTF8Len += StringUtils.writeCharAsModifiedUTF8(c, out.getDataOutput());
            pos += UTF8StringPointable.charSize(data, pos);
        }
        // Set UTF length of token.
        out.getByteArray()[tokenUTF8LenOff] = (byte) ((tokenUTF8Len >>> 8) & 0xFF);
        out.getByteArray()[tokenUTF8LenOff + 1] = (byte) ((tokenUTF8Len >>> 0) & 0xFF);
    }
}
