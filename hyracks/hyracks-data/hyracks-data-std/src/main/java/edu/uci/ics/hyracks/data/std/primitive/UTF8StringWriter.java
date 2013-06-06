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
package edu.uci.ics.hyracks.data.std.primitive;

import java.io.DataOutput;
import java.io.IOException;
import java.io.UTFDataFormatException;

public class UTF8StringWriter {
    private byte[] tempBytes;

    public void writeUTF8String(CharSequence str, DataOutput out) throws IOException {
        int strlen = str.length();
        int utflen = 0;
        int c, count = 0;

        for (int i = 0; i < strlen; i++) {
            c = str.charAt(i);
            if ((c >= 0x0001) && (c <= 0x007F)) {
                utflen++;
            } else if (c > 0x07FF) {
                utflen += 3;
            } else {
                utflen += 2;
            }
        }

        if (utflen > 65535) {
            throw new UTFDataFormatException("encoded string too long: " + utflen + " bytes");
        }

        if (tempBytes == null || tempBytes.length < utflen + 2) {
            tempBytes = new byte[utflen + 2];
        }

        tempBytes[count++] = (byte) ((utflen >>> 8) & 0xFF);
        tempBytes[count++] = (byte) ((utflen >>> 0) & 0xFF);

        int i = 0;
        for (i = 0; i < strlen; i++) {
            c = str.charAt(i);
            if (!((c >= 0x0001) && (c <= 0x007F))) {
                break;
            }
            tempBytes[count++] = (byte) c;
        }

        for (; i < strlen; i++) {
            c = str.charAt(i);
            if ((c >= 0x0001) && (c <= 0x007F)) {
                tempBytes[count++] = (byte) c;
            } else if (c > 0x07FF) {
                tempBytes[count++] = (byte) (0xE0 | ((c >> 12) & 0x0F));
                tempBytes[count++] = (byte) (0x80 | ((c >> 6) & 0x3F));
                tempBytes[count++] = (byte) (0x80 | ((c >> 0) & 0x3F));
            } else {
                tempBytes[count++] = (byte) (0xC0 | ((c >> 6) & 0x1F));
                tempBytes[count++] = (byte) (0x80 | ((c >> 0) & 0x3F));
            }
        }
        out.write(tempBytes, 0, utflen + 2);
    }
}