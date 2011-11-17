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
package edu.uci.ics.hyracks.dataflow.common.data.util;

import java.io.DataOutput;
import java.io.IOException;

public class StringUtils {
    public static char charAt(byte[] b, int s) {
        int c = b[s] & 0xff;
        switch (c >> 4) {
            case 0:
            case 1:
            case 2:
            case 3:
            case 4:
            case 5:
            case 6:
            case 7:
                return (char) c;

            case 12:
            case 13:
                return (char) (((c & 0x1F) << 6) | ((b[s + 1]) & 0x3F));

            case 14:
                return (char) (((c & 0x0F) << 12) | (((b[s + 1]) & 0x3F) << 6) | (((b[s + 2]) & 0x3F) << 0));

            default:
                throw new IllegalArgumentException();
        }
    }

    public static int charSize(byte[] b, int s) {
        int c = b[s] & 0xff;
        switch (c >> 4) {
            case 0:
            case 1:
            case 2:
            case 3:
            case 4:
            case 5:
            case 6:
            case 7:
                return 1;

            case 12:
            case 13:
                return 2;

            case 14:
                return 3;
        }
        throw new IllegalStateException();
    }

    public static int getModifiedUTF8Len(char c) {
        if (c >= 0x0000 && c <= 0x007F) {
            return 1;
        } else if (c <= 0x07FF) {
            return 2;
        } else {
            return 3;
        }
    }

    public static int getStrLen(byte[] b, int s) {
        int pos = s + 2;
        int end = pos + getUTFLen(b, s);
        int charCount = 0;
        while (pos < end) {
            charCount++;
            pos += charSize(b, pos);
        }
        return charCount;
    }

    public static int getUTFLen(byte[] b, int s) {
        return ((b[s] & 0xff) << 8) + ((b[s + 1] & 0xff) << 0);
    }

    public static void writeCharAsModifiedUTF8(char c, DataOutput dos) throws IOException {

        if (c >= 0x0000 && c <= 0x007F) {
            dos.writeByte(c);
        } else if (c <= 0x07FF) {
            dos.writeByte((byte) (0xC0 | ((c >> 6) & 0x3F)));
            dos.writeByte((byte) (0x80 | (c & 0x3F)));
        } else {
            dos.writeByte((byte) (0xE0 | ((c >> 12) & 0x0F)));
            dos.writeByte((byte) (0x80 | ((c >> 6) & 0x3F)));
            dos.writeByte((byte) (0x80 | (c & 0x3F)));
        }
    }

    public static void writeUTF8Len(int len, DataOutput dos) throws IOException {
        dos.write((len >>> 8) & 0xFF);
        dos.write((len >>> 0) & 0xFF);
    }
}