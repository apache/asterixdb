/**
 * Copyright 2010-2011 The Regents of the University of California
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on
 * an "AS IS"; BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under
 * the License.
 *
 * Author: Alexander Behm <abehm (at) ics.uci.edu>
 */

package edu.uci.ics.asterix.fuzzyjoin.tokenizer;

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

    public static char toLowerCase(char c) {
        switch (c) {
            case 'A':
                return 'a';
            case 'B':
                return 'b';
            case 'C':
                return 'c';
            case 'D':
                return 'd';
            case 'E':
                return 'e';
            case 'F':
                return 'f';
            case 'G':
                return 'g';
            case 'H':
                return 'h';
            case 'I':
                return 'i';
            case 'J':
                return 'j';
            case 'K':
                return 'k';
            case 'L':
                return 'l';
            case 'M':
                return 'm';
            case 'N':
                return 'n';
            case 'O':
                return 'o';
            case 'P':
                return 'p';
            case 'Q':
                return 'q';
            case 'R':
                return 'r';
            case 'S':
                return 's';
            case 'T':
                return 't';
            case 'U':
                return 'u';
            case 'V':
                return 'v';
            case 'W':
                return 'w';
            case 'X':
                return 'x';
            case 'Y':
                return 'y';
            case 'Z':
                return 'z';
            case 'Ä':
                return 'ä';
            case 'Ǟ':
                return 'ǟ';
            case 'Ë':
                return 'ë';
            case 'Ḧ':
                return 'ḧ';
            case 'Ï':
                return 'ï';
            case 'Ḯ':
                return 'ḯ';
            case 'Ö':
                return 'ö';
            case 'Ȫ':
                return 'ȫ';
            case 'Ṏ':
                return 'ṏ';
            case 'Ü':
                return 'ü';
            case 'Ǖ':
                return 'ǖ';
            case 'Ǘ':
                return 'ǘ';
            case 'Ǚ':
                return 'ǚ';
            case 'Ǜ':
                return 'ǜ';
            case 'Ṳ':
                return 'ṳ';
            case 'Ṻ':
                return 'ṻ';
            case 'Ẅ':
                return 'ẅ';
            case 'Ẍ':
                return 'ẍ';
            case 'Ÿ':
                return 'ÿ';
            default:
                // since I probably missed some chars above
                // use Java to convert to lower case to be safe
                return Character.toLowerCase(c);
        }
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