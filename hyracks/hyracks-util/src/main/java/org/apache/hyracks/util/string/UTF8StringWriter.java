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
package org.apache.hyracks.util.string;

import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;

import org.apache.hyracks.util.encoding.VarLenIntEncoderDecoder;

public class UTF8StringWriter implements Serializable{
    private byte[] tempBytes;

    public final void writeUTF8(CharSequence str, DataOutput out) throws IOException {
        int strlen = str.length();
        int utflen = 0;
        char c;
        int count = 0;

        for (int i = 0; i < strlen; i++) {
            c = str.charAt(i);
            utflen += UTF8StringUtil.getModifiedUTF8Len(c);
        }

        ensureTempSize(utflen);

        count += VarLenIntEncoderDecoder.encode(utflen, tempBytes, count);

        int i = 0;
        for (; i < strlen; i++) {
            c = str.charAt(i);
            if (!((c >= 0x0001) && (c <= 0x007F))) {
                break;
            }
            tempBytes[count++] = (byte) c;
        }

        for (; i < strlen; i++) {
            c = str.charAt(i);
            count += writeToBytes(tempBytes, count, c);
        }
        out.write(tempBytes, 0, count);
    }

    public final void writeUTF8(char[] buffer, int start, int length, DataOutput out) throws IOException {
        int utflen = 0;
        int count = 0;
        char c;

        for (int i = 0; i < length; i++) {
            c = buffer[i + start];
            utflen += UTF8StringUtil.getModifiedUTF8Len(c);
        }

        ensureTempSize(utflen);

        count += VarLenIntEncoderDecoder.encode(utflen, tempBytes, count);

        int i = 0;
        for (; i < length; i++) {
            c = buffer[i + start];
            if (!((c >= 0x0001) && (c <= 0x007F))) {
                break;
            }
            tempBytes[count++] = (byte) c;
        }

        for (; i < length; i++) {
            c = buffer[i + start];
            count += writeToBytes(tempBytes, count, c);
        }
        out.write(tempBytes, 0, count);
    }

    private static int writeToBytes(byte[] tempBytes, int count, char c) {
        int orig = count;
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
        return count - orig;
    }

    private void ensureTempSize(int utflen) {
        if (tempBytes == null || tempBytes.length < utflen + 5) {
            tempBytes = new byte[utflen + 5];
        }

    }

}