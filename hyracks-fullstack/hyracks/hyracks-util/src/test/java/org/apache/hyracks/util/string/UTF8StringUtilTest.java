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

import static org.apache.hyracks.util.string.UTF8StringSample.STRING_LEN_127;
import static org.apache.hyracks.util.string.UTF8StringSample.STRING_LEN_128;
import static org.apache.hyracks.util.string.UTF8StringSample.STRING_LEN_MEDIUM;
import static org.apache.hyracks.util.string.UTF8StringSample.STRING_UTF8_3;
import static org.apache.hyracks.util.string.UTF8StringSample.STRING_UTF8_MIX;
import static org.apache.hyracks.util.string.UTF8StringSample.STRING_UTF8_MIX_LOWERCASE;
import static org.apache.hyracks.util.string.UTF8StringUtil.charAt;
import static org.apache.hyracks.util.string.UTF8StringUtil.charSize;
import static org.apache.hyracks.util.string.UTF8StringUtil.compareTo;
import static org.apache.hyracks.util.string.UTF8StringUtil.getModifiedUTF8Len;
import static org.apache.hyracks.util.string.UTF8StringUtil.getNumBytesToStoreLength;
import static org.apache.hyracks.util.string.UTF8StringUtil.getStringLength;
import static org.apache.hyracks.util.string.UTF8StringUtil.getUTFLength;
import static org.apache.hyracks.util.string.UTF8StringUtil.hash;
import static org.apache.hyracks.util.string.UTF8StringUtil.lowerCaseCompareTo;
import static org.apache.hyracks.util.string.UTF8StringUtil.lowerCaseHash;
import static org.apache.hyracks.util.string.UTF8StringUtil.normalize;
import static org.apache.hyracks.util.string.UTF8StringUtil.rawByteCompareTo;
import static org.apache.hyracks.util.string.UTF8StringUtil.writeStringToBytes;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;

import org.junit.Test;

public class UTF8StringUtilTest {

    @Test
    public void testCharAtCharSizeGetLen() throws Exception {
        char[] utf8Mix = STRING_UTF8_MIX.toCharArray();
        byte[] buffer = writeStringToBytes(STRING_UTF8_MIX);
        int pos = getNumBytesToStoreLength(getUTFLength(buffer, 0));
        for (char c : utf8Mix) {
            assertEquals(c, charAt(buffer, pos));
            assertEquals(getModifiedUTF8Len(c), charSize(buffer, pos));
            pos += charSize(buffer, pos);
        }
    }

    @Test
    public void testGetStringLength() throws Exception {
        byte[] buffer = writeStringToBytes(STRING_UTF8_MIX);
        assertEquals(STRING_UTF8_MIX.length(), getStringLength(buffer, 0));
    }

    @Test
    public void testChinese() {
        byte[] bufferDe = writeStringToBytes("的");
        byte[] bufferLi = writeStringToBytes("离");
        int ret = compareTo(bufferDe, 0, bufferLi, 0);
        assertTrue(ret != 0);
    }

    @Test
    public void testCompareToAndNormolize() throws Exception {
        testCompare(STRING_UTF8_MIX, STRING_UTF8_MIX, OPTION.STANDARD);
        testCompare(STRING_UTF8_3, STRING_UTF8_MIX, OPTION.STANDARD);
        testCompare(STRING_LEN_MEDIUM, STRING_UTF8_MIX, OPTION.STANDARD);
    }

    public boolean isSameSign(int r1, int r2) {
        if (r1 > 0) {
            return r2 > 0;
        }
        if (r1 < 0) {
            return r2 < 0;
        }
        return r2 == 0;
    }

    enum OPTION {
        STANDARD,
        RAW_BYTE,
        LOWERCASE
    }

    public void testCompare(String str1, String str2, OPTION option) throws IOException {
        byte[] buffer1 = writeStringToBytes(str1);
        byte[] buffer2 = writeStringToBytes(str2);

        switch (option) {
            case STANDARD:
                assertEquals(str1.compareTo(str2), compareTo(buffer1, 0, buffer2, 0));
                int n1 = normalize(buffer1, 0);
                int n2 = normalize(buffer2, 0);
                assertTrue(isSameSign(str1.compareTo(str2), n1 - n2));
                break;
            case RAW_BYTE:
                assertEquals(str1.compareTo(str2), rawByteCompareTo(buffer1, 0, buffer2, 0));
                break;
            case LOWERCASE:
                assertEquals(str1.compareToIgnoreCase(str2), lowerCaseCompareTo(buffer1, 0, buffer2, 0));
                break;
        }

    }

    @Test
    public void testRawByteCompareTo() throws Exception {
        testCompare(STRING_LEN_MEDIUM, STRING_LEN_MEDIUM, OPTION.RAW_BYTE);
        testCompare(STRING_LEN_127, STRING_LEN_128, OPTION.RAW_BYTE);
    }

    @Test
    public void testLowerCaseCompareTo() throws Exception {
        testCompare(STRING_LEN_127, STRING_LEN_128, OPTION.LOWERCASE);
        testCompare(STRING_LEN_127, STRING_UTF8_MIX, OPTION.LOWERCASE);
        testCompare(STRING_UTF8_MIX, STRING_UTF8_MIX_LOWERCASE, OPTION.LOWERCASE);
        testCompare(STRING_UTF8_MIX_LOWERCASE, STRING_UTF8_MIX, OPTION.LOWERCASE);
    }

    @Test
    public void testToString() throws Exception {

        StringBuilder sb = new StringBuilder();
        byte[] buffer = writeStringToBytes(STRING_UTF8_MIX);
        assertEquals(STRING_UTF8_MIX, UTF8StringUtil.toString(sb, buffer, 0).toString());
    }

    @Test
    public void testHash() throws IOException {
        byte[] buffer = writeStringToBytes(STRING_UTF8_MIX_LOWERCASE);
        int lowerHash = hash(buffer, 0);

        buffer = writeStringToBytes(STRING_UTF8_MIX_LOWERCASE);
        int upperHash = lowerCaseHash(buffer, 0);
        assertEquals(lowerHash, upperHash);

        int familyOne = hash(buffer, 0, 7, 297);
        int familyTwo = hash(buffer, 0, 8, 297);
        assertTrue(familyOne != familyTwo);
    }

}
