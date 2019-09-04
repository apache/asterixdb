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

package org.apache.asterix.om.base;

import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.common.exceptions.RuntimeDataException;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.util.string.UTF8StringUtil;

public class AMutableUUID extends AUUID {

    private final byte[] hexBytesBuffer = new byte[UUID_CHARS];

    public void parseUUIDString(char[] buffer, int begin, int len) throws HyracksDataException {
        if (len != UUID_CHARS) {
            throw new HyracksDataException("This is not a correct UUID value: " + new String(buffer, begin, len));
        }
        for (int i = 0; i < len; i++) {
            hexBytesBuffer[i] = (byte) buffer[begin + i];
        }
        parseUUIDHexBytes(hexBytesBuffer, 0);
    }

    public void parseUUIDHexBytes(byte[] serString, int offset) throws HyracksDataException {
        // First part - 8 bytes
        decodeBytesFromHex(serString, offset, uuidBytes, 0, 8);
        offset += 8;

        // Skip the hyphen part
        offset += 1;

        // Second part - 4 bytes
        decodeBytesFromHex(serString, offset, uuidBytes, 4, 4);
        offset += 4;

        // Skip the hyphen part
        offset += 1;

        // Third part - 4 bytes
        decodeBytesFromHex(serString, offset, uuidBytes, 6, 4);
        offset += 4;

        // Skip the hyphen part
        offset += 1;

        // Fourth part - 4 bytes
        decodeBytesFromHex(serString, offset, uuidBytes, 8, 4);
        offset += 4;

        // Skip the hyphen part
        offset += 1;

        // The last part - 12 bytes
        decodeBytesFromHex(serString, offset, uuidBytes, 10, 12);
    }

    // Calculate a long value from a hex string.
    private static void decodeBytesFromHex(byte[] hexArray, int hexArrayOffset, byte[] outputArray, int outputOffset,
            int length) throws HyracksDataException {
        try {
            for (int i = hexArrayOffset; i < hexArrayOffset + length;) {
                int hi = transformHexCharToInt(hexArray[i++]);
                outputArray[outputOffset++] = (byte) (hi << 4 | transformHexCharToInt(hexArray[i++]));
            }
        } catch (Exception ex) {
            // Can also happen in case of invalid length, out of bound exception
            throw new RuntimeDataException(ErrorCode.INVALID_FORMAT, "uuid", UTF8StringUtil.toString(hexArray, 1));
        }
    }

    // Interpret a character to the corresponding integer value.
    private static int transformHexCharToInt(byte val) throws HyracksDataException {
        switch (val) {
            case '0':
                return 0;
            case '1':
                return 1;
            case '2':
                return 2;
            case '3':
                return 3;
            case '4':
                return 4;
            case '5':
                return 5;
            case '6':
                return 6;
            case '7':
                return 7;
            case '8':
                return 8;
            case '9':
                return 9;
            case 'a':
            case 'A':
                return 10;
            case 'b':
            case 'B':
                return 11;
            case 'c':
            case 'C':
                return 12;
            case 'd':
            case 'D':
                return 13;
            case 'e':
            case 'E':
                return 14;
            case 'f':
            case 'F':
                return 15;
            default:
                throw new RuntimeDataException(ErrorCode.INVALID_FORMAT);
        }
    }
}