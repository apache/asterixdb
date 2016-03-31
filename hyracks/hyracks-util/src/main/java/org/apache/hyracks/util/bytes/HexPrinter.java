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

package org.apache.hyracks.util.bytes;

import java.io.IOException;

public class HexPrinter {
    public enum CASE {
        LOWER_CASE,
        UPPER_CASE,
    }

    public static byte hex(int i, CASE c) {
        switch (c) {
            case LOWER_CASE:
                return (byte) (i < 10 ? i + '0' : i + ('a' - 10));
            case UPPER_CASE:
                return (byte) (i < 10 ? i + '0' : i + ('A' - 10));
        }
        return Byte.parseByte(null);
    }

    public static Appendable printHexString(byte[] bytes, int start, int length, Appendable appendable)
            throws IOException {
        for (int i = 0; i < length; ++i) {
            appendable.append((char) hex((bytes[start + i] >>> 4) & 0x0f, CASE.UPPER_CASE));
            appendable.append((char) hex((bytes[start + i] & 0x0f), CASE.UPPER_CASE));
        }
        return appendable;
    }
}
