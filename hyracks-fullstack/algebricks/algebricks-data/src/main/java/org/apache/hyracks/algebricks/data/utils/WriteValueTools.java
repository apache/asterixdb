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
package org.apache.hyracks.algebricks.data.utils;

import java.io.IOException;
import java.io.OutputStream;

import org.apache.hyracks.util.string.UTF8StringUtil;

public final class WriteValueTools {

    private final static int[] INT_INTERVALS =
            { 9, 99, 999, 9999, 99999, 999999, 9999999, 99999999, 999999999, Integer.MAX_VALUE };
    private final static int[] INT_DIVIDERS =
            { 10, 100, 1000, 10000, 100000, 1000000, 10000000, 100000000, 1000000000 };
    private final static int[] DIGITS = { '0', '1', '2', '3', '4', '5', '6', '7', '8', '9' };

    public static void writeInt(int i, OutputStream os) throws IOException {
        if (i < 0) {
            if (i == Integer.MIN_VALUE) {
                os.write("-2147483648".getBytes());
                return;
            }
            os.write('-');
            i = -i;
        }
        int k = 0;
        for (; k < INT_INTERVALS.length; k++) {
            if (i <= INT_INTERVALS[k]) {
                break;
            }
        }
        while (k > 0) {
            int q = i / INT_DIVIDERS[k - 1];
            os.write(DIGITS[q % 10]);
            k--;
        }
        // now, print the units
        os.write(DIGITS[i % 10]);
    }

    public static void writeLong(long d, OutputStream os) throws IOException {
        // now, print the units
        if (d < 0) {
            if (d == Long.MIN_VALUE) {
                os.write("-9223372036854775808".getBytes());
                return;
            }
            os.write('-');
            d = -d;
        }
        long divisor = 1000000000000000000L;
        while (divisor > d) {
            divisor = divisor / 10;
        }
        while (divisor > 1) {
            os.write(DIGITS[(int) ((d / divisor) % 10)]);
            divisor = divisor / 10;
        }
        os.write(DIGITS[(int) (d % 10)]);
    }

    public static void writeUTF8StringWithQuotes(String string, OutputStream ps) throws IOException {
        UTF8StringUtil.printUTF8StringWithQuotes(string, ps);
    }

    public static void writeUTF8StringNoQuotes(String string, OutputStream ps) throws IOException {
        UTF8StringUtil.printUTF8StringNoQuotes(string, ps);
    }
}
