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

package org.apache.hyracks.dataflow.std.parallel.util;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.primitive.DoublePointable;
import org.apache.hyracks.data.std.primitive.FloatPointable;
import org.apache.hyracks.data.std.primitive.IntegerPointable;
import org.apache.hyracks.data.std.primitive.LongPointable;
import org.apache.hyracks.data.std.primitive.ShortPointable;
import org.apache.hyracks.data.std.primitive.UTF8StringPointable;
import org.apache.hyracks.util.string.UTF8StringUtil;

/**
 * @author michael
 */
public class HistogramUtils {
    private static final boolean USE_SOFT = false;

    public static byte[] ansiToUTF8Byte(String ansiStr, int start) {
        return UTF8StringUtil.writeStringToBytes(ansiStr);
    }

    public static byte[] toUTF8Byte(String str, int start) {
        return UTF8StringUtil.writeStringToBytes(str);
    }

    public static byte ansiByteAt(byte[] b, int s) throws HyracksDataException {
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
                return b[s];
            case 12:
            case 13:
                throw new HyracksDataException(
                        "Binary exception: Current streaming histogram supports ansi string only.");
            case 14:
                throw new HyracksDataException(
                        "Triple exception: Current streaming histogram supports ansi string only.");
            default:
                throw new IllegalArgumentException();
        }
    }

    public static long ansiMappingToLong(UTF8StringPointable uStr, int s, int len) throws HyracksDataException {
        long lenToLong = 0;
        int cStart = uStr.getCharStartOffset();
        int nChars = uStr.getUTF8Length();
        if (len > 9)
            throw new HyracksDataException(
                    "Length exception: Current streaming histogram support nine characters at most");
        for (int i = 0; i < s + len; i++) {
            char c = 0;
            if (i < nChars)
                c = uStr.charAt(cStart);
            else
                break;
            cStart += UTF8StringUtil.getModifiedUTF8Len(c);
            //Currently, the streaming histogram support ansi string only, the exception will be thrown otherwise.
            if (i < s)
                continue;
            lenToLong |= ((long) (c - 32)) << ((len - i + s - 1) * 7);
        }
        return lenToLong;
    }

    public static UTF8StringPointable longMappingToAnsiStrict(long quantile, int len) {
        UTF8StringPointable uStr = new UTF8StringPointable();
        byte[] uByte = new byte[len + 1];
        for (int i = 0; i < len; i++) {
            byte b = (byte) ((((quantile) >> i * 7) & 0x7f) + 32);
            if (b < 0)
                b = 0;
            uByte[len - i/* + 1*/] = b;
        }
        //uByte[0] = (byte) (len << 16 >> 24);
        uByte[0] = (byte) (len & 0xff);
        uStr.set(uByte, 0, len + 1);
        return uStr;
    }

    //Continuously reverting the string and skip the illegal range of UTF8 chars.
    public static UTF8StringPointable longMappingToAnsiSoft(long quantile, int len) {
        UTF8StringPointable uStr = new UTF8StringPointable();
        byte[] uByte = new byte[len * 3 + 1];
        for (int i = 0; i < len; i++) {
            byte b = (byte) ((((quantile) >> i * 7) & 0x7f) + 32);
            if (b < 0) {
                byte[] bs = new byte[3];
                bs[0] = b;
                char c = UTF8StringUtil.charAt(bs, 0);
                int l = UTF8StringUtil.getModifiedUTF8Len(c);
                System.arraycopy(bs, 0, uByte, len - i + 1, l);
                len += (l - 1);
            } else
                uByte[len - i/* + 1*/] = b;
        }
        //uByte[0] = (byte) (len << 16 >> 24);
        uByte[0] = (byte) (len & 0xff);
        uStr.set(uByte, 0, len + 1);
        return uStr;
    }

    public static UTF8StringPointable longMappingToAnsi(long quantile, int len) {
        if (USE_SOFT)
            return longMappingToAnsiSoft(quantile, len);
        else
            return longMappingToAnsiStrict(quantile, len);
    }

    public static double ansiMappingToQuantile(IPointable uStr, int s, int len) throws HyracksDataException {
        return (double) ansiMappingToLong((UTF8StringPointable) uStr, s, len);
    }

    public static IPointable quantileRevertToAnsi(double quantile, int len) {
        return longMappingToAnsi((long) quantile, len);
    }

    public static double integerMappingToQuantile(IPointable ip) {
        return (double) ((IntegerPointable) ip).getInteger();
    }

    public static IPointable quantileRevertToInteger(double d) {
        IntegerPointable ip = (IntegerPointable) IntegerPointable.FACTORY.createPointable();
        byte[] buf = new byte[IntegerPointable.TYPE_TRAITS.getFixedLength()];
        ip.set(buf, 0, IntegerPointable.TYPE_TRAITS.getFixedLength());
        ip.setInteger((int) d);
        return ip;
    }

    public static double longMappingToQuantile(IPointable lp) {
        return (double) ((LongPointable) lp).getLong();
    }

    public static IPointable quantileRevertToLong(double d) {
        LongPointable lp = (LongPointable) LongPointable.FACTORY.createPointable();
        byte[] buf = new byte[LongPointable.TYPE_TRAITS.getFixedLength()];
        lp.set(buf, 0, LongPointable.TYPE_TRAITS.getFixedLength());
        lp.setLong((long) d);
        return lp;
    }

    public static double doubleMappingToQuantile(IPointable dp) {
        return (double) ((DoublePointable) dp).getDouble();
    }

    public static IPointable quantileRevertToDouble(double d) {
        DoublePointable dp = (DoublePointable) DoublePointable.FACTORY.createPointable();
        byte[] buf = new byte[DoublePointable.TYPE_TRAITS.getFixedLength()];
        dp.set(buf, 0, DoublePointable.TYPE_TRAITS.getFixedLength());
        dp.setDouble(d);
        return dp;
    }

    public static double shortMappingToQuantile(IPointable sp) {
        return (double) ((ShortPointable) sp).getShort();
    }

    public static IPointable quantileRevertToShort(double d) {
        ShortPointable sp = (ShortPointable) ShortPointable.FACTORY.createPointable();
        byte[] buf = new byte[ShortPointable.TYPE_TRAITS.getFixedLength()];
        sp.set(buf, 0, ShortPointable.TYPE_TRAITS.getFixedLength());
        sp.setShort((short) d);
        return sp;
    }

    public static double floatMappingToQuantile(IPointable fp) {
        return (double) ((FloatPointable) fp).getFloat();
    }

    public static IPointable quantileRevertToFloat(double d) {
        FloatPointable fp = (FloatPointable) FloatPointable.FACTORY.createPointable();
        byte[] buf = new byte[FloatPointable.TYPE_TRAITS.getFixedLength()];
        fp.set(buf, 0, FloatPointable.TYPE_TRAITS.getFixedLength());
        fp.setFloat((float) d);
        return fp;
    }
}
