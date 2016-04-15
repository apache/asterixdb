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

package org.apache.hyracks.dataflow.std.parallel;

import static org.junit.Assert.assertEquals;

import java.util.logging.Logger;

import org.apache.hyracks.api.exceptions.HyracksException;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.primitive.IntegerPointable;
import org.apache.hyracks.data.std.primitive.UTF8StringPointable;
import org.apache.hyracks.dataflow.std.parallel.util.HistogramUtils;
import org.apache.hyracks.util.string.UTF8StringUtil;
import org.junit.Test;

public class TypeTranslateTest {
    private static final Logger LOGGER = Logger.getLogger(TypeTranslateTest.class.getName());

    @Test
    public void testIntPoitable() throws HyracksException {
        //IBinaryComparator comp = PointableBinaryComparatorFactory.of(IntegerPointable.FACTORY).createBinaryComparator();
        byte[] ip255 = new byte[4];
        ip255[3] |= 0xff;
        IPointable ip = new IntegerPointable();
        ip.set(ip255, 0, IntegerPointable.TYPE_TRAITS.getFixedLength());
        int iip = ((IntegerPointable) ip).getInteger();
        assertEquals(iip, 255);
        ip255[2] |= 0xff;
        ip.set(ip255, 0, IntegerPointable.TYPE_TRAITS.getFixedLength());
        iip = ((IntegerPointable) ip).getInteger();
        assertEquals(iip, 65535);

        String longString = new String("2345n,3+)*(&)*&)**UPIUIPPIJKLH7youihuh1kjerhto34545435"
                + "8t73048534j5;kj;krejtpreiutpiq34n;krnq;kwerj;qwkj4pi32ou4j;wker;qwernqwe/mr"
                + "nqwlh432j423nn4.qmrnqm.wn34lj23q4.q3nw4.mqn4lhq2j34n3qmn4.w34hnjqk2n4.3mn4."
                + "3wqnr.mqweh4\"\"[][]][]<>()j3qn.4mqnw34hqjkw4nmersnhjknwemrw.r中h23nwrjjjkh5");
        IPointable sp = new UTF8StringPointable();
        byte[] bsc = HistogramUtils.ansiToUTF8Byte(longString, 0);
        sp.set(bsc, 0, bsc.length);
        LOGGER.info("The pointable string has the length: " + sp.getLength() + " from origin length: "
                + longString.length());
        StringBuilder sb = new StringBuilder();
        UTF8StringUtil.toString(sb, sp.getByteArray(), 0);
        String s1 = sb.toString();
        String s2 = longString;
        assertEquals(s1, s2);

        IPointable usp = new UTF8StringPointable();
        byte[] ubsc = HistogramUtils.toUTF8Byte(new String("横空出世"), 0);
        usp.set(ubsc, 0, ubsc.length);
        StringBuilder usb = new StringBuilder();
        UTF8StringUtil.toString(usb, usp.getByteArray(), 0);
        String us1 = usb.toString();
        String us2 = "横空出世";
        assertEquals(us1, us2);

        long quantile = HistogramUtils.ansiMappingToLong((UTF8StringPointable) sp, 0, 8);
        UTF8StringPointable sQuan = HistogramUtils.longMappingToAnsi(quantile, 8);
        StringBuilder sb1 = new StringBuilder();
        UTF8StringUtil.toString(sb1, sQuan.getByteArray(), 0);
        assertEquals(sb.toString().substring(0, 8), sb1.toString());
        LOGGER.info("The origin length is: " + s2.length() + " of the string: " + s2 + " by atomically cut as: "
                + sb1.length() + " of the string " + sb1);
        return;
    }
}
