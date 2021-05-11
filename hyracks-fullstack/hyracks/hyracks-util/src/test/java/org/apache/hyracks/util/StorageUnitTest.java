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
package org.apache.hyracks.util;

import org.junit.Assert;
import org.junit.Test;

public class StorageUnitTest {

    @Test
    public void test() {
        // Valid cases
        double result1NoUnit = StorageUtil.getSizeInBytes("1"); // Defaults to bytes
        Assert.assertEquals(1.0, result1NoUnit, 0);

        double result1B = StorageUtil.getSizeInBytes("1B");
        Assert.assertEquals(1.0, result1B, 0);

        double result1BWithSpaces = StorageUtil.getSizeInBytes("1 B ");
        Assert.assertEquals(1.0, result1BWithSpaces, 0);

        double result1Kb = StorageUtil.getSizeInBytes("1KB");
        Assert.assertEquals(1024.0, result1Kb, 0);

        double result1KbWithSpaces = StorageUtil.getSizeInBytes(" 1 K B ");
        Assert.assertEquals(1024.0, result1KbWithSpaces, 0);

        double resultPoint5KB = StorageUtil.getSizeInBytes(".5KB");
        Assert.assertEquals(512.0, resultPoint5KB, 0);

        double resultPoint5SmallKB = StorageUtil.getSizeInBytes(".5kB");
        Assert.assertEquals(512.0, resultPoint5SmallKB, 0);

        double result1Mb = StorageUtil.getSizeInBytes("1MB");
        Assert.assertEquals(1024.0 * 1024.0, result1Mb, 0);

        double result1Point0Mb = StorageUtil.getSizeInBytes("1.0MB");
        Assert.assertEquals(1024.0 * 1024.0, result1Point0Mb, 0);

        double result01Point0Mb = StorageUtil.getSizeInBytes("01.0MB");
        Assert.assertEquals(1024.0 * 1024.0, result01Point0Mb, 0);

        // Invalid cases
        invalidCase("");
        invalidCase("99999999999999999999999999999999999999999999999999999999999999999999999999999999999999999");
        invalidCase("32MB123");
        invalidCase("1.1.1");
        invalidCase("12KBMB");
        invalidCase("MB");
        invalidCase("1AB");
        invalidCase("MB1MB");
        invalidCase("123MBB");
    }

    private void invalidCase(String value) {
        try {
            StorageUtil.getSizeInBytes(value);
        } catch (Exception ex) {
            Assert.assertTrue(ex.toString()
                    .contains("IllegalArgumentException: The given string: " + value + " is not a byte unit string"));
        }
    }
}
