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

package org.apache.hyracks.util.trace;

import org.junit.Assert;
import org.junit.Test;

public class TraceCategoryRegistryTest {

    @Test
    public void limit() {
        TraceCategoryRegistry registry = new TraceCategoryRegistry();

        int success = 0;
        int fail = 0;
        String expectedError = "Cannot add category";
        String actualError = null;
        for (int i = 0; i < ITraceCategoryRegistry.NO_CATEGORIES + 1; ++i) {
            try {
                final String name = "cat" + Integer.toHexString(i);
                registry.get(name);
                ++success;
            } catch (IllegalStateException ise) {
                actualError = ise.getMessage();
                ++fail;
            }
        }
        Assert.assertEquals(64, success);
        Assert.assertEquals(1, fail);
        Assert.assertTrue(actualError.contains(expectedError));
    }

    @Test
    public void getNameValidCode() {
        TraceCategoryRegistry registry = new TraceCategoryRegistry();
        registry.get("catA");
        registry.get("catB");
        registry.get("catC");

        int success = 0;
        int fail = 0;
        String expectedError = "No category for code";
        String actualError = null;
        String expectedResult = "catC";
        String actualResult = null;
        for (int i = 0; i < ITraceCategoryRegistry.NO_CATEGORIES; ++i) {
            try {
                final long code = 1L << i;
                actualResult = registry.getName(code);
                ++success;
            } catch (IllegalArgumentException iae) {
                actualError = iae.getMessage();
                ++fail;
            }
        }

        Assert.assertEquals(3, success);
        Assert.assertEquals(61, fail);
        Assert.assertEquals(actualResult, expectedResult);
        Assert.assertTrue(actualError.contains(expectedError));
    }

    @Test
    public void getNameInvalidCode() {
        TraceCategoryRegistry registry = new TraceCategoryRegistry();

        final String expectedError = "No category for code";
        String actualError = null;
        try {
            registry.getName(11);
        } catch (IllegalArgumentException iae) {
            actualError = iae.getMessage();
        }
        Assert.assertTrue(actualError.contains(expectedError));
    }
}
