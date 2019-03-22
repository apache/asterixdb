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

package org.apache.hyracks.storage.am.bloomfilter.util;

import java.util.Random;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.After;
import org.junit.Before;

public abstract class AbstractBloomFilterTest {
    protected final Logger LOGGER = LogManager.getLogger();

    protected final BloomFilterTestHarness harness;

    public AbstractBloomFilterTest() {
        harness = new BloomFilterTestHarness();
    }

    public AbstractBloomFilterTest(int pageSize, int numPages, int maxOpenFiles, int hyracksFrameSize) {
        harness = new BloomFilterTestHarness(pageSize, numPages, maxOpenFiles, hyracksFrameSize);
    }

    @Before
    public void setUp() throws HyracksDataException {
        harness.setUp();
    }

    @After
    public void tearDown() throws HyracksDataException {
        harness.tearDown();
    }

    public static String randomString(int length, Random random) {
        char[] chars = "abcdefghijklmnopqrstuvwxyz".toCharArray();
        StringBuilder strBuilder = new StringBuilder();
        for (int i = 0; i < length; ++i) {
            char c = chars[random.nextInt(chars.length)];
            strBuilder.append(c);
        }
        return strBuilder.toString();
    }
}
