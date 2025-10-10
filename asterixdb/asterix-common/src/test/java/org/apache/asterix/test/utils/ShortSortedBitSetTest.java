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

package org.apache.asterix.test.utils;

import java.util.Random;

import org.apache.asterix.common.utils.ShortSortedBitSet;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;

import it.unimi.dsi.fastutil.shorts.ShortIterator;
import it.unimi.dsi.fastutil.shorts.ShortSortedSet;

public class ShortSortedBitSetTest {
    private static final Logger LOGGER = LogManager.getLogger();

    @Test
    public void test() {
        Random r = new Random();
        ShortSortedSet set = new ShortSortedBitSet();
        short uniques = 0;
        for (short i = 0; i < 500; i++) {
            uniques += set.add((short) r.nextInt(1000)) ? 1 : 0;
        }
        LOGGER.info("set {}", set);
        LOGGER.info("set contains {} unique elements", uniques);
        Assert.assertEquals("set size() != number of uniques", uniques, set.size());
        for (short values : set) {
            uniques--;
        }
        Assert.assertEquals("iteration count != number of uniques", 0, uniques);
        int size = set.size();
        short removed = 0;
        for (ShortIterator iter = set.iterator(); iter.hasNext();) {
            iter.nextShort();
            iter.remove();
            removed++;
        }
        Assert.assertEquals("removed count != before size", size, removed);
        Assert.assertEquals("new size != 0", 0, set.size());
        for (ShortIterator iter = set.iterator(); iter.hasNext();) {
            Assert.fail("got iteration on empty set: " + iter.nextShort());
        }
        for (short value : set) {
            Assert.fail("got iteration on empty set: " + value);
        }
    }

    @Test
    public void testSingletonSets() {
        for (short i = 0; i <= 16384; i++) {
            testSingletonSet(i);
        }
    }

    private void testSingletonSet(short key) {
        LOGGER.log(key % 1024 == 0 ? Level.INFO : Level.DEBUG, "testing {}", key);
        ShortSortedSet set = new ShortSortedBitSet();
        set.add(key);
        final int size = set.size();
        LOGGER.debug("singleton set {} contains {} unique elements", set, size);
        Assert.assertEquals("singleton size != 1", 1, size);
        for (short values : set) {
            LOGGER.debug("got value: {}", values);
        }
        int removed = 0;
        for (ShortIterator iter = set.iterator(); iter.hasNext();) {
            iter.nextShort();
            iter.remove();
            removed++;
        }
        Assert.assertEquals("removed count != before size", size, removed);
        Assert.assertEquals("new size != 0", 0, set.size());
    }

    @Test
    public void emptySetTest() {
        ShortSortedSet set = new ShortSortedBitSet();
        LOGGER.info("set {}", set);
        LOGGER.info("set contains {} unique elements", set.size());
        for (ShortIterator iter = set.iterator(); iter.hasNext();) {
            Assert.fail("got iteration on empty set: " + iter.nextShort());
        }
        for (short value : set) {
            Assert.fail("got iteration on empty set: " + value);
        }
    }
}
