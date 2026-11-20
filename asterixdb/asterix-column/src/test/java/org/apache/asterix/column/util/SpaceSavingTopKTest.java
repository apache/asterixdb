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
package org.apache.asterix.column.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.asterix.om.types.ATypeTag;
import org.apache.hyracks.data.std.primitive.VoidPointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.junit.Test;

public class SpaceSavingTopKTest {

    private static ArrayBackedValueStorage key(String s) throws Exception {
        ArrayBackedValueStorage storage = new ArrayBackedValueStorage();
        storage.getDataOutput().write(s.getBytes(StandardCharsets.UTF_8));
        return storage;
    }

    /** A removed key is no longer counted or offered, and its slot is free for the next newcomer, error-free. */
    @Test
    public void aRemovedKeyLeavesTheOthersAsTheyWere() throws Exception {
        SpaceSavingTopK counter = new SpaceSavingTopK(3);
        for (int i = 0; i < 10; i++) {
            counter.add(key("a"), ATypeTag.BIGINT);
            counter.add(key("b"), ATypeTag.BIGINT);
            counter.add(key("c"), ATypeTag.BIGINT);
        }
        counter.remove(key("a"));
        counter.remove(key("absent"));
        counter.add(key("d"), ATypeTag.BIGINT);
        List<String> offered = new ArrayList<>();
        for (byte[] k : counter.keysAtLeast(1)) {
            offered.add(new String(k, StandardCharsets.UTF_8));
        }
        Collections.sort(offered);
        assertEquals(List.of("b", "c", "d"), offered);
        for (SpaceSavingTopK.Entry e : counter.entries()) {
            String k = new String(e.key(), StandardCharsets.UTF_8);
            assertEquals(k, k.equals("d") ? 1 : 10, e.count());
            assertEquals(k, 0, e.error());
        }
    }

    @Test
    public void capacityIsNeverExceeded() throws Exception {
        SpaceSavingTopK counter = new SpaceSavingTopK(8);
        for (int i = 0; i < 1000; i++) {
            counter.add(key("key-" + i), null);
        }
        assertTrue("capacity must bound the counter regardless of distinct key count", counter.entries().size() <= 8);
    }

    @Test
    public void denseKeySurvivesAFloodOfUniqueKeys() throws Exception {
        SpaceSavingTopK counter = new SpaceSavingTopK(8);
        ArrayBackedValueStorage hot = key("hot");
        for (int i = 0; i < 1000; i++) {
            counter.add(hot, null);
            counter.add(key("cold-" + i), null);
        }
        SpaceSavingTopK.Entry hotEntry = null;
        for (SpaceSavingTopK.Entry e : counter.entries()) {
            if (new String(e.key(), StandardCharsets.UTF_8).equals("hot")) {
                hotEntry = e;
            }
        }
        assertNotNull("the dense key must be retained", hotEntry);
        assertTrue("its guaranteed lower bound must reflect most of its occurrences",
                hotEntry.count() - hotEntry.error() >= 900);
    }

    @Test
    public void exactWhenUnderCapacity() throws Exception {
        SpaceSavingTopK counter = new SpaceSavingTopK(8);
        for (int i = 0; i < 5; i++) {
            counter.add(key("a"), null);
        }
        counter.add(key("b"), null);
        assertEquals(2, counter.entries().size());
        for (SpaceSavingTopK.Entry e : counter.entries()) {
            assertEquals("no eviction means no error", 0, e.error());
        }
    }

    @Test
    public void reusingTheCallersBufferDoesNotCorruptKeys() throws Exception {
        SpaceSavingTopK counter = new SpaceSavingTopK(8);
        ArrayBackedValueStorage reused = new ArrayBackedValueStorage();
        for (String name : new String[] { "alpha", "beta", "alpha" }) {
            reused.reset();
            reused.getDataOutput().write(name.getBytes(StandardCharsets.UTF_8));
            counter.add(reused, null);
        }
        assertEquals("distinct keys must survive the caller reusing one buffer", 2, counter.entries().size());
        for (SpaceSavingTopK.Entry e : counter.entries()) {
            if (new String(e.key(), StandardCharsets.UTF_8).equals("alpha")) {
                assertEquals(2, e.count());
            }
        }
    }

    @Test
    public void keysAtDifferentOffsetsMatchByContent() throws Exception {
        SpaceSavingTopK counter = new SpaceSavingTopK(8);
        byte[] buffer = "xxalphaxbetaalpha".getBytes(StandardCharsets.UTF_8);
        VoidPointable slice = new VoidPointable();
        slice.set(buffer, 2, 5);
        counter.add(slice, null);
        slice.set(buffer, 8, 4);
        counter.add(slice, null);
        slice.set(buffer, 12, 5);
        counter.add(slice, null);
        assertEquals("the same bytes at another offset are the same key", 2, counter.entries().size());
        for (SpaceSavingTopK.Entry e : counter.entries()) {
            String name = new String(e.key(), StandardCharsets.UTF_8);
            assertEquals(name, name.equals("alpha") ? 2 : 1, e.count());
        }
    }

    /** An evicted slot is rewritten for its replacement, whether or not the two keys are the same length. */
    @Test
    public void aReusedSlotHoldsItsNewKeyExactly() throws Exception {
        SpaceSavingTopK counter = new SpaceSavingTopK(1);
        counter.add(key("abc"), null);
        counter.add(key("xyz"), null);
        counter.add(key("longer"), null);
        counter.add(key("ab"), null);
        SpaceSavingTopK.Entry entry = counter.entries().get(0);
        assertEquals("ab", new String(entry.key(), StandardCharsets.UTF_8));
        assertEquals(4, entry.count());
        assertEquals(3, entry.error());
        counter.add(key("ab"), null);
        assertEquals("the reused key must still be found by content", 5, counter.entries().get(0).count());
    }

    @Test
    public void keysAtLeastReturnsDenseTypedKeysBestFirst() throws Exception {
        SpaceSavingTopK counter = new SpaceSavingTopK(4);
        add(counter, "b", ATypeTag.BIGINT, 3);
        add(counter, "a", ATypeTag.BIGINT, 5);
        add(counter, "c", ATypeTag.BIGINT, 1);
        add(counter, "onlyNull", null, 6);
        List<String> found = new ArrayList<>();
        for (byte[] key : counter.keysAtLeast(2)) {
            found.add(new String(key, StandardCharsets.UTF_8));
        }
        assertEquals(List.of("a", "b"), found);
        assertSame("nothing qualifying must allocate nothing", Collections.emptyList(), counter.keysAtLeast(6));
    }

    @Test
    public void keysAtLeastTestsTheLowerBound() throws Exception {
        SpaceSavingTopK counter = new SpaceSavingTopK(1);
        add(counter, "a", ATypeTag.BIGINT, 2);
        add(counter, "b", ATypeTag.BIGINT, 1);
        // b evicted a and inherited its 2: counted 3, of which only 1 is certain.
        assertEquals(3, counter.entries().get(0).count());
        assertTrue(counter.keysAtLeast(2).isEmpty());
        assertEquals(1, counter.keysAtLeast(1).size());
    }

    private static void add(SpaceSavingTopK counter, String name, ATypeTag typeTag, int times) throws Exception {
        for (int i = 0; i < times; i++) {
            counter.add(key(name), typeTag);
        }
    }

    @Test
    public void resetClears() throws Exception {
        SpaceSavingTopK counter = new SpaceSavingTopK(4);
        counter.add(key("a"), null);
        counter.reset();
        assertEquals(0, counter.entries().size());
        assertTrue(counter.entries().isEmpty());
    }
}
