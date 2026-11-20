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
package org.apache.hyracks.storage.am.lsm.btree.column.utils;

import static org.junit.Assert.assertArrayEquals;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IValueReference;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.storage.am.common.freepage.MutableArrayValueReference;
import org.apache.hyracks.storage.am.lsm.common.api.IComponentMetadata;
import org.apache.hyracks.storage.am.lsm.common.impls.ChunkedComponentMetadataReaderWriter;
import org.apache.hyracks.storage.common.compression.SnappyCompressorDecompressor;
import org.junit.Test;

/**
 * A chunked metadata value is written on whatever room the page holding the entries before it has left -- the theta
 * sketch after the other component metadata, say. Too large for that room, it is chunked: chunk 0 must still hold its
 * whole header, however few bytes the page has left.
 */
public class ChunkedComponentMetadataTest {
    private static final int PAGE_SIZE = 256;
    private static final ChunkedComponentMetadataReaderWriter READER_WRITER = new ChunkedComponentMetadataReaderWriter(
            new MutableArrayValueReference("CHUNKED".getBytes()), SnappyCompressorDecompressor.INSTANCE);

    @Test
    public void aValueReadsBackWhateverRoomThePageHasLeft() throws Exception {
        byte[] bytes = new byte[3 * PAGE_SIZE];
        new Random(42).nextBytes(bytes);
        ArrayBackedValueStorage value = new ArrayBackedValueStorage();
        value.append(bytes, 0, bytes.length);
        for (int free = 0; free <= 40; free++) {
            PagedMetadata metadata = new PagedMetadata();
            metadata.fillTo(free);
            READER_WRITER.writeMetadata(value, metadata);
            ArrayBackedValueStorage read = new ArrayBackedValueStorage();
            READER_WRITER.readMetadata(metadata, read);
            assertArrayEquals("with " + free + " bytes available", bytes, Arrays.copyOfRange(read.getByteArray(),
                    read.getStartOffset(), read.getStartOffset() + read.getLength()));
        }
    }

    /**
     * Pages as the append-only metadata page manager keeps them: an entry costs its key and value and two lengths, an
     * entry that does not fit goes to a new page, and a page left with no room is followed by a new one.
     */
    private static final class PagedMetadata implements IComponentMetadata {
        private static final int ENTRY_OVERHEAD = 2 * Integer.BYTES;
        private final List<Map<String, byte[]>> pages = new ArrayList<>();
        private int space;

        PagedMetadata() {
            newPage();
        }

        /** Takes the current page down to {@code available} bytes available, with an entry of its own. */
        void fillTo(int available) throws HyracksDataException {
            ArrayBackedValueStorage filler = new ArrayBackedValueStorage();
            filler.setSize(space - (available + ENTRY_OVERHEAD) - 1 - ENTRY_OVERHEAD);
            ArrayBackedValueStorage key = new ArrayBackedValueStorage();
            key.append(new byte[] { 'f' }, 0, 1);
            put(key, filler);
        }

        private void newPage() {
            pages.add(new LinkedHashMap<>());
            space = PAGE_SIZE;
        }

        @Override
        public int getPageSize() {
            return PAGE_SIZE;
        }

        @Override
        public int getAvailableSpace() {
            return space - ENTRY_OVERHEAD;
        }

        @Override
        public void put(IValueReference key, IValueReference value) throws HyracksDataException {
            int required = key.getLength() + value.getLength() + ENTRY_OVERHEAD;
            if (space < required) {
                newPage();
            }
            if (value.getLength() < 0 || space < required) {
                throw new HyracksDataException("cannot store a value of " + value.getLength() + " bytes");
            }
            pages.get(pages.size() - 1).put(string(key), Arrays.copyOfRange(value.getByteArray(),
                    value.getStartOffset(), value.getStartOffset() + value.getLength()));
            space -= required;
            if (space == 0) {
                newPage();
            }
        }

        @Override
        public boolean get(IValueReference key, ArrayBackedValueStorage storage) throws HyracksDataException {
            for (Map<String, byte[]> page : pages) {
                byte[] value = page.get(string(key));
                if (value != null) {
                    storage.append(value, 0, value.length);
                    return true;
                }
            }
            return false;
        }

        private static String string(IValueReference key) {
            return Arrays.toString(Arrays.copyOfRange(key.getByteArray(), key.getStartOffset(),
                    key.getStartOffset() + key.getLength()));
        }
    }
}
