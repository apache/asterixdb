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
package org.apache.hyracks.storage.am.common.frames;

import java.nio.ByteBuffer;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.api.IValueReference;
import org.apache.hyracks.storage.am.common.api.ITreeIndexFrame;
import org.apache.hyracks.storage.am.common.api.ITreeIndexFrame.Constants;
import org.apache.hyracks.storage.am.common.api.ITreeIndexMetadataFrame;
import org.apache.hyracks.storage.common.buffercache.ICachedPage;

/**
 * Frame content
 * [ Headers defined in {@link ITreeIndexFrame}][max page][next page][valid]
 * [storage version][root page][free page count][k1 length][k1][v1 length][v1]
 * [k2 length][k2][v2 length][v2].......
 * ....
 * ....
 * [free page 5][free page 4][free page 3][free page 2][free page 1]
 *
 */
public class LIFOMetaDataFrame implements ITreeIndexMetadataFrame {

    private static final byte META_PAGE_LEVEL_INDICATOR = -1;
    private static final byte FREE_PAGE_LEVEL_INDICATOR = -2;
    protected static final int MAGIC_VALID_INT = 0x1B16DA7A;
    protected static final int MAX_PAGE_OFFSET = ITreeIndexFrame.Constants.RESERVED_HEADER_SIZE;
    protected static final int NEXT_PAGE_OFFSET = MAX_PAGE_OFFSET + 4;
    protected static final int VALID_OFFSET = NEXT_PAGE_OFFSET + 4;
    protected static final int STORAGE_VERSION_OFFSET = VALID_OFFSET + 4;
    protected static final int ROOT_PAGE_OFFSET = STORAGE_VERSION_OFFSET + 4;
    protected static final int FREE_PAGE_COUNT_OFFSET = ROOT_PAGE_OFFSET + 4;
    protected static final int HEADER_END_OFFSET = FREE_PAGE_COUNT_OFFSET + 4;

    protected ICachedPage page = null;
    protected ByteBuffer buf = null;

    @Override
    public int getMaxPage() {
        return buf.getInt(MAX_PAGE_OFFSET);
    }

    @Override
    public void setMaxPage(int maxPage) {
        buf.putInt(MAX_PAGE_OFFSET, maxPage);
    }

    @Override
    public int getFreePage() {
        int freePages = buf.getInt(FREE_PAGE_COUNT_OFFSET);
        if (freePages > 0) {
            decrement(FREE_PAGE_COUNT_OFFSET);
            return buf.getInt(buf.array().length - Integer.BYTES * freePages);
        }
        return -1;
    }

    @Override
    public int getSpace() {
        return buf.array().length - buf.getInt(Constants.FREE_SPACE_OFFSET)
                - (Integer.BYTES * buf.getInt(FREE_PAGE_COUNT_OFFSET));
    }

    @Override
    public void addFreePage(int freePage) {
        increment(FREE_PAGE_COUNT_OFFSET);
        int numFreePages = buf.getInt(FREE_PAGE_COUNT_OFFSET);
        buf.putInt(buf.array().length - (Integer.BYTES * numFreePages), freePage);
    }

    @Override
    public byte getLevel() {
        return buf.get(Constants.LEVEL_OFFSET);
    }

    @Override
    public void setLevel(byte level) {
        buf.put(Constants.LEVEL_OFFSET, level);
    }

    @Override
    public ICachedPage getPage() {
        return page;
    }

    @Override
    public void setPage(ICachedPage page) {
        this.page = page;
        this.buf = page.getBuffer();
    }

    @Override
    public void init() {
        buf.putInt(Constants.TUPLE_COUNT_OFFSET, 0);
        buf.putInt(Constants.FREE_SPACE_OFFSET, HEADER_END_OFFSET);
        buf.putInt(MAX_PAGE_OFFSET, 0);
        buf.put(Constants.LEVEL_OFFSET, META_PAGE_LEVEL_INDICATOR);
        buf.putInt(NEXT_PAGE_OFFSET, -1);
        buf.putInt(ROOT_PAGE_OFFSET, 1);
        buf.putInt(FREE_PAGE_COUNT_OFFSET, 0);
        buf.putInt(STORAGE_VERSION_OFFSET, ITreeIndexFrame.Constants.VERSION);
        setValid(false);
    }

    @Override
    public int getNextMetadataPage() {
        return buf.getInt(NEXT_PAGE_OFFSET);
    }

    @Override
    public void setNextMetadataPage(int nextPage) {
        buf.putInt(NEXT_PAGE_OFFSET, nextPage);
    }

    @Override
    public boolean isValid() {
        return buf.getInt(VALID_OFFSET) == MAGIC_VALID_INT;
    }

    @Override
    public void setValid(boolean isValid) {
        buf.putInt(VALID_OFFSET, isValid ? MAGIC_VALID_INT : 0);
    }

    @Override
    public int getVersion() {
        return buf.getInt(STORAGE_VERSION_OFFSET);
    }

    @Override
    public void setRootPageId(int rootPage) {
        buf.putInt(ROOT_PAGE_OFFSET, rootPage);
    }

    @Override
    public int getRootPageId() {
        return buf.getInt(ROOT_PAGE_OFFSET);
    }

    @Override
    public boolean isMetadataPage() {
        return getLevel() == META_PAGE_LEVEL_INDICATOR;
    }

    @Override
    public boolean isFreePage() {
        return getLevel() == FREE_PAGE_LEVEL_INDICATOR;
    }

    @Override
    public void get(IValueReference key, IPointable value) {
        int tupleCount = getTupleCount();
        int tupleStart = getTupleStart(0);
        for (int i = 0; i < tupleCount; i++) {
            if (isInner(key, tupleStart)) {
                get(tupleStart + key.getLength() + Integer.BYTES, value);
                return;
            }
            tupleStart = getNextTupleStart(tupleStart);
        }
        value.set(null, 0, 0);
    }

    private int find(IValueReference key) {
        int tupleCount = getTupleCount();
        int tupleStart = getTupleStart(0);
        for (int i = 0; i < tupleCount; i++) {
            if (isInner(key, tupleStart)) {
                return i;
            }
            tupleStart = getNextTupleStart(tupleStart);
        }
        return -1;
    }

    private void get(int offset, IPointable value) {
        int valueLength = buf.getInt(offset);
        value.set(buf.array(), offset + Integer.BYTES, valueLength);
    }

    private static final int compare(byte[] b1, int s1, byte[] b2, int s2, int l) {
        for (int i = 0; i < l; i++) {
            if (b1[s1 + i] != b2[s2 + i]) {
                return b1[s1 + i] - b2[s2 + i];
            }
        }
        return 0;
    }

    private boolean isInner(IValueReference key, int tupleOffset) {
        int keySize = buf.getInt(tupleOffset);
        if (keySize == key.getLength()) {
            return LIFOMetaDataFrame.compare(key.getByteArray(), key.getStartOffset(), buf.array(),
                    tupleOffset + Integer.BYTES, keySize) == 0;
        }
        return false;
    }

    private int getTupleStart(int index) {
        int offset = HEADER_END_OFFSET;
        int i = 0;
        while (i < index) {
            i++;
            offset = getNextTupleStart(offset);
        }
        return offset;
    }

    private int getNextTupleStart(int prevTupleOffset) {
        int keyLength = buf.getInt(prevTupleOffset);
        int offset = prevTupleOffset + keyLength + Integer.BYTES;
        return offset + buf.getInt(offset) + Integer.BYTES;
    }

    private void put(int index, IValueReference value) throws HyracksDataException {
        int offset = getTupleStart(index);
        int length = buf.getInt(offset);
        offset += Integer.BYTES + length;
        length = buf.getInt(offset);
        if (length != value.getLength()) {
            throw new HyracksDataException("This frame doesn't support overwriting dynamically sized values");
        }
        offset += Integer.BYTES;
        System.arraycopy(value.getByteArray(), value.getStartOffset(), buf.array(), offset, value.getLength());
    }

    @Override
    public void put(IValueReference key, IValueReference value) throws HyracksDataException {
        int index = find(key);
        if (index >= 0) {
            put(index, value);
        } else {
            int offset = buf.getInt(Constants.FREE_SPACE_OFFSET);
            int available = getSpace();
            int required = key.getLength() + Integer.BYTES + Integer.BYTES + value.getLength();
            if (available < required) {
                throw new HyracksDataException("Available space in the page (" + available
                        + ") is not enough to store the key value pair(" + required + ")");
            }
            buf.putInt(offset, key.getLength());
            offset += Integer.BYTES;
            System.arraycopy(key.getByteArray(), key.getStartOffset(), buf.array(), offset, key.getLength());
            offset += key.getLength();
            buf.putInt(offset, value.getLength());
            offset += Integer.BYTES;
            System.arraycopy(value.getByteArray(), value.getStartOffset(), buf.array(), offset, value.getLength());
            offset += value.getLength();
            increment(Constants.TUPLE_COUNT_OFFSET);
            buf.putInt(Constants.FREE_SPACE_OFFSET, offset);
        }
    }

    @Override
    public int getTupleCount() {
        return buf.getInt(Constants.TUPLE_COUNT_OFFSET);
    }

    private void increment(int offset) {
        buf.putInt(offset, buf.getInt(offset) + 1);
    }

    private void decrement(int offset) {
        buf.putInt(offset, buf.getInt(offset) - 1);
    }

    @Override
    public int getOffset(IValueReference key) {
        int index = find(key);
        if (index >= 0) {
            int offset = getTupleStart(index);
            return offset + key.getLength() + 2 * Integer.BYTES;
        }
        return -1;
    }

    @Override
    public String toString() {
        StringBuilder aString = new StringBuilder(this.getClass().getSimpleName()).append('\n')
                .append("Tuple Count: " + getTupleCount()).append('\n')
                .append("Free Space offset: " + buf.getInt(Constants.FREE_SPACE_OFFSET)).append('\n')
                .append("Level: " + buf.get(Constants.LEVEL_OFFSET)).append('\n')
                .append("Version: " + buf.getInt(STORAGE_VERSION_OFFSET)).append('\n')
                .append("Max Page: " + buf.getInt(MAX_PAGE_OFFSET)).append('\n')
                .append("Root Page: " + buf.getInt(ROOT_PAGE_OFFSET)).append('\n')
                .append("Number of free pages: " + buf.getInt(FREE_PAGE_COUNT_OFFSET));
        int tupleCount = getTupleCount();
        int offset;
        for (int i = 0; i < tupleCount; i++) {
            offset = getTupleStart(i);
            int keyLength = buf.getInt(offset);
            aString.append('\n').append("Key " + i + " size = " + keyLength);
            offset += Integer.BYTES + keyLength;
            int valueLength = buf.getInt(offset);
            aString.append(", Value " + i + " size = " + valueLength);
        }
        return aString.toString();
    }
}
