/*
 * Copyright 2009-2010 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package edu.uci.ics.hyracks.storage.am.common.frames;

import java.nio.ByteBuffer;

import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexMetaDataFrame;
import edu.uci.ics.hyracks.storage.common.buffercache.ICachedPage;

// all meta pages of this kind have a negative level
// the first meta page has level -1, all other meta pages have level -2
// the first meta page is special because it guarantees to have a correct max page
// other meta pages (i.e., with level -2) have junk in the max page field

public class LIFOMetaDataFrame implements ITreeIndexMetaDataFrame {

    protected static final int tupleCountOff = 0;
    protected static final int freeSpaceOff = tupleCountOff + 4;
    protected static final int maxPageOff = freeSpaceOff + 4;
    protected static final byte levelOff = maxPageOff + 1;
    protected static final byte nextPageOff = maxPageOff + 8;

    protected ICachedPage page = null;
    protected ByteBuffer buf = null;

    public int getMaxPage() {
        return buf.getInt(maxPageOff);
    }

    public void setMaxPage(int maxPage) {
        buf.putInt(maxPageOff, maxPage);
    }

    public int getFreePage() {
        int tupleCount = buf.getInt(tupleCountOff);
        if (tupleCount > 0) {
            // return the last page from the linked list of free pages
            // TODO: this is a dumb policy, but good enough for now
            int lastPageOff = buf.getInt(freeSpaceOff) - 4;
            buf.putInt(freeSpaceOff, lastPageOff);
            buf.putInt(tupleCountOff, tupleCount - 1);
            return buf.getInt(lastPageOff);
        } else {
            return -1;
        }
    }

    // must be checked before adding free page
    // user of this class is responsible for getting a free page as a new meta
    // page, latching it, etc. if there is no space on this page
    public boolean hasSpace() {
        return buf.getInt(freeSpaceOff) + 4 < buf.capacity();
    }

    // on bounds checking is done, there must be free space
    public void addFreePage(int freePage) {
        int freeSpace = buf.getInt(freeSpaceOff);
        buf.putInt(freeSpace, freePage);
        buf.putInt(freeSpaceOff, freeSpace + 4);
        buf.putInt(tupleCountOff, buf.getInt(tupleCountOff) + 1);
    }

    @Override
    public byte getLevel() {
        return buf.get(levelOff);
    }

    @Override
    public void setLevel(byte level) {
        buf.put(levelOff, level);
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
    public void initBuffer(int level) {
        buf.putInt(freeSpaceOff, nextPageOff + 4);
        buf.putInt(tupleCountOff, 0);
        buf.putInt(levelOff, level);
        buf.putInt(nextPageOff, -1);
    }

    @Override
    public int getNextPage() {
        return buf.getInt(nextPageOff);
    }

    @Override
    public void setNextPage(int nextPage) {
        buf.putInt(nextPageOff, nextPage);
    }
}
