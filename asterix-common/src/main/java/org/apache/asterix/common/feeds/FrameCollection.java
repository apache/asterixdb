/*
 * Copyright 2009-2013 by The Regents of the University of California
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
package edu.uci.ics.asterix.common.feeds;

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import edu.uci.ics.asterix.common.feeds.api.IFeedMemoryComponent;
import edu.uci.ics.asterix.common.feeds.api.IFeedMemoryManager;

/**
 * Represents an expandable collection of frames.
 */
public class FrameCollection implements IFeedMemoryComponent {

    /** A unique identifier for the feed memory component **/
    private final int componentId;

    /** A collection of frames (each being a ByteBuffer) **/
    private final List<ByteBuffer> frames = new LinkedList<ByteBuffer>();

    /** The permitted maximum size, the collection may grow to **/
    private int maxSize;

    /** The {@link IFeedMemoryManager} for the NodeController **/
    private final IFeedMemoryManager memoryManager;

    public FrameCollection(int componentId, IFeedMemoryManager memoryManager, int maxSize) {
        this.componentId = componentId;
        this.maxSize = maxSize;
        this.memoryManager = memoryManager;
    }

    public boolean addFrame(ByteBuffer frame) {
        if (frames.size() == maxSize) {
            boolean expansionGranted = memoryManager.expandMemoryComponent(this);
            if (!expansionGranted) {
                return false;
            }
        }
        ByteBuffer storageBuffer = ByteBuffer.allocate(frame.capacity());
        storageBuffer.put(frame);
        frames.add(storageBuffer);
        storageBuffer.flip();
        return true;
    }

    public Iterator<ByteBuffer> getFrameCollectionIterator() {
        return frames.iterator();
    }

    @Override
    public int getTotalAllocation() {
        return frames.size();
    }

    @Override
    public Type getType() {
        return Type.COLLECTION;
    }

    @Override
    public int getComponentId() {
        return componentId;
    }

    @Override
    public void expand(int delta) {
        maxSize = maxSize + delta;
    }

    @Override
    public void reset() {
        frames.clear();
        maxSize = IFeedMemoryManager.START_COLLECTION_SIZE;
    }

    @Override
    public String toString() {
        return "FrameCollection" + "[" + componentId + "]" + "(" + frames.size() + "/" + maxSize + ")";
    }

}
