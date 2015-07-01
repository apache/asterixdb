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
package edu.uci.ics.asterix.tools.external.data;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicLong;

public class GULongIDGenerator {

    private final int partition;
    private final long baseValue;
    private final AtomicLong nextValue;

    public GULongIDGenerator(int partition, byte seed) {
        this.partition = partition;
        ByteBuffer buffer = ByteBuffer.allocate(8);
        buffer.put(seed);
        buffer.put((byte) partition);
        buffer.putInt(0);
        buffer.putShort((short) 0);
        buffer.flip();
        this.baseValue = new Long(buffer.getLong());
        this.nextValue = new AtomicLong(baseValue);
    }

    public long getNextULong() {
        return nextValue.incrementAndGet();
    }

    public int getPartition() {
        return partition;
    }

}
