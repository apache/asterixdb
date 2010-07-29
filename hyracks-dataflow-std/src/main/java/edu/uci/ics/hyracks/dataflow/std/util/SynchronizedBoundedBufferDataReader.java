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
package edu.uci.ics.hyracks.dataflow.std.util;

import edu.uci.ics.hyracks.api.dataflow.IOpenableDataReader;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;

public class SynchronizedBoundedBufferDataReader implements IOpenableDataReader<Object[]> {
    private SynchronizedBoundedBuffer<Object[]> queue;

    public SynchronizedBoundedBufferDataReader(SynchronizedBoundedBuffer<Object[]> queue) {
        this.queue = queue;
    }

    @Override
    public Object[] readData() throws HyracksDataException {
        try {
            return queue.get();
        } catch (InterruptedException e) {
            throw new HyracksDataException(e);
        }
    }

    @Override
    public void close() {
        queue = null;
    }

    @Override
    public void open() {
        // do nothing
    }
}