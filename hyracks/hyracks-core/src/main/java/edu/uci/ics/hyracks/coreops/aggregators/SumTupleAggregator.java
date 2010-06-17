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
package edu.uci.ics.hyracks.coreops.aggregators;

import edu.uci.ics.hyracks.api.dataflow.IDataWriter;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.coreops.ITupleAggregator;

public class SumTupleAggregator implements ITupleAggregator {

    private Object key;
    private int count;

    @Override
    public void add(Object[] data) {
        count++;
    }

    @Override
    public void init(Object[] data) {
        key = data[0];
        count = 0;
    }

    @Override
    public void write(IDataWriter<Object[]> writer) throws HyracksDataException {
        writer.writeData(new Object[] { key, count });
    }
}