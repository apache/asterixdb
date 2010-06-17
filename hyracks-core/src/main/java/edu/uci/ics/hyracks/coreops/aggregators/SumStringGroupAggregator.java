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

import edu.uci.ics.hyracks.api.dataflow.IDataReader;
import edu.uci.ics.hyracks.api.dataflow.IDataWriter;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.coreops.group.IGroupAggregator;

public class SumStringGroupAggregator implements IGroupAggregator {
    private static final long serialVersionUID = 1L;
    private int keyIdx;

    public SumStringGroupAggregator(int keyIdx) {
        this.keyIdx = keyIdx;
    }

    @Override
    public void aggregate(IDataReader<Object[]> reader, IDataWriter<Object[]> writer) throws HyracksDataException {
        String key = "";
        Object[] data;
        int count = 0;
        while ((data = reader.readData()) != null) {
            key = (String) data[keyIdx];
            ++count;
        }
        writer.writeData(new Object[] { key, String.valueOf(count) });
    }

    @Override
    public void close() throws Exception {
    }
}