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
package edu.uci.ics.hyracks.dataflow.std.group;

import java.util.ArrayList;
import java.util.List;

import edu.uci.ics.hyracks.api.dataflow.IOpenableDataReader;
import edu.uci.ics.hyracks.api.dataflow.IOpenableDataWriter;
import edu.uci.ics.hyracks.api.dataflow.value.IComparator;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.std.base.IOpenableDataWriterOperator;

public class DeserializedPreclusteredGroupOperator implements IOpenableDataWriterOperator {
    private final int[] groupFields;

    private final IComparator[] comparators;

    private final IGroupAggregator aggregator;

    private Object[] lastData;

    private IOpenableDataWriter<Object[]> writer;

    private List<Object[]> buffer;

    private IOpenableDataReader<Object[]> reader;

    public DeserializedPreclusteredGroupOperator(int[] groupFields, IComparator[] comparators,
            IGroupAggregator aggregator) {
        this.groupFields = groupFields;
        this.comparators = comparators;
        this.aggregator = aggregator;
        buffer = new ArrayList<Object[]>();
        reader = new IOpenableDataReader<Object[]>() {
            private int idx;

            @Override
            public void open() {
                idx = 0;
            }

            @Override
            public void close() {
            }

            @Override
            public Object[] readData() {
                return idx >= buffer.size() ? null : buffer.get(idx++);
            }
        };
    }

    @Override
    public void close() throws HyracksDataException {
        if (!buffer.isEmpty()) {
            aggregate();
        }
        writer.close();
        try {
            aggregator.close();
        } catch (Exception e) {
            throw new HyracksDataException(e);
        }
    }

    private void aggregate() throws HyracksDataException {
        reader.open();
        aggregator.aggregate(reader, writer);
        reader.close();
        buffer.clear();
    }

    @Override
    public void open() throws HyracksDataException {
        lastData = null;
        writer.open();
    }

    @Override
    public void setDataWriter(int index, IOpenableDataWriter<Object[]> writer) {
        if (index != 0) {
            throw new IllegalArgumentException();
        }
        this.writer = writer;
    }

    @Override
    public void writeData(Object[] data) throws HyracksDataException {
        if (lastData != null && compare(data, lastData) != 0) {
            aggregate();
        }
        lastData = data;
        buffer.add(data);
    }

    private int compare(Object[] d1, Object[] d2) {
        for (int i = 0; i < groupFields.length; ++i) {
            int fIdx = groupFields[i];
            int c = comparators[i].compare(d1[fIdx], d2[fIdx]);
            if (c != 0) {
                return c;
            }
        }
        return 0;
    }

    @Override
    public void fail() throws HyracksDataException {
        // TODO Auto-generated method stub

    }
}