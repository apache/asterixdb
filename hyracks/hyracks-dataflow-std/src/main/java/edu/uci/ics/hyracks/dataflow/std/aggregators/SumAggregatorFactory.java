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
package edu.uci.ics.hyracks.dataflow.std.aggregators;

import java.io.DataOutput;
import java.io.IOException;

import edu.uci.ics.hyracks.api.comm.IFrameTupleAccessor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.IntegerSerializerDeserializer;

/**
 * SUM aggregator factory (for integer only; another SUM aggregator for floats
 * is available at {@link FloatSumAggregatorFactory})
 */
public class SumAggregatorFactory implements IFieldValueResultingAggregatorFactory {

    private int sumField;

    public SumAggregatorFactory(int field) {
        sumField = field;
    }

    /**
	 * 
	 */
    private static final long serialVersionUID = 1L;

    /*
     * (non-Javadoc)
     * 
     * @see edu.uci.ics.hyracks.dataflow.std.aggregators.
     * IFieldValueResultingAggregatorFactory
     * #createFieldValueResultingAggregator()
     */
    @Override
    public IFieldValueResultingAggregator createFieldValueResultingAggregator() {
        return new IFieldValueResultingAggregator() {
            private int sum;

            @Override
            public void output(DataOutput resultAcceptor) throws HyracksDataException {
                try {
                    resultAcceptor.writeInt(sum);
                } catch (IOException e) {
                    throw new HyracksDataException(e);
                }
            }

            @Override
            public void init(IFrameTupleAccessor accessor, int tIndex) throws HyracksDataException {
                sum++;
            }

            @Override
            public void accumulate(IFrameTupleAccessor accessor, int tIndex) throws HyracksDataException {
                int tupleOffset = accessor.getTupleStartOffset(tIndex);
                int fieldCount = accessor.getFieldCount();
                int fieldStart = accessor.getFieldStartOffset(tIndex, sumField);
                sum += IntegerSerializerDeserializer.getInt(accessor.getBuffer().array(), tupleOffset + 2 * fieldCount
                        + fieldStart);
            }
        };
    }

    /*
     * (non-Javadoc)
     * 
     * @see edu.uci.ics.hyracks.dataflow.std.aggregators.spillable.
     * ISpillableFieldValueResultingAggregatorFactory
     * #createFieldValueResultingAggregator()
     */
    @Override
    public ISpillableFieldValueResultingAggregator createSpillableFieldValueResultingAggregator() {
        return new ISpillableFieldValueResultingAggregator() {

            private int sum;

            @Override
            public void output(DataOutput resultAcceptor) throws HyracksDataException {
                try {
                    resultAcceptor.writeInt(sum);
                } catch (IOException ex) {
                    throw new HyracksDataException(ex);
                }
            }

            @Override
            public void initFromPartial(IFrameTupleAccessor accessor, int tIndex, int fIndex)
                    throws HyracksDataException {
                sum = IntegerSerializerDeserializer.getInt(
                        accessor.getBuffer().array(),
                        accessor.getTupleStartOffset(tIndex) + accessor.getFieldCount() * 4
                                + accessor.getFieldStartOffset(tIndex, fIndex));
            }

            @Override
            public void init(IFrameTupleAccessor accessor, int tIndex) throws HyracksDataException {
                sum = 0;
            }

            @Override
            public void accumulatePartialResult(IFrameTupleAccessor accessor, int tIndex, int fIndex)
                    throws HyracksDataException {
                sum += IntegerSerializerDeserializer.getInt(
                        accessor.getBuffer().array(),
                        accessor.getTupleStartOffset(tIndex) + accessor.getFieldCount() * 4
                                + accessor.getFieldStartOffset(tIndex, fIndex));
            }

            @Override
            public void accumulate(IFrameTupleAccessor accessor, int tIndex) throws HyracksDataException {
                int tupleOffset = accessor.getTupleStartOffset(tIndex);
                int fieldCount = accessor.getFieldCount();
                int fieldStart = accessor.getFieldStartOffset(tIndex, sumField);
                sum += IntegerSerializerDeserializer.getInt(accessor.getBuffer().array(), tupleOffset + 2 * fieldCount
                        + fieldStart);
            }
        };
    }

}
