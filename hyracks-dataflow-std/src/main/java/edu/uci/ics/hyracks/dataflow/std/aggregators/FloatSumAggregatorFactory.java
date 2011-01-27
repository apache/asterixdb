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
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.FloatSerializerDeserializer;

/**
 * SUM aggregator on float type data.
 * 
 * @author jarodwen
 */
public class FloatSumAggregatorFactory implements IFieldValueResultingAggregatorFactory {

    /**
	 * 
	 */
    private static final long serialVersionUID = 1L;
    private int sumField;

    public FloatSumAggregatorFactory(int field) {
        this.sumField = field;
    }

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

            private float sum;

            @Override
            public void output(DataOutput resultAcceptor) throws HyracksDataException {
                try {
                    resultAcceptor.writeFloat(sum);
                } catch (IOException ex) {
                    throw new HyracksDataException(ex);
                }
            }

            @Override
            public void init(IFrameTupleAccessor accessor, int tIndex) throws HyracksDataException {
                sum = 0;
            }

            @Override
            public void accumulate(IFrameTupleAccessor accessor, int tIndex) throws HyracksDataException {
                int tupleOffset = accessor.getTupleStartOffset(tIndex);
                int fieldCount = accessor.getFieldCount();
                int fieldStart = accessor.getFieldStartOffset(tIndex, sumField);
                sum += FloatSerializerDeserializer.getFloat(accessor.getBuffer().array(), tupleOffset + 2 * fieldCount
                        + fieldStart);
            }
        };
    }

    /*
     * (non-Javadoc)
     * 
     * @see edu.uci.ics.hyracks.dataflow.std.aggregators.
     * IFieldValueResultingAggregatorFactory
     * #createSpillableFieldValueResultingAggregator()
     */
    @Override
    public ISpillableFieldValueResultingAggregator createSpillableFieldValueResultingAggregator() {
        return new ISpillableFieldValueResultingAggregator() {

            private float sum;

            @Override
            public void output(DataOutput resultAcceptor) throws HyracksDataException {
                try {
                    resultAcceptor.writeFloat(sum);
                } catch (IOException ex) {
                    throw new HyracksDataException(ex);
                }
            }

            @Override
            public void initFromPartial(IFrameTupleAccessor accessor, int tIndex, int fIndex)
                    throws HyracksDataException {
                sum = FloatSerializerDeserializer.getFloat(
                        accessor.getBuffer().array(),
                        accessor.getTupleStartOffset(tIndex) + accessor.getFieldCount() * 2
                                + accessor.getFieldStartOffset(tIndex, fIndex));
            }

            @Override
            public void init(IFrameTupleAccessor accessor, int tIndex) throws HyracksDataException {
                sum = 0;
            }

            @Override
            public void accumulatePartialResult(IFrameTupleAccessor accessor, int tIndex, int fIndex)
                    throws HyracksDataException {
                sum += FloatSerializerDeserializer.getFloat(
                        accessor.getBuffer().array(),
                        accessor.getTupleStartOffset(tIndex) + accessor.getFieldCount() * 2
                                + accessor.getFieldStartOffset(tIndex, fIndex));
            }

            @Override
            public void accumulate(IFrameTupleAccessor accessor, int tIndex) throws HyracksDataException {
                int tupleOffset = accessor.getTupleStartOffset(tIndex);
                int fieldCount = accessor.getFieldCount();
                int fieldStart = accessor.getFieldStartOffset(tIndex, sumField);
                sum += FloatSerializerDeserializer.getFloat(accessor.getBuffer().array(), tupleOffset + 2 * fieldCount
                        + fieldStart);
            }
        };
    }

}
