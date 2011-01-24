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

public class CountAggregatorFactory implements IFieldValueResultingAggregatorFactory {
    private static final long serialVersionUID = 1L;

    @Override
    public IFieldValueResultingAggregator createFieldValueResultingAggregator() {
        return new IFieldValueResultingAggregator() {
            private int count;

            @Override
            public void output(DataOutput resultAcceptor) throws HyracksDataException {
                try {
                    resultAcceptor.writeInt(count);
                } catch (IOException e) {
                    throw new HyracksDataException(e);
                }
            }

            @Override
            public void init(IFrameTupleAccessor accessor, int tIndex) throws HyracksDataException {
                count = 0;
            }

            @Override
            public void accumulate(IFrameTupleAccessor accessor, int tIndex) throws HyracksDataException {
                ++count;
            }
        };
    }

	@Override
	public ISpillableFieldValueResultingAggregator createSpillableFieldValueResultingAggregator() {
		return new ISpillableFieldValueResultingAggregator() {
			private int count;

			@Override
			public void output(DataOutput resultAcceptor)
					throws HyracksDataException {
				try {
					resultAcceptor.writeInt(count);
				} catch (IOException e) {
					throw new HyracksDataException(e);
				}
			}

			@Override
			public void initFromPartial(IFrameTupleAccessor accessor, int tIndex, int fIndex)
					throws HyracksDataException {
				count = IntegerSerializerDeserializer.getInt(accessor.getBuffer().array(), accessor.getTupleStartOffset(tIndex) + accessor.getFieldCount() * 2 + accessor.getFieldStartOffset(tIndex, fIndex));

			}

			@Override
			public void accumulate(IFrameTupleAccessor accessor, int tIndex)
					throws HyracksDataException {
				count++;
			}

			@Override
			public void init(IFrameTupleAccessor accessor, int tIndex) throws HyracksDataException {
				count = 0;
			}

			@Override
			public void accumulatePartialResult(IFrameTupleAccessor accessor,
					int tIndex, int fIndex) throws HyracksDataException {
				count += IntegerSerializerDeserializer.getInt(accessor.getBuffer().array(), accessor.getTupleStartOffset(tIndex) + accessor.getFieldCount() * 2 + accessor.getFieldStartOffset(tIndex, fIndex));
			}
		};
	}
}