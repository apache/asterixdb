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

import edu.uci.ics.hyracks.api.comm.IFrameTupleAccessor;
import edu.uci.ics.hyracks.api.context.IHyracksStageletContext;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import edu.uci.ics.hyracks.dataflow.std.group.IAccumulatingAggregator;
import edu.uci.ics.hyracks.dataflow.std.group.IAccumulatingAggregatorFactory;
import edu.uci.ics.hyracks.dataflow.std.group.ISpillableAccumulatingAggregator;

public class MultiAggregatorFactory implements IAccumulatingAggregatorFactory {
	private static final long serialVersionUID = 1L;

	private IFieldValueResultingAggregatorFactory[] aFactories;

	public MultiAggregatorFactory(
			IFieldValueResultingAggregatorFactory[] aFactories) {
		this.aFactories = aFactories;
	}

	@Override
	public ISpillableAccumulatingAggregator createSpillableAggregator(
			IHyracksStageletContext ctx, RecordDescriptor inRecordDesc,
			final RecordDescriptor outRecordDescriptor) {
		final ISpillableFieldValueResultingAggregator aggregators[] = new ISpillableFieldValueResultingAggregator[aFactories.length];
		for (int i = 0; i < aFactories.length; ++i) {
			aggregators[i] = aFactories[i]
					.createSpillableFieldValueResultingAggregator();
		}
		final ArrayTupleBuilder tb = new ArrayTupleBuilder(
				outRecordDescriptor.getFields().length);
		return new ISpillableAccumulatingAggregator() {
			private boolean pending;

			@Override
			public boolean output(FrameTupleAppender appender,
					IFrameTupleAccessor accessor, int tIndex,
					int[] keyFieldIndexes) throws HyracksDataException {
				if (!pending) {
					tb.reset();
					for (int i = 0; i < keyFieldIndexes.length; ++i) {
						tb.addField(accessor, tIndex, keyFieldIndexes[i]);
					}
					DataOutput dos = tb.getDataOutput();
					for (int i = 0; i < aggregators.length; ++i) {
						aggregators[i].output(dos);
						tb.addFieldEndOffset();
					}
				}
				if (!appender.append(tb.getFieldEndOffsets(),
						tb.getByteArray(), 0, tb.getSize())) {
					pending = true;
					return false;
				}
				return true;
			}

			@Override
			public void init(IFrameTupleAccessor accessor, int tIndex)
					throws HyracksDataException {
				tb.reset();
				for (int i = 0; i < aggregators.length; ++i) {
					aggregators[i].init(accessor, tIndex);
				}
				pending = false;
			}

			@Override
			public void accumulate(IFrameTupleAccessor accessor, int tIndex)
					throws HyracksDataException {
				for (int i = 0; i < aggregators.length; ++i) {
					aggregators[i].accumulate(accessor, tIndex);
				}
			}

			@Override
			public void initFromPartial(IFrameTupleAccessor accessor,
					int tIndex, int[] keyFieldIndexes)
					throws HyracksDataException {
				tb.reset();
				for (int i = 0; i < aggregators.length; i++) {
					aggregators[i].initFromPartial(accessor, tIndex,
							keyFieldIndexes.length + i);
				}
				pending = false;
			}

			@Override
			public void accumulatePartialResult(IFrameTupleAccessor accessor,
					int tIndex, int[] keyFieldIndexes)
					throws HyracksDataException {
				for (int i = 0; i < aggregators.length; i++) {
					aggregators[i].accumulatePartialResult(accessor, tIndex,
							keyFieldIndexes.length + i);
				}

			}

			@Override
			public boolean output(FrameTupleAppender appender,
					ArrayTupleBuilder tbder) throws HyracksDataException {
				if (!pending) {
					// TODO Here to be fixed:
					DataOutput dos = tbder.getDataOutput();
					for (int i = 0; i < aggregators.length; ++i) {
						aggregators[i].output(dos);
						tbder.addFieldEndOffset();
					}
				}
				if (!appender.append(tbder.getFieldEndOffsets(),
						tbder.getByteArray(), 0, tbder.getSize())) {
					pending = true;
					return false;
				}
				return true;
			}
		};
	}

	@Override
	public IAccumulatingAggregator createAggregator(
			IHyracksStageletContext ctx, RecordDescriptor inRecordDesc,
			RecordDescriptor outRecordDescriptor) throws HyracksDataException {
		final IFieldValueResultingAggregator aggregators[] = new IFieldValueResultingAggregator[aFactories.length];
		for (int i = 0; i < aFactories.length; ++i) {
			aggregators[i] = aFactories[i]
					.createFieldValueResultingAggregator();
		}
		final ArrayTupleBuilder tb = new ArrayTupleBuilder(
				outRecordDescriptor.getFields().length);
		return new IAccumulatingAggregator() {

			private boolean pending;

			@Override
			public void init(IFrameTupleAccessor accessor, int tIndex)
					throws HyracksDataException {
				tb.reset();
				for (int i = 0; i < aggregators.length; ++i) {
					aggregators[i].init(accessor, tIndex);
				}
				pending = false;
			}

			@Override
			public void accumulate(IFrameTupleAccessor accessor, int tIndex)
					throws HyracksDataException {
				for (int i = 0; i < aggregators.length; ++i) {
					aggregators[i].accumulate(accessor, tIndex);
				}
			}

			@Override
			public boolean output(FrameTupleAppender appender,
					IFrameTupleAccessor accessor, int tIndex,
					int[] keyFieldIndexes) throws HyracksDataException {
				if (!pending) {
					tb.reset();
					for (int i = 0; i < keyFieldIndexes.length; ++i) {
						tb.addField(accessor, tIndex, keyFieldIndexes[i]);
					}
					DataOutput dos = tb.getDataOutput();
					for (int i = 0; i < aggregators.length; ++i) {
						aggregators[i].output(dos);
						tb.addFieldEndOffset();
					}
				}
				if (!appender.append(tb.getFieldEndOffsets(),
						tb.getByteArray(), 0, tb.getSize())) {
					pending = true;
					return false;
				}
				return true;
			}

		};
	}
}