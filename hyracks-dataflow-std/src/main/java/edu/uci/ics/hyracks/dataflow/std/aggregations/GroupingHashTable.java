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
package edu.uci.ics.hyracks.dataflow.std.aggregations;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import edu.uci.ics.hyracks.api.comm.IFrameWriter;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparator;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.api.dataflow.value.ITuplePartitionComputer;
import edu.uci.ics.hyracks.api.dataflow.value.ITuplePartitionComputerFactory;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTuplePairComparator;

class GroupingHashTable {
	/**
	 * The pointers in the link store 3 int values for each entry in the
	 * hashtable: (bufferIdx, tIndex, accumulatorIdx).
	 * 
	 * @author vinayakb
	 */
	private static class Link {
		private static final int INIT_POINTERS_SIZE = 9;

		int[] pointers;
		int size;

		Link() {
			pointers = new int[INIT_POINTERS_SIZE];
			size = 0;
		}

		void add(int bufferIdx, int tIndex, int accumulatorIdx) {
			while (size + 3 > pointers.length) {
				pointers = Arrays.copyOf(pointers, pointers.length * 2);
			}
			pointers[size++] = bufferIdx;
			pointers[size++] = tIndex;
			pointers[size++] = accumulatorIdx;
		}
	}

	private static final int INIT_ACCUMULATORS_SIZE = 8;
	private final IHyracksTaskContext ctx;
	private final FrameTupleAppender appender;
	private final List<ByteBuffer> buffers;
	private final Link[] table;
	private AggregateState[][] aggregateStates;
	private int accumulatorSize;

	private int lastBIndex;
	private final int[] fields;
	private final int[] storedKeys;
	private final IBinaryComparator[] comparators;
	private final FrameTuplePairComparator ftpc;
	private final ITuplePartitionComputer tpc;
	private final IFieldAggregateDescriptor[] aggregators;
	private final RecordDescriptor inRecordDescriptor;
	private final RecordDescriptor outRecordDescriptor;

	private final FrameTupleAccessor storedKeysAccessor;

	GroupingHashTable(IHyracksTaskContext ctx, int[] fields,
			IBinaryComparatorFactory[] comparatorFactories,
			ITuplePartitionComputerFactory tpcf,
			IFieldAggregateDescriptorFactory[] aggregatorFactories,
			RecordDescriptor inRecordDescriptor,
			RecordDescriptor outRecordDescriptor, int tableSize)
			throws HyracksDataException {
		this.ctx = ctx;
		appender = new FrameTupleAppender(ctx.getFrameSize());
		buffers = new ArrayList<ByteBuffer>();
		table = new Link[tableSize];
		
		this.aggregateStates = new AggregateState[aggregatorFactories.length][INIT_ACCUMULATORS_SIZE];
		accumulatorSize = 0;
		
		this.fields = fields;
		storedKeys = new int[fields.length];
		@SuppressWarnings("rawtypes")
		ISerializerDeserializer[] storedKeySerDeser = new ISerializerDeserializer[fields.length];
		for (int i = 0; i < fields.length; ++i) {
			storedKeys[i] = i;
			storedKeySerDeser[i] = inRecordDescriptor.getFields()[fields[i]];
		}
		
		comparators = new IBinaryComparator[comparatorFactories.length];
		for (int i = 0; i < comparatorFactories.length; ++i) {
			comparators[i] = comparatorFactories[i].createBinaryComparator();
		}
		ftpc = new FrameTuplePairComparator(fields, storedKeys, comparators);
		tpc = tpcf.createPartitioner();
		
		this.inRecordDescriptor = inRecordDescriptor;
		this.outRecordDescriptor = outRecordDescriptor;
		
		this.aggregators = new IFieldAggregateDescriptor[aggregatorFactories.length];
		for (int i = 0; i < aggregatorFactories.length; i++) {
			this.aggregators[i] = aggregatorFactories[i].createAggregator(ctx,
					this.inRecordDescriptor, this.outRecordDescriptor);
		}
		RecordDescriptor storedKeysRecordDescriptor = new RecordDescriptor(
				storedKeySerDeser);
		storedKeysAccessor = new FrameTupleAccessor(ctx.getFrameSize(),
				storedKeysRecordDescriptor);
		lastBIndex = -1;
		addNewBuffer();
	}

	private void addNewBuffer() {
		ByteBuffer buffer = ctx.allocateFrame();
		buffer.position(0);
		buffer.limit(buffer.capacity());
		buffers.add(buffer);
		appender.reset(buffer, true);
		++lastBIndex;
	}

	private void flushFrame(FrameTupleAppender appender, IFrameWriter writer)
			throws HyracksDataException {
		ByteBuffer frame = appender.getBuffer();
		frame.position(0);
		frame.limit(frame.capacity());
		writer.nextFrame(appender.getBuffer());
		appender.reset(appender.getBuffer(), true);
	}

	void insert(FrameTupleAccessor accessor, int tIndex)
			throws Exception {
		int entry = tpc.partition(accessor, tIndex, table.length);
		Link link = table[entry];
		if (link == null) {
			link = table[entry] = new Link();
		}
		int saIndex = -1;
		for (int i = 0; i < link.size; i += 3) {
			int sbIndex = link.pointers[i];
			int stIndex = link.pointers[i + 1];
			storedKeysAccessor.reset(buffers.get(sbIndex));
			int c = ftpc.compare(accessor, tIndex, storedKeysAccessor, stIndex);
			if (c == 0) {
				saIndex = link.pointers[i + 2];
				break;
			}
		}
		if (saIndex < 0) {
			// Did not find the key. Insert a new entry.
			saIndex = accumulatorSize++;
			if (!appender.appendProjection(accessor, tIndex, fields)) {
				addNewBuffer();
				if (!appender.appendProjection(accessor, tIndex, fields)) {
					throw new IllegalStateException();
				}
			}
			int sbIndex = lastBIndex;
			int stIndex = appender.getTupleCount() - 1;
			for (int i = 0; i < aggregators.length; i++) {
				AggregateState aggState = aggregators[i].createState();
				aggregators[i].init(accessor, tIndex, null, aggState);
				if (saIndex >= aggregateStates[i].length) {
					aggregateStates[i] = Arrays.copyOf(aggregateStates[i],
							aggregateStates[i].length * 2);
				}
				aggregateStates[i][saIndex] = aggState;
			}
			link.add(sbIndex, stIndex, saIndex);
		} else {
			for (int i = 0; i < aggregators.length; i++) {
				aggregators[i].aggregate(accessor, tIndex, null, 0,
						aggregateStates[i][saIndex]);
			}
		}
	}

	void write(IFrameWriter writer) throws HyracksDataException {
		ByteBuffer buffer = ctx.allocateFrame();
		appender.reset(buffer, true);
		ArrayTupleBuilder tupleBuilder = new ArrayTupleBuilder(
				outRecordDescriptor.getFields().length);
		for (int i = 0; i < table.length; ++i) {
			Link link = table[i];
			if (link != null) {
				for (int j = 0; j < link.size; j += 3) {
					int bIndex = link.pointers[j];
					int tIndex = link.pointers[j + 1];
					int aIndex = link.pointers[j + 2];
					ByteBuffer keyBuffer = buffers.get(bIndex);
					storedKeysAccessor.reset(keyBuffer);

					tupleBuilder.reset();
					for (int k : this.fields) {
						tupleBuilder.addField(storedKeysAccessor, tIndex, k);
					}
					for (int k = 0; k < aggregators.length; k++) {
						aggregators[k].outputFinalResult(
								tupleBuilder.getDataOutput(), null, 0,
								aggregateStates[k][aIndex]);
						tupleBuilder.addFieldEndOffset();
					}
					while (!appender.append(tupleBuilder.getFieldEndOffsets(),
							tupleBuilder.getByteArray(), 0,
							tupleBuilder.getSize())) {
						flushFrame(appender, writer);
					}
				}
			}
		}
		if (appender.getTupleCount() != 0) {
			flushFrame(appender, writer);
		}
	}
}