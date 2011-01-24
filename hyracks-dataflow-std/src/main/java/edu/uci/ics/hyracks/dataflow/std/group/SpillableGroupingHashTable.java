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
package edu.uci.ics.hyracks.dataflow.std.group;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import edu.uci.ics.hyracks.api.comm.IFrameWriter;
import edu.uci.ics.hyracks.api.context.IHyracksStageletContext;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparator;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.api.dataflow.value.ITuplePartitionComputer;
import edu.uci.ics.hyracks.api.dataflow.value.ITuplePartitionComputerFactory;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTuplePairComparator;
import edu.uci.ics.hyracks.dataflow.common.comm.util.FrameUtils;

/**
 * An in-mem hash table for spillable grouping operations. 
 * 
 * A table of {@link #Link}s are maintained in this object, and each row
 * of this table represents a hash partition. 
 * 
 * 
 */
public class SpillableGroupingHashTable {

	/**
	 * Context.
	 */
	private final IHyracksStageletContext ctx;

	/**
	 * Columns for group-by
	 */
	private final int[] fields;

	/**
	 * Key fields of records in the hash table (starting from 0
	 * to the number of the key fields). 
	 * 
	 * This is different from the key fields in the input records, 
	 * since these fields are extracted when being inserted into
	 * the hash table.
	 */
	private final int[] storedKeys;

	/**
	 * Comparators: one for each column in {@link #groupFields}
	 */
	private final IBinaryComparator[] comparators;

	/**
	 * Record descriptor for the input tuple.
	 */
	private final RecordDescriptor inRecordDescriptor;

	/**
	 * Record descriptor for the partial aggregation result.
	 */
	private final RecordDescriptor outputRecordDescriptor;

	/**
	 * Accumulators in the main memory.
	 */
	private ISpillableAccumulatingAggregator[] accumulators;

	/**
	 * The hashing group table containing pointers to aggregators and also the
	 * corresponding key tuples. So for each entry, there will be three integer
	 * fields:
	 * 
	 * 1. The frame index containing the key tuple; 2. The tuple index inside of
	 * the frame for the key tuple; 3. The index of the aggregator.
	 * 
	 * Note that each link in the table is a partition for the input records. Multiple
	 * records in the same partition based on the {@link #tpc} are stored as
	 * pointers.
	 */
	private final Link[] table;

	/**
	 * Number of accumulators.
	 */
	private int accumulatorSize = 0;

	/**
	 * Factory for the aggregators.
	 */
	private final IAccumulatingAggregatorFactory aggregatorFactory;

	private final List<ByteBuffer> frames;
	
	private final ByteBuffer outFrame;

	/**
	 * Frame appender for output frames in {@link #frames}.
	 */
	private final FrameTupleAppender appender;

	/**
	 * The count of used frames in the table.
	 * 
	 * Note that this cannot be replaced by {@link #frames} since frames will
	 * not be removed after being created.
	 */
	private int dataFrameCount;

	/**
	 * Pointers for the sorted aggregators
	 */
	private int[] tPointers;

	private static final int INIT_ACCUMULATORS_SIZE = 8;

	/**
	 * The maximum number of frames available for this hashing group table.
	 */
	private final int framesLimit;

	private final FrameTuplePairComparator ftpc;

	/**
	 * A partition computer to partition the hashing group table.
	 */
	private final ITuplePartitionComputer tpc;

	/**
	 * Accessors for the tuples. Two accessors are necessary during the sort.
	 */
	private final FrameTupleAccessor storedKeysAccessor1;
	private final FrameTupleAccessor storedKeysAccessor2;

	/**
	 * Create a spillable grouping hash table. 
	 * @param ctx					The context of the job.
	 * @param fields				Fields of keys for grouping.
	 * @param comparatorFactories	The comparators.
	 * @param tpcf					The partitioners. These are used to partition the incoming records into proper partition of the hash table.
	 * @param aggregatorFactory		The aggregators.
	 * @param inRecordDescriptor	Record descriptor for input data.
	 * @param outputRecordDescriptor	Record descriptor for output data.
	 * @param framesLimit			The maximum number of frames usable in the memory for hash table.
	 * @param tableSize				The size of the table, which specified the number of partitions of the table.
	 */
	public SpillableGroupingHashTable(IHyracksStageletContext ctx, int[] fields,
			IBinaryComparatorFactory[] comparatorFactories,
			ITuplePartitionComputerFactory tpcf,
			IAccumulatingAggregatorFactory aggregatorFactory,
			RecordDescriptor inRecordDescriptor,
			RecordDescriptor outputRecordDescriptor, int framesLimit,
			int tableSize) {
		this.ctx = ctx;
		this.fields = fields;

		storedKeys = new int[fields.length];
		@SuppressWarnings("rawtypes")
		ISerializerDeserializer[] storedKeySerDeser = new ISerializerDeserializer[fields.length];
		
		// Note that after storing a record into the hash table, the index for the fields should
		// be updated. Here we assume that all these key fields are written at the beginning of 
		// the record, so their index should start from 0 and end at the length of the key fields.
		for (int i = 0; i < fields.length; ++i) {
			storedKeys[i] = i;
			storedKeySerDeser[i] = inRecordDescriptor.getFields()[fields[i]];
		}
		RecordDescriptor storedKeysRecordDescriptor = new RecordDescriptor(
				storedKeySerDeser);
		storedKeysAccessor1 = new FrameTupleAccessor(ctx.getFrameSize(),
				storedKeysRecordDescriptor);
		storedKeysAccessor2 = new FrameTupleAccessor(ctx.getFrameSize(),
				storedKeysRecordDescriptor);

		comparators = new IBinaryComparator[comparatorFactories.length];
		for (int i = 0; i < comparatorFactories.length; ++i) {
			comparators[i] = comparatorFactories[i].createBinaryComparator();
		}

		this.table = new Link[tableSize];

		this.aggregatorFactory = aggregatorFactory;
		accumulators = new ISpillableAccumulatingAggregator[INIT_ACCUMULATORS_SIZE];

		this.framesLimit = framesLimit;

		// Tuple pair comparator
		ftpc = new FrameTuplePairComparator(fields, storedKeys, comparators);
		
		// Partitioner
		tpc = tpcf.createPartitioner();

		this.inRecordDescriptor = inRecordDescriptor;
		this.outputRecordDescriptor = outputRecordDescriptor;
		frames = new ArrayList<ByteBuffer>();
		appender = new FrameTupleAppender(ctx.getFrameSize());

		dataFrameCount = -1;
		
		outFrame = ctx.allocateFrame();
	}

	public void reset() {
		dataFrameCount = -1;
		tPointers = null;
		// Reset the grouping hash table
		for (int i = 0; i < table.length; i++) {
			table[i] = new Link();
		}
	}

	public int getFrameCount() {
		return dataFrameCount;
	}

	/**
	 * How to define pointers for the partial aggregation
	 * 
	 * @return
	 */
	public int[] getTPointers() {
		return tPointers;
	}

	/**
	 * Redefine the number of fields in the pointer. 
	 * 
	 * Only two pointers are necessary for external grouping: one is to the
	 * index of the hash table, and the other is to the row index inside of the
	 * hash table.
	 * 
	 * @return
	 */
	public int getPtrFields() {
		return 2;
	}

	public List<ByteBuffer> getFrames() {
		return frames;
	}

	/**
	 * Set the working frame to the next available frame in the
	 * frame list. There are two cases:<br>
	 * 
	 * 1) If the next frame is not initialized, allocate
	 * a new frame.
	 * 
	 * 2) When frames are already created, they are recycled.
	 * 
	 * @return Whether a new frame is added successfully.
	 */
	private boolean nextAvailableFrame() {
		// Return false if the number of frames is equal to the limit.
		if (dataFrameCount + 1 >= framesLimit)
			return false;
		
		if (frames.size() < framesLimit) {
			// Insert a new frame
			ByteBuffer frame = ctx.allocateFrame();
			frame.position(0);
			frame.limit(frame.capacity());
			frames.add(frame);
			appender.reset(frame, true);
			dataFrameCount++;
		} else {
			// Reuse an old frame
			dataFrameCount++;
			ByteBuffer frame = frames.get(dataFrameCount);
			frame.position(0);
			frame.limit(frame.capacity());
			appender.reset(frame, true);
		}
		return true;
	}

	/**
	 * Insert a new record from the input frame.
	 * 
	 * @param accessor
	 * @param tIndex
	 * @return
	 * @throws HyracksDataException
	 */
	public boolean insert(FrameTupleAccessor accessor, int tIndex)
			throws HyracksDataException {
		if (dataFrameCount < 0)
			nextAvailableFrame();
		// Get the partition for the inserting tuple
		int entry = tpc.partition(accessor, tIndex, table.length);
		Link link = table[entry];
		if (link == null) {
			link = table[entry] = new Link();
		}
		// Find the corresponding aggregator from existing aggregators
		ISpillableAccumulatingAggregator aggregator = null;
		for (int i = 0; i < link.size; i += 3) {
			int sbIndex = link.pointers[i];
			int stIndex = link.pointers[i + 1];
			int saIndex = link.pointers[i + 2];
			storedKeysAccessor1.reset(frames.get(sbIndex));
			int c = ftpc
					.compare(accessor, tIndex, storedKeysAccessor1, stIndex);
			if (c == 0) {
				aggregator = accumulators[saIndex];
				break;
			}
		}
		// Do insert
		if (aggregator == null) {
			// Did not find the aggregator. Insert a new aggregator entry
			if (!appender.appendProjection(accessor, tIndex, fields)) {
				if (!nextAvailableFrame()) {
					// If buffer is full, return false to trigger a run file
					// write
					return false;
				} else {
					// Try to do insert after adding a new frame.
					if (!appender.appendProjection(accessor, tIndex, fields)) {
						throw new IllegalStateException();
					}
				}
			}
			int sbIndex = dataFrameCount;
			int stIndex = appender.getTupleCount() - 1;
			if (accumulatorSize >= accumulators.length) {
				accumulators = Arrays.copyOf(accumulators,
						accumulators.length * 2);
			}
			int saIndex = accumulatorSize++;
			aggregator = accumulators[saIndex] = aggregatorFactory
					.createSpillableAggregator(ctx, inRecordDescriptor,
							outputRecordDescriptor);
			aggregator.init(accessor, tIndex);
			link.add(sbIndex, stIndex, saIndex);
		}
		aggregator.accumulate(accessor, tIndex);
		return true;
	}

	/**
	 * Sort partial results
	 */
	public void sortFrames() {
		int totalTCount = 0;
		// Get the number of records
		for (int i = 0; i < table.length; i++) {
			if (table[i] == null)
				continue;
			totalTCount += table[i].size / 3;
		}
		// Start sorting:
		/*
		 * Based on the data structure for the partial aggregates, the
		 * pointers should be initialized.
		 */
		tPointers = new int[totalTCount * getPtrFields()];
		// Initialize pointers
		int ptr = 0;
		// Maintain two pointers to each entry of the hashing group table
		for (int i = 0; i < table.length; i++) {
			if (table[i] == null)
				continue;
			for (int j = 0; j < table[i].size; j = j + 3) {
				tPointers[ptr * getPtrFields()] = i;
				tPointers[ptr * getPtrFields() + 1] = j;
				ptr++;
			}
		}
		// Sort using quick sort
		if (tPointers.length > 0) {
			sort(tPointers, 0, totalTCount);
		}
	}

	/**
	 * 
	 * @param writer
	 * @throws HyracksDataException
	 */
	public void flushFrames(IFrameWriter writer, boolean sorted) throws HyracksDataException {
		FrameTupleAppender appender = new FrameTupleAppender(ctx.getFrameSize());

		ISpillableAccumulatingAggregator aggregator = null;
		writer.open();
		appender.reset(outFrame, true);
		if (sorted){
			sortFrames();
		}
		if(tPointers == null){
			// Not sorted
			for (int i = 0; i < table.length; ++i) {
	            Link link = table[i];
	            if (link != null) {
	                for (int j = 0; j < link.size; j += 3) {
	                    int bIndex = link.pointers[j];
	                    int tIndex = link.pointers[j + 1];
	                    int aIndex = link.pointers[j + 2];
	                    ByteBuffer keyBuffer = frames.get(bIndex);
	                    storedKeysAccessor1.reset(keyBuffer);
	                    aggregator = accumulators[aIndex];
	                    while (!aggregator.output(appender, storedKeysAccessor1, tIndex, storedKeys)) {
	                    	FrameUtils.flushFrame(outFrame, writer);
	                    	appender.reset(outFrame, true);
	                    }
	                }
	            }
	        }
	        if (appender.getTupleCount() != 0) {
	        	FrameUtils.flushFrame(outFrame, writer);
	        }
	        return;
		}
		int n = tPointers.length / getPtrFields();
		for (int ptr = 0; ptr < n; ptr++) {
			int tableIndex = tPointers[ptr * 2];
			int rowIndex = tPointers[ptr * 2 + 1];
			int frameIndex = table[tableIndex].pointers[rowIndex];
			int tupleIndex = table[tableIndex].pointers[rowIndex + 1];
			int aggregatorIndex = table[tableIndex].pointers[rowIndex + 2];
			// Get the frame containing the value
			ByteBuffer buffer = frames.get(frameIndex);
			storedKeysAccessor1.reset(buffer);

			// Get the aggregator
			aggregator = accumulators[aggregatorIndex];
			// Insert
			if (!aggregator.output(appender, storedKeysAccessor1, tupleIndex,
					fields)) {
				FrameUtils.flushFrame(outFrame, writer);
				appender.reset(outFrame, true);
				if (!aggregator.output(appender, storedKeysAccessor1,
						tupleIndex, fields)) {
					throw new IllegalStateException();
				} else {
					accumulators[aggregatorIndex] = null;
				}
			} else {
				accumulators[aggregatorIndex] = null;
			}
		}
		if (appender.getTupleCount() > 0) {
			FrameUtils.flushFrame(outFrame, writer);
		}
	}

	private void sort(int[] tPointers, int offset, int length) {
		int m = offset + (length >> 1);
		// Get table index
		int mTable = tPointers[m * 2];
		int mRow = tPointers[m * 2 + 1];
		// Get frame and tuple index
		int mFrame = table[mTable].pointers[mRow];
		int mTuple = table[mTable].pointers[mRow + 1];
		storedKeysAccessor1.reset(frames.get(mFrame));

		int a = offset;
		int b = a;
		int c = offset + length - 1;
		int d = c;
		while (true) {
			while (b <= c) {
				int bTable = tPointers[b * 2];
				int bRow = tPointers[b * 2 + 1];
				int bFrame = table[bTable].pointers[bRow];
				int bTuple = table[bTable].pointers[bRow + 1];
				storedKeysAccessor2.reset(frames.get(bFrame));
				int cmp = ftpc.compare(storedKeysAccessor2, bTuple,
						storedKeysAccessor1, mTuple);
				// int cmp = compare(tPointers, b, mi, mj, mv);
				if (cmp > 0) {
					break;
				}
				if (cmp == 0) {
					swap(tPointers, a++, b);
				}
				++b;
			}
			while (c >= b) {
				int cTable = tPointers[c * 2];
				int cRow = tPointers[c * 2 + 1];
				int cFrame = table[cTable].pointers[cRow];
				int cTuple = table[cTable].pointers[cRow + 1];
				storedKeysAccessor2.reset(frames.get(cFrame));
				int cmp = ftpc.compare(storedKeysAccessor2, cTuple,
						storedKeysAccessor1, mTuple);
				// int cmp = compare(tPointers, c, mi, mj, mv);
				if (cmp < 0) {
					break;
				}
				if (cmp == 0) {
					swap(tPointers, c, d--);
				}
				--c;
			}
			if (b > c)
				break;
			swap(tPointers, b++, c--);
		}

		int s;
		int n = offset + length;
		s = Math.min(a - offset, b - a);
		vecswap(tPointers, offset, b - s, s);
		s = Math.min(d - c, n - d - 1);
		vecswap(tPointers, b, n - s, s);

		if ((s = b - a) > 1) {
			sort(tPointers, offset, s);
		}
		if ((s = d - c) > 1) {
			sort(tPointers, n - s, s);
		}
	}

	private void swap(int x[], int a, int b) {
		for (int i = 0; i < 2; ++i) {
			int t = x[a * 2 + i];
			x[a * 2 + i] = x[b * 2 + i];
			x[b * 2 + i] = t;
		}
	}

	private void vecswap(int x[], int a, int b, int n) {
		for (int i = 0; i < n; i++, a++, b++) {
			swap(x, a, b);
		}
	}

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

		public String toString() {
			StringBuilder sb = new StringBuilder();
			sb.append("[Size=" + size + "]");
			for (int i = 0; i < pointers.length; i = i + 3) {
				sb.append(pointers[i] + ",");
				sb.append(pointers[i + 1] + ",");
				sb.append(pointers[i + 2] + "; ");
			}
			return sb.toString();
		}
	}
}
