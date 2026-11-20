/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.asterix.column.operation.lsm.merge;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;

import org.apache.asterix.column.metadata.PromotedKeys;
import org.apache.asterix.column.operation.lsm.flush.FlushColumnTupleWriter;
import org.apache.asterix.column.tuple.MergeColumnTupleReference;
import org.apache.asterix.column.util.RunLengthIntArray;
import org.apache.asterix.column.values.IColumnValuesReader;
import org.apache.asterix.column.values.IColumnValuesWriter;
import org.apache.asterix.column.values.writer.ColumnBatchWriter;
import org.apache.asterix.column.zero.PageZeroWriterFlavorSelector;
import org.apache.asterix.column.zero.writers.DefaultColumnPageZeroWriter;
import org.apache.asterix.column.zero.writers.SparseColumnPageZeroWriter;
import org.apache.asterix.column.zero.writers.multipage.DefaultColumnMultiPageZeroWriter;
import org.apache.asterix.column.zero.writers.multipage.SparseColumnMultiPageZeroWriter;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IValueReference;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.storage.am.lsm.btree.column.api.AbstractColumnTupleWriter;
import org.apache.hyracks.storage.am.lsm.btree.column.api.IColumnTupleIterator;
import org.apache.hyracks.storage.am.lsm.btree.column.api.IColumnWriteMultiPageOp;
import org.apache.hyracks.storage.am.lsm.btree.column.cloud.buffercache.IColumnWriteContext;
import org.apache.hyracks.storage.am.lsm.btree.column.error.ColumnarValueException;
import org.apache.hyracks.storage.am.lsm.btree.column.impls.btree.IColumnPageZeroWriter;
import org.apache.hyracks.storage.am.lsm.btree.column.impls.btree.IColumnPageZeroWriterFlavorSelector;

import com.fasterxml.jackson.databind.node.ObjectNode;

import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntLinkedOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.ints.IntSet;
import it.unimi.dsi.fastutil.objects.Object2IntMap;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;

public class MergeColumnTupleWriter extends AbstractColumnTupleWriter {
    private final MergeColumnWriteMetadata columnMetadata;
    private final int maxLeafNodeSize;
    private final MergeColumnTupleReference[] componentsTuples;
    private final RunLengthIntArray writtenComponents;

    private final IColumnValuesWriter[] primaryKeyWriters;
    private final PriorityQueue<IColumnValuesWriter> orderedColumns;
    private final ColumnBatchWriter writer;
    private final IColumnPageZeroWriterFlavorSelector pageZeroWriterFlavorSelector;
    protected final BitSet presentColumnsIndexes;
    private final int maxNumberOfTuples;
    private int primaryKeysEstimatedSize;
    private int numberOfAntiMatter;
    /** Column index -> index of the movable key it holds, or -1. */
    private final int[] movableKeyOfColumn;
    /** [input][movable key]: whether that input may hold the key in its map, so the merge must fill from it. */
    private final boolean[][] fillFromMap;
    /** Per input: which keys and readers to use when filling from its maps. */
    private final InputFill[] inputFills;
    /** Node (by map key column) -> key name -> movable key index. */
    private final Int2ObjectMap<Object2IntMap<ByteBuffer>> movableKeyOfName = new Int2ObjectOpenHashMap<>();
    /** Nodes already filled in the current run, by map key column. */
    private final BitSet filledNodes = new BitSet();
    /** Per movable key: whether the current record's value is already written. */
    private final boolean[] keyWritten;

    public MergeColumnTupleWriter(MergeColumnWriteMetadata columnMetadata, int pageSize, int maxNumberOfTuples,
            double tolerance, int maxLeafNodeSize, IColumnWriteContext writeContext) {
        this.columnMetadata = columnMetadata;
        this.pageZeroWriterFlavorSelector = new PageZeroWriterFlavorSelector();
        this.maxLeafNodeSize = maxLeafNodeSize;
        this.presentColumnsIndexes = new BitSet();
        int numberOfColumns = columnMetadata.getNumberOfColumns();
        presentColumnsIndexes.set(0, numberOfColumns);
        List<IColumnTupleIterator> componentsTuplesList = columnMetadata.getComponentsTuples();
        this.componentsTuples = new MergeColumnTupleReference[componentsTuplesList.size()];
        int totalLength = 0;
        int totalNumberOfTuples = 0;
        for (int i = 0; i < componentsTuplesList.size(); i++) {
            MergeColumnTupleReference mergeTuple = (MergeColumnTupleReference) componentsTuplesList.get(i);
            this.componentsTuples[i] = mergeTuple;
            mergeTuple.registerEndOfPageCallBack(this::writeAllColumns);
            totalNumberOfTuples += mergeTuple.getTupleCount();
            totalLength += mergeTuple.getMergingLength();
        }
        this.maxNumberOfTuples = getMaxNumberOfTuples(maxNumberOfTuples, totalNumberOfTuples, totalLength);
        this.writtenComponents = new RunLengthIntArray();
        writer = new ColumnBatchWriter(columnMetadata.getMultiPageOpRef(), pageSize, tolerance, writeContext);
        writtenComponents.reset();
        primaryKeyWriters = new IColumnValuesWriter[columnMetadata.getNumberOfPrimaryKeys()];
        for (int i = 0; i < primaryKeyWriters.length; i++) {
            primaryKeyWriters[i] = columnMetadata.getWriter(i);
        }
        orderedColumns = new PriorityQueue<>(Comparator.comparingInt(x -> -x.getEstimatedSize()));
        numberOfAntiMatter = 0;
        List<PromotedKeys.Key> movableKeys = columnMetadata.getMovableKeys();
        movableKeyOfColumn = new int[numberOfColumns];
        Arrays.fill(movableKeyOfColumn, -1);
        for (int k = 0; k < movableKeys.size(); k++) {
            movableKeyOfColumn[movableKeys.get(k).column] = k;
        }
        fillFromMap = new boolean[componentsTuples.length][movableKeys.size()];
        inputFills = new InputFill[componentsTuples.length];
        for (int c = 0; c < componentsTuples.length; c++) {
            for (int k = 0; k < movableKeys.size(); k++) {
                fillFromMap[c][k] = columnMetadata.mayHoldInMap(c, movableKeys.get(k));
            }
            inputFills[c] = new InputFill(componentsTuples[c], movableKeys, fillFromMap[c]);
        }
        for (int k = 0; k < movableKeys.size(); k++) {
            PromotedKeys.Key key = movableKeys.get(k);
            IValueReference name = key.name;
            movableKeyOfName.computeIfAbsent(key.node.mapKeyColumn, n -> {
                Object2IntMap<ByteBuffer> keys = new Object2IntOpenHashMap<>();
                keys.defaultReturnValue(-1);
                return keys;
            }).put(ByteBuffer.wrap(Arrays.copyOfRange(name.getByteArray(), name.getStartOffset(),
                    name.getStartOffset() + name.getLength())), k);
        }
        keyWritten = new boolean[movableKeys.size()];
    }

    @Override
    public int bytesRequired(ITupleReference tuple) {
        int primaryKeysSize = 0;
        for (int i = 0; i < columnMetadata.getNumberOfPrimaryKeys(); i++) {
            primaryKeysSize += primaryKeyWriters[i].getEstimatedSize(tuple.getFieldLength(i));
        }

        return primaryKeysSize;
    }

    @Override
    public void init(IColumnWriteMultiPageOp multiPageOp) throws HyracksDataException {
        columnMetadata.init(multiPageOp);
    }

    @Override
    public int getAbsoluteNumberOfColumns(boolean includeCurrentTupleColumns) {
        return columnMetadata.getNumberOfColumns();
    }

    @Override
    public int getMaxNumberOfTuples() {
        return maxNumberOfTuples;
    }

    @Override
    public int getPrimaryKeysEstimatedSize() {
        return primaryKeysEstimatedSize;
    }

    @Override
    public void writeTuple(ITupleReference tuple) throws HyracksDataException {
        MergeColumnTupleReference columnTuple = (MergeColumnTupleReference) tuple;
        int componentIndex = columnTuple.getComponentIndex();
        int skipCount = columnTuple.getAndResetSkipCount();
        if (skipCount > 0) {
            writtenComponents.add(setAntimatterIndicator(componentIndex), skipCount);
        }

        if (!columnTuple.isAntimatter()) {
            // anti matters contain only the primary keys, and none of the other columns
            writtenComponents.add(componentIndex);
        } else {
            // counter for logging purposes
            numberOfAntiMatter++;
        }

        writePrimaryKeys(columnTuple);
    }

    @Override
    public IColumnPageZeroWriterFlavorSelector getColumnPageZeroWriterFlavorSelector() {
        return pageZeroWriterFlavorSelector;
    }

    @Override
    public int getPageZeroWriterOccupiedSpace(int maxColumnsInPageZerothSegment, int bufferCapacity,
            boolean includeCurrentTupleColumns, IColumnPageZeroWriter.ColumnPageZeroWriterType writerType) {
        int spaceOccupiedByDefaultWriter;
        int spaceOccupiedBySparseWriter;

        if (writerType == IColumnPageZeroWriter.ColumnPageZeroWriterType.DEFAULT) {
            // go for default multi-page writer
            spaceOccupiedByDefaultWriter =
                    getSpaceOccupiedByDefaultWriter(maxColumnsInPageZerothSegment, includeCurrentTupleColumns);
            return spaceOccupiedByDefaultWriter;
        } else if (writerType == IColumnPageZeroWriter.ColumnPageZeroWriterType.SPARSE) {
            // Maximum space occupied by the columns = maxColumnsInPageZerothSegment * (offset + filter size)
            spaceOccupiedBySparseWriter = getSpaceOccupiedBySparseWriter(maxColumnsInPageZerothSegment, bufferCapacity);
            return spaceOccupiedBySparseWriter;
        }

        spaceOccupiedBySparseWriter = getSpaceOccupiedBySparseWriter(maxColumnsInPageZerothSegment, bufferCapacity);
        spaceOccupiedByDefaultWriter =
                getSpaceOccupiedByDefaultWriter(maxColumnsInPageZerothSegment, includeCurrentTupleColumns);
        pageZeroWriterFlavorSelector.switchPageZeroWriterIfNeeded(spaceOccupiedByDefaultWriter,
                spaceOccupiedBySparseWriter);
        return Math.min(spaceOccupiedBySparseWriter, spaceOccupiedByDefaultWriter);
    }

    private int getSpaceOccupiedByDefaultWriter(int maxColumnsInPageZerothSegment, boolean includeCurrentTupleColumns) {
        int spaceOccupiedByDefaultWriter;
        int totalNumberOfColumns = getAbsoluteNumberOfColumns(includeCurrentTupleColumns);
        totalNumberOfColumns = Math.min(totalNumberOfColumns, maxColumnsInPageZerothSegment);
        spaceOccupiedByDefaultWriter = DefaultColumnMultiPageZeroWriter.EXTENDED_HEADER_SIZE + totalNumberOfColumns
                * (DefaultColumnPageZeroWriter.COLUMN_OFFSET_SIZE + DefaultColumnPageZeroWriter.FILTER_SIZE);
        return spaceOccupiedByDefaultWriter;
    }

    private int getSpaceOccupiedBySparseWriter(int maxColumnsInPageZerothSegment, int bufferCapacity) {
        int numberOfColumns = columnMetadata.getNumberOfColumns();
        int maximumNumberOfColumnsInASegment =
                SparseColumnMultiPageZeroWriter.getMaximumNumberOfColumnsInAPage(bufferCapacity);
        int numberOfExtraPagesRequired = numberOfColumns <= maxColumnsInPageZerothSegment ? 0
                : (int) Math.ceil(
                        (double) (numberOfColumns - maxColumnsInPageZerothSegment) / maximumNumberOfColumnsInASegment);
        int headerSpace = SparseColumnMultiPageZeroWriter.getHeaderSpace(numberOfExtraPagesRequired);
        numberOfColumns = Math.min(numberOfColumns, maxColumnsInPageZerothSegment);

        // space occupied by the sparse writer
        return headerSpace + numberOfColumns
                * (SparseColumnPageZeroWriter.COLUMN_OFFSET_SIZE + DefaultColumnPageZeroWriter.FILTER_SIZE);
    }

    @Override
    public int flush(IColumnPageZeroWriter pageZeroWriter) throws HyracksDataException {
        // here the numberOfColumns is the total number of columns present in the LSM Index (across all disk components)
        // Hence, a merge will fail if union(NumberOfColumns(D1) + NumberOfColumns(D2) + ... + NumberOfColumns(DN)) >
        // pageZero space, and since the merged page contains this many number of columns, the first flush will fail.
        int numberOfColumns = columnMetadata.getNumberOfColumns();
        int numberOfPrimaryKeys = columnMetadata.getNumberOfPrimaryKeys();

        // If writtenComponents is not empty, process non-key columns
        if (writtenComponents.getSize() > 0) {
            writeNonKeyColumns();
            writtenComponents.reset();
        }
        for (int i = numberOfPrimaryKeys; i < numberOfColumns; i++) {
            orderedColumns.add(columnMetadata.getWriter(i));
        }

        // Reset pageZeroWriter based on the writer
        writer.setPageZeroWriter(pageZeroWriter, toIndexArray(presentColumnsIndexes), numberOfColumns);

        // Write primary key columns
        writer.writePrimaryKeyColumns(primaryKeyWriters);

        // Write the other columns and get the total length
        int totalLength = writer.writeColumns(orderedColumns);

        // Reset numberOfAntiMatter (assuming this is part of the logic)
        numberOfAntiMatter = 0;

        return totalLength;
    }

    public static int[] toIndexArray(BitSet bitSet) {
        return FlushColumnTupleWriter.toIndexArray(bitSet);
    }

    @Override
    public byte getWriterFlag() {
        return pageZeroWriterFlavorSelector.getWriterFlag();
    }

    @Override
    public void close() {
        columnMetadata.close();
        writer.close();
    }

    @Override
    public final void abort() {
        // this call will reset the writers, releasing the 0th buffer back to the pool
        columnMetadata.close();
    }

    @Override
    public void reset() {
    }

    private void writePrimaryKeys(MergeColumnTupleReference columnTuple) throws HyracksDataException {
        int primaryKeySize = 0;
        for (int i = 0; i < columnMetadata.getNumberOfPrimaryKeys(); i++) {
            IColumnValuesReader columnReader = columnTuple.getReader(i);
            IColumnValuesWriter columnWriter = primaryKeyWriters[i];
            columnReader.write(columnWriter, false);
            primaryKeySize += columnWriter.getEstimatedSize();
        }
        primaryKeysEstimatedSize = primaryKeySize;
    }

    private void writeNonKeyColumns() throws HyracksDataException {
        for (int i = 0; i < writtenComponents.getNumberOfBlocks(); i++) {
            int componentIndex = writtenComponents.getBlockValue(i);
            if (componentIndex < 0) {
                //Skip writing values of deleted tuples
                componentIndex = clearAntimatterIndicator(componentIndex);
                skipReaders(componentIndex, writtenComponents.getBlockSize(i));
                continue;
            }
            MergeColumnTupleReference componentTuple = componentsTuples[componentIndex];
            int count = writtenComponents.getBlockSize(i);
            filledNodes.clear();
            for (int j = columnMetadata.getNumberOfPrimaryKeys(); j < columnMetadata.getNumberOfColumns(); j++) {
                IColumnValuesReader columnReader = componentTuple.getReader(j);
                IColumnValuesWriter columnWriter = columnMetadata.getWriter(j);
                int k = movableKeyOfColumn[j];
                if (k >= 0 && fillFromMap[componentIndex][k]) {
                    // All of a node's keys are filled together, when its first key is reached.
                    int node = columnMetadata.getMovableKeys().get(k).node.mapKeyColumn;
                    if (!filledNodes.get(node)) {
                        filledNodes.set(node);
                        fillFromMap(node, componentIndex, componentTuple, count);
                    }
                } else {
                    writeColumn(i, componentIndex, columnReader, columnWriter, count);
                }
            }
        }
    }

    private void writeColumn(int blockIndex, int componentIndex, IColumnValuesReader columnReader,
            IColumnValuesWriter columnWriter, int count) throws HyracksDataException {
        try {
            columnReader.write(columnWriter, count);
        } catch (ColumnarValueException e) {
            ObjectNode node = e.createNode(getClass().getSimpleName());
            appendExceptionCommonInfo(node, componentIndex, count);
            node.put("blockIndex", blockIndex);
            throw e;
        }
    }

    /**
     * For {@code count} records of input {@code c}, writes each movable key of {@code node} into its column: the
     * record's own column value if it has one, otherwise its map entry's value. Each record's map entries are read once
     * for all keys.
     * <p>
     * The map entry is left in place; when the record is assembled, the first field with a name wins. A value whose
     * type differs from the column's stays only in the map, and the key is marked as possibly still in the map.
     */
    private void fillFromMap(int node, int c, MergeColumnTupleReference componentTuple, int count)
            throws HyracksDataException {
        List<PromotedKeys.Key> movableKeys = columnMetadata.getMovableKeys();
        int[] nodeKeys = inputFills[c].keysOfNode.get(node);
        IColumnValuesReader[] values = inputFills[c].valuesOfNode.get(node);
        Object2IntMap<ByteBuffer> keyOfName = movableKeyOfName.get(node);
        IColumnValuesReader keys = componentTuple.getLookup(node);
        for (int r = 0; r < count; r++) {
            for (int k : nodeKeys) {
                IColumnValuesReader own = componentTuple.getReader(movableKeys.get(k).column);
                own.next();
                keyWritten[k] = own.isValue() || own.isNull();
                if (keyWritten[k]) {
                    own.write(columnMetadata.getWriter(movableKeys.get(k).column), false);
                }
            }
            // Walk the record's map entries up to the group's end delimiter.
            next(keys, values);
            boolean hadEntries = keys.isRepeatedValue();
            if (hadEntries) {
                while (!keys.isLastDelimiter()) {
                    if (keys.isValue()) {
                        IValueReference name = keys.getBytes();
                        int k = keyOfName
                                .getInt(ByteBuffer.wrap(name.getByteArray(), name.getStartOffset(), name.getLength()));
                        if (k >= 0 && fillFromMap[c][k] && !keyWritten[k]) {
                            fillFromEntry(k, componentTuple);
                        }
                    }
                    next(keys, values);
                }
            }
            for (int k : nodeKeys) {
                if (!keyWritten[k]) {
                    int column = movableKeys.get(k).column;
                    writeMissing(k, componentTuple.getReader(column), keys, hadEntries,
                            columnMetadata.getWriter(column));
                }
            }
        }
    }

    /** Writes key {@code k}'s value from the current map entry, if its type matches the column. */
    private void fillFromEntry(int k, MergeColumnTupleReference componentTuple) throws HyracksDataException {
        PromotedKeys.Key key = columnMetadata.getMovableKeys().get(k);
        IColumnValuesReader own = componentTuple.getReader(key.column);
        IColumnValuesReader value = componentTuple.getLookup(key.valueColumn);
        IColumnValuesWriter writer = columnMetadata.getWriter(key.column);
        if (value.isValue()) {
            writer.writeLevel(own.getMaxLevel());
            writer.writeValue(value);
            keyWritten[k] = true;
        } else if (value.isNull()) {
            // Write null as the flush does: at the parent's level with the null bit, not at the column's own level.
            writer.writeNull(own.getMaxLevel() - 1);
            keyWritten[k] = true;
        } else {
            columnMetadata.leftInMap(k);
        }
    }

    private static void next(IColumnValuesReader keys, IColumnValuesReader[] values) throws HyracksDataException {
        keys.next();
        for (IColumnValuesReader value : values) {
            value.next();
        }
    }

    /**
     * Writes "missing" for key {@code k} at the right level, so a reader of this column alone can tell a null parent
     * from a missing one. The level comes from the key's own column or, if that is absent from the page, from the
     * map's key column (both sit under the same node). If both are absent the level is unknown, and the key is marked
     * as possibly still in the map.
     */
    private void writeMissing(int k, IColumnValuesReader own, IColumnValuesReader keys, boolean hadEntries,
            IColumnValuesWriter writer) throws HyracksDataException {
        if (!own.isAbsent()) {
            own.write(writer, false);
        } else if (keys.isAbsent()) {
            own.write(writer, false);
            columnMetadata.leftInMap(k);
        } else if (hadEntries) {
            // The record has map entries, so the node exists; the key is missing right under it.
            writer.writeLevel(own.getMaxLevel() - 1);
        } else if (keys.isNull()) {
            writer.writeNull(keys.getLevel());
        } else {
            writer.writeLevel(keys.getLevel());
        }
    }

    private void skipReaders(int componentIndex, int count) throws HyracksDataException {
        MergeColumnTupleReference componentTuple = componentsTuples[componentIndex];
        try {
            for (int j = columnMetadata.getNumberOfPrimaryKeys(); j < columnMetadata.getNumberOfColumns(); j++) {
                IColumnValuesReader columnReader = componentTuple.getReader(j);
                columnReader.skip(count);
            }
            // Advance each lookup once; a node's keys share them.
            for (int column : inputFills[componentIndex].lookupColumns) {
                componentTuple.getLookup(column).skip(count);
            }
        } catch (ColumnarValueException e) {
            ObjectNode node = e.createNode(getClass().getSimpleName());
            appendExceptionCommonInfo(node, componentIndex, count);
            throw e;
        }
    }

    private void writeAllColumns(MergeColumnTupleReference columnTuple) throws HyracksDataException {
        /*
         * The last tuple from one of the components was reached. Since we are going to the next leaf, we will not be
         * able to access the readers of this component's leaf after this tuple. So, we are going to write
         * the values of all columns as recorded in writtenComponents
         */
        int skipCount = columnTuple.getAndResetSkipCount();
        if (skipCount > 0) {
            writtenComponents.add(setAntimatterIndicator(columnTuple.getComponentIndex()), skipCount);
        }
        writeNonKeyColumns();
        writtenComponents.reset();
    }

    private static int setAntimatterIndicator(int componentIndex) {
        // This is to avoid -0, where the '-' is the antimatter indicator
        return -(componentIndex + 1);
    }

    private static int clearAntimatterIndicator(int componentIndex) {
        return -componentIndex - 1;
    }

    private int getMaxNumberOfTuples(int maxNumberOfTuples, int totalNumberOfTuples, int totalLength) {
        int numberOfTuplesUsingMaxSize = Integer.MAX_VALUE;
        if (totalLength > maxLeafNodeSize && totalNumberOfTuples > 0) {
            int bytesPerTuple = (int) Math.ceil(totalLength / (double) totalNumberOfTuples);
            numberOfTuplesUsingMaxSize = maxLeafNodeSize / bytesPerTuple;
        }
        return Math.min(maxNumberOfTuples, numberOfTuplesUsingMaxSize);
    }

    private void appendExceptionCommonInfo(ObjectNode node, int componentIndex, int count) {
        node.put("numberOfWrittenPrimaryKeys", primaryKeyWriters[0].getCount());
        node.put("writtenComponents", writtenComponents.toString());
        node.put("numberOFAntiMatters", numberOfAntiMatter);
        node.put("componentIndex", componentIndex);
        node.put("count", count);
    }

    public void updateColumnMetadataForCurrentTuple(ITupleReference tuple) throws HyracksDataException {
    }

    @Override
    public void resetTemporaryBufferForCurrentTuple() {

    }

    public int getRequiredTemporaryBuffersCountIncludingCurrentTuple() {
        return columnMetadata.getRequiredTemporaryBuffersCount();
    }

    /** For one input: per node, the keys to fill from its map and the readers to advance. */
    private static final class InputFill {
        private final Int2ObjectMap<int[]> keysOfNode = new Int2ObjectOpenHashMap<>();
        /** Per node: the map value readers for its keys' types, no duplicates. */
        private final Int2ObjectMap<IColumnValuesReader[]> valuesOfNode = new Int2ObjectOpenHashMap<>();
        /** All lookup readers this input advances, no duplicates. */
        private final int[] lookupColumns;

        private InputFill(MergeColumnTupleReference componentTuple, List<PromotedKeys.Key> movableKeys,
                boolean[] fillFromMap) {
            Int2ObjectMap<IntList> keys = new Int2ObjectOpenHashMap<>();
            Int2ObjectMap<IntSet> valueColumns = new Int2ObjectOpenHashMap<>();
            IntSet lookups = new IntLinkedOpenHashSet();
            for (int k = 0; k < movableKeys.size(); k++) {
                if (fillFromMap[k]) {
                    PromotedKeys.Key key = movableKeys.get(k);
                    int node = key.node.mapKeyColumn;
                    keys.computeIfAbsent(node, n -> new IntArrayList()).add(k);
                    valueColumns.computeIfAbsent(node, n -> new IntLinkedOpenHashSet()).add(key.valueColumn);
                    lookups.add(node);
                    lookups.add(key.valueColumn);
                }
            }
            for (Int2ObjectMap.Entry<IntList> entry : keys.int2ObjectEntrySet()) {
                int node = entry.getIntKey();
                keysOfNode.put(node, entry.getValue().toIntArray());
                int[] columns = valueColumns.get(node).toIntArray();
                IColumnValuesReader[] values = new IColumnValuesReader[columns.length];
                for (int v = 0; v < columns.length; v++) {
                    values[v] = componentTuple.getLookup(columns[v]);
                }
                valuesOfNode.put(node, values);
            }
            lookupColumns = lookups.toIntArray();
        }
    }
}
