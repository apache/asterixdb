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
package org.apache.asterix.column.tuple;

import static org.apache.hyracks.storage.am.common.frames.AbstractSlotManager.ERROR_INDICATOR;
import static org.apache.hyracks.storage.am.common.frames.AbstractSlotManager.GREATEST_KEY_INDICATOR;

import org.apache.asterix.column.assembler.value.IValueGetter;
import org.apache.asterix.column.assembler.value.ValueGetterFactory;
import org.apache.asterix.column.bytes.stream.in.AbstractBytesInputStream;
import org.apache.asterix.column.bytes.stream.in.ByteBufferInputStream;
import org.apache.asterix.column.bytes.stream.in.DummyBytesInputStream;
import org.apache.asterix.column.bytes.stream.in.MultiByteBufferInputStream;
import org.apache.asterix.column.values.IColumnValuesReader;
import org.apache.asterix.column.values.reader.PrimitiveColumnValuesReader;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.primitive.VoidPointable;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.storage.am.common.ophelpers.FindTupleMode;
import org.apache.hyracks.storage.am.common.ophelpers.FindTupleNoExactMatchPolicy;
import org.apache.hyracks.storage.am.lsm.btree.column.api.IColumnBufferProvider;
import org.apache.hyracks.storage.am.lsm.btree.column.api.IColumnReadMultiPageOp;
import org.apache.hyracks.storage.am.lsm.btree.column.api.IColumnTupleIterator;
import org.apache.hyracks.storage.am.lsm.btree.column.api.projection.IColumnProjectionInfo;
import org.apache.hyracks.storage.am.lsm.btree.column.error.ColumnarValueException;
import org.apache.hyracks.storage.am.lsm.btree.column.impls.btree.ColumnBTreeReadLeafFrame;
import org.apache.hyracks.storage.am.lsm.btree.column.impls.lsm.tuples.AbstractColumnTupleReference;
import org.apache.hyracks.storage.common.MultiComparator;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

public abstract class AbstractAsterixColumnTupleReference extends AbstractColumnTupleReference {
    private final IValueGetter[] primaryKeysValueGetters;
    protected final ByteBufferInputStream[] primaryKeyStreams;
    protected final PrimitiveColumnValuesReader[] primaryKeyReaders;
    protected final VoidPointable[] primaryKeys;
    protected final AbstractBytesInputStream[] columnStreams;

    protected AbstractAsterixColumnTupleReference(int componentIndex, ColumnBTreeReadLeafFrame frame,
            IColumnProjectionInfo info, IColumnReadMultiPageOp multiPageOp) {
        super(componentIndex, frame, info, multiPageOp);
        primaryKeyReaders = getPrimaryKeyReaders(info);
        int numberOfPrimaryKeys = primaryKeyReaders.length;

        this.primaryKeyStreams = new ByteBufferInputStream[numberOfPrimaryKeys];
        primaryKeysValueGetters = new IValueGetter[numberOfPrimaryKeys];
        primaryKeys = new VoidPointable[numberOfPrimaryKeys];

        for (int i = 0; i < numberOfPrimaryKeys; i++) {
            // Shared with the assembler PK readers (if assembler requires PK readers)
            primaryKeyStreams[i] = new ByteBufferInputStream();
            primaryKeysValueGetters[i] =
                    ValueGetterFactory.INSTANCE.createValueGetter(primaryKeyReaders[i].getTypeTag());
            primaryKeys[i] = new VoidPointable();
        }

        this.columnStreams = new AbstractBytesInputStream[info.getNumberOfProjectedColumns()];
        for (int i = 0; i < columnStreams.length; i++) {
            if (info.getColumnIndex(i) >= numberOfPrimaryKeys) {
                columnStreams[i] = new MultiByteBufferInputStream();
            } else {
                // Assembler's PK readers are shared with the cursor's PK readers
                columnStreams[i] = DummyBytesInputStream.INSTANCE;
            }
        }
    }

    protected abstract PrimitiveColumnValuesReader[] getPrimaryKeyReaders(IColumnProjectionInfo info);

    @Override
    protected int setPrimaryKeysAt(int index, int skipCount) throws HyracksDataException {
        int numberOfSkippedAntiMatters = resetPrimaryKeyReader(0, index, skipCount);
        for (int i = 1; i < primaryKeyReaders.length; i++) {
            resetPrimaryKeyReader(i, index, skipCount);
        }
        return skipCount - numberOfSkippedAntiMatters;
    }

    @Override
    protected final void startPrimaryKey(IColumnBufferProvider provider, int ordinal, int numberOfTuples)
            throws HyracksDataException {
        ByteBufferInputStream primaryKeyStream = primaryKeyStreams[ordinal];
        primaryKeyStream.reset(provider);
        IColumnValuesReader reader = primaryKeyReaders[ordinal];
        reader.reset(primaryKeyStream, numberOfTuples);
    }

    @Override
    protected final void onNext() throws HyracksDataException {
        for (int i = 0; i < primaryKeys.length; i++) {
            IColumnValuesReader reader = primaryKeyReaders[i];
            reader.next();
            primaryKeys[i].set(primaryKeysValueGetters[i].getValue(reader));
        }
    }

    @Override
    public void lastTupleReached() throws HyracksDataException {
        //Default: noOp
    }

    @Override
    public final int getFieldCount() {
        return primaryKeys.length;
    }

    @Override
    public final byte[] getFieldData(int fIdx) {
        return primaryKeys[fIdx].getByteArray();
    }

    @Override
    public final int getFieldStart(int fIdx) {
        return primaryKeys[fIdx].getStartOffset();
    }

    @Override
    public final int getFieldLength(int fIdx) {
        return primaryKeys[fIdx].getLength();
    }

    @Override
    public final int getTupleSize() {
        return -1;
    }

    @Override
    public final boolean isAntimatter() {
        /*
         * The primary key cannot be missing, but the actual tuple is missing. There is no need to check other
         * primary key readers (for composite primary keys). One primary key reader is sufficient to determine if a
         * tuple is an anti-matter tuple.
         */
        return primaryKeyReaders[0].isMissing();
    }

    @Override
    public final int compareTo(IColumnTupleIterator o) {
        AbstractAsterixColumnTupleReference other = (AbstractAsterixColumnTupleReference) o;
        int compare = 0;
        for (int i = 0; i < primaryKeys.length && compare == 0; i++) {
            compare = primaryKeyReaders[i].compareTo(other.primaryKeyReaders[i]);
        }
        return compare;
    }

    @Override
    public int findTupleIndex(ITupleReference searchKey, MultiComparator comparator, FindTupleMode mode,
            FindTupleNoExactMatchPolicy matchPolicy) throws HyracksDataException {
        int tupleCount = getTupleCount();
        if (tupleCount <= 0) {
            return GREATEST_KEY_INDICATOR;
        }

        int mid;
        int begin = tupleIndex;
        int end = tupleCount - 1;

        while (begin <= end) {
            mid = (begin + end) / 2;

            setKeyAt(mid);
            int cmp = comparator.compare(searchKey, this);
            if (cmp < 0) {
                end = mid - 1;
            } else if (cmp > 0) {
                begin = mid + 1;
            } else {
                if (mode == FindTupleMode.EXCLUSIVE) {
                    if (matchPolicy == FindTupleNoExactMatchPolicy.HIGHER_KEY) {
                        begin = mid + 1;
                    } else {
                        end = mid - 1;
                    }
                } else {
                    if (mode == FindTupleMode.EXCLUSIVE_ERROR_IF_EXISTS) {
                        return ERROR_INDICATOR;
                    } else {
                        return mid;
                    }
                }
            }
        }

        if (mode == FindTupleMode.EXACT) {
            return ERROR_INDICATOR;
        }

        if (matchPolicy == FindTupleNoExactMatchPolicy.HIGHER_KEY) {
            if (begin > tupleCount - 1) {
                return GREATEST_KEY_INDICATOR;
            }

            setKeyAt(begin);
            if (comparator.compare(searchKey, this) < 0) {
                return begin;
            } else {
                return GREATEST_KEY_INDICATOR;
            }
        } else {
            if (end < 0) {
                return GREATEST_KEY_INDICATOR;
            }

            setKeyAt(end);
            if (comparator.compare(searchKey, this) > 0) {
                return end;
            } else {
                return GREATEST_KEY_INDICATOR;
            }
        }
    }

    protected void setKeyAt(int index) {
        for (int i = 0; i < primaryKeyReaders.length; i++) {
            PrimitiveColumnValuesReader reader = primaryKeyReaders[i];
            reader.getValue(index);
            primaryKeys[i].set(primaryKeysValueGetters[i].getValue(reader));
        }
    }

    protected void appendExceptionInformation(ColumnarValueException e, int previousIndex) {
        ObjectNode node = e.createNode(getClass().getSimpleName());
        node.put("isAntiMatter", isAntimatter());
        node.put("previousIndex", previousIndex);
        ArrayNode pkNodes = node.putArray("primaryKeyReaders");
        for (IColumnValuesReader reader : primaryKeyReaders) {
            reader.appendReaderInformation(pkNodes.addObject());
        }
    }

    private int resetPrimaryKeyReader(int i, int index, int skipCount) throws HyracksDataException {
        PrimitiveColumnValuesReader reader = primaryKeyReaders[i];
        // Returns the number of encountered anti-matters
        int numberOfSkippedAntiMatters = reader.reset(index, skipCount);
        primaryKeys[i].set(primaryKeysValueGetters[i].getValue(reader));
        // include the current key if the current key is an anti-matter
        return numberOfSkippedAntiMatters + (isAntimatter() ? 1 : 0);
    }
}
