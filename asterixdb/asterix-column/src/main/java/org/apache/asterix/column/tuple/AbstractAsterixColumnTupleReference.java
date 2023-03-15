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

import org.apache.asterix.column.assembler.value.IValueGetter;
import org.apache.asterix.column.assembler.value.ValueGetterFactory;
import org.apache.asterix.column.bytes.stream.in.AbstractBytesInputStream;
import org.apache.asterix.column.bytes.stream.in.ByteBufferInputStream;
import org.apache.asterix.column.bytes.stream.in.MultiByteBufferInputStream;
import org.apache.asterix.column.values.IColumnValuesReader;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.primitive.VoidPointable;
import org.apache.hyracks.storage.am.lsm.btree.column.api.IColumnBufferProvider;
import org.apache.hyracks.storage.am.lsm.btree.column.api.IColumnReadMultiPageOp;
import org.apache.hyracks.storage.am.lsm.btree.column.api.IColumnTupleIterator;
import org.apache.hyracks.storage.am.lsm.btree.column.api.projection.IColumnProjectionInfo;
import org.apache.hyracks.storage.am.lsm.btree.column.impls.btree.ColumnBTreeReadLeafFrame;
import org.apache.hyracks.storage.am.lsm.btree.column.impls.lsm.tuples.AbstractColumnTupleReference;

public abstract class AbstractAsterixColumnTupleReference extends AbstractColumnTupleReference {
    private final IValueGetter[] primaryKeysValueGetters;
    protected final ByteBufferInputStream[] primaryKeyStreams;
    protected final IColumnValuesReader[] primaryKeyReaders;
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
                columnStreams[i] = new ByteBufferInputStream();
            }
        }
    }

    protected abstract IColumnValuesReader[] getPrimaryKeyReaders(IColumnProjectionInfo info);

    @Override
    protected final void startPrimaryKey(IColumnBufferProvider provider, int startIndex, int ordinal,
            int numberOfTuples) throws HyracksDataException {
        ByteBufferInputStream primaryKeyStream = primaryKeyStreams[ordinal];
        primaryKeyStream.reset(provider);
        IColumnValuesReader reader = primaryKeyReaders[ordinal];
        reader.reset(primaryKeyStream, numberOfTuples);
        reader.skip(startIndex);
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
}
