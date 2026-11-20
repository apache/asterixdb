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

import java.io.IOException;

import org.apache.asterix.column.bytes.stream.in.MultiByteBufferInputStream;
import org.apache.asterix.column.metadata.PromotedKeys;
import org.apache.asterix.column.operation.lsm.merge.IEndOfPageCallBack;
import org.apache.asterix.column.operation.lsm.merge.MergeColumnReadMetadata;
import org.apache.asterix.column.values.IColumnValuesReader;
import org.apache.asterix.column.values.reader.PrimitiveColumnValuesReader;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.storage.am.lsm.btree.column.api.IColumnBufferProvider;
import org.apache.hyracks.storage.am.lsm.btree.column.api.IColumnReadMultiPageOp;
import org.apache.hyracks.storage.am.lsm.btree.column.api.projection.IColumnProjectionInfo;
import org.apache.hyracks.storage.am.lsm.btree.column.impls.btree.ColumnBTreeReadLeafFrame;

import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;

public final class MergeColumnTupleReference extends AbstractAsterixColumnTupleReference {
    // NoOP callback is for empty pages only
    private static final IEndOfPageCallBack EMPTY_PAGE_CALLBACK = createNoOpCallBack();
    private final IColumnValuesReader[] columnReaders;
    /**
     * Second readers ("lookups") for the columns a merge reads map values from: each node's map key column and its map
     * value column per key type. They read the same pages as the main readers, one per column, shared by all keys of
     * a node. Created here because the first page loads before the merge writer exists, and advanced on every page.
     * See {@link #getLookup}.
     */
    private final Int2ObjectMap<Lookup> lookups = new Int2ObjectOpenHashMap<>();
    private int skipCount;
    private IEndOfPageCallBack endOfPageCallBack;
    private int mergingLength;

    public MergeColumnTupleReference(int componentIndex, ColumnBTreeReadLeafFrame frame,
            MergeColumnReadMetadata columnMetadata, IColumnReadMultiPageOp multiPageOp) {
        super(componentIndex, frame, columnMetadata, multiPageOp);
        this.columnReaders = columnMetadata.getColumnReaders();
        endOfPageCallBack = EMPTY_PAGE_CALLBACK;
        try {
            for (PromotedKeys.Key key : columnMetadata.getMovableKeys()) {
                addLookup(columnMetadata, key.node.mapKeyColumn);
                addLookup(columnMetadata, key.valueColumn);
            }
        } catch (HyracksDataException e) {
            throw new IllegalStateException(e);
        }
    }

    private void addLookup(MergeColumnReadMetadata columnMetadata, int columnIndex) throws HyracksDataException {
        if (!lookups.containsKey(columnIndex)) {
            lookups.put(columnIndex, new Lookup(columnMetadata.createColumnReader(columnIndex)));
        }
    }

    @Override
    protected PrimitiveColumnValuesReader[] getPrimaryKeyReaders(IColumnProjectionInfo info) {
        MergeColumnReadMetadata columnMetadata = (MergeColumnReadMetadata) info;
        int numberOfPrimaryKeys = columnMetadata.getNumberOfPrimaryKeys();
        PrimitiveColumnValuesReader[] primaryKeyReaders = new PrimitiveColumnValuesReader[numberOfPrimaryKeys];
        IColumnValuesReader[] readers = columnMetadata.getColumnReaders();
        for (int i = 0; i < numberOfPrimaryKeys; i++) {
            primaryKeyReaders[i] = (PrimitiveColumnValuesReader) readers[i];
        }
        return primaryKeyReaders;
    }

    @Override
    protected void skipMegaLeafNode() {
        // no-op
    }

    @Override
    protected boolean startNewPage(int numberOfTuples) {
        //Skip filters
        frame.skipFilters();
        // skip count is always start from zero as no "search" is conducted during a merge
        this.skipCount = 0;
        mergingLength = 0;
        return true;
    }

    @Override
    protected void startColumn(IColumnBufferProvider buffersProvider, int ordinal, int numberOfTuples)
            throws HyracksDataException {
        int numberOfPrimaryKeys = primaryKeyStreams.length;
        if (ordinal < numberOfPrimaryKeys) {
            //Skip primary key
            return;
        }
        MultiByteBufferInputStream columnStream = (MultiByteBufferInputStream) columnStreams[ordinal];
        columnStream.reset(buffersProvider);
        Lookup lookup = lookups.get(ordinal);
        if (lookup != null) {
            // Must happen before the main reader consumes the stream header.
            lookup.reset(columnStream, numberOfTuples);
        }
        IColumnValuesReader reader = columnReaders[ordinal];
        reader.reset(columnStream, numberOfTuples);
        mergingLength += buffersProvider.getLength();
    }

    @Override
    protected void startColumnFilter(IColumnBufferProvider buffersProvider, int ordinal, int numberOfTuples)
            throws HyracksDataException {
        // NoOp
    }

    @Override
    protected boolean evaluateFilter() throws HyracksDataException {
        return true;
    }

    @Override
    public void initSkip(int tupleIndex, int count) throws HyracksDataException {
        skip(count);
    }

    @Override
    public void skipCurrentTuple() throws HyracksDataException {
        skip(1);
    }

    private void skip(int count) throws HyracksDataException {
        skipCount += count;
    }

    @Override
    public void lastTupleReached() throws HyracksDataException {
        endOfPageCallBack.callEnd(this);
    }

    public int getAndResetSkipCount() {
        int currentSkipCount = skipCount;
        skipCount = 0;
        return currentSkipCount;
    }

    public IColumnValuesReader getReader(int columnIndex) {
        return columnReaders[columnIndex];
    }

    /** The lookup reader for column {@code columnIndex} (a map key or map value column). */
    public IColumnValuesReader getLookup(int columnIndex) {
        return lookups.get(columnIndex).reader;
    }

    private static final class Lookup {
        private final MultiByteBufferInputStream stream = new MultiByteBufferInputStream();
        private final IColumnValuesReader reader;

        private Lookup(IColumnValuesReader reader) {
            this.reader = reader;
        }

        private void reset(MultiByteBufferInputStream columnStream, int numberOfTuples) throws HyracksDataException {
            if (columnStream.available() == 0) {
                // Column absent from this page: the reader reads it as missing.
                stream.reset();
            } else {
                try {
                    stream.resetAt(0, columnStream);
                } catch (IOException e) {
                    throw HyracksDataException.create(e);
                }
            }
            reader.reset(stream, numberOfTuples);
        }
    }

    public void registerEndOfPageCallBack(IEndOfPageCallBack endOfPageCallBack) {
        this.endOfPageCallBack = endOfPageCallBack;
    }

    public int getMergingLength() {
        return mergingLength;
    }

    private static IEndOfPageCallBack createNoOpCallBack() {
        return columnTuple -> {
            if (!columnTuple.isEmpty()) {
                // safeguard against unset proper call back for non-empty pages
                throw new NullPointerException("endOfPageCallBack is null");
            }
        };
    }
}
