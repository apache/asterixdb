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
package org.apache.asterix.column;

import java.io.IOException;
import java.util.List;

import org.apache.asterix.column.operation.lsm.flush.FlushColumnMetadata;
import org.apache.asterix.column.operation.lsm.merge.MergeColumnTupleProjector;
import org.apache.asterix.column.operation.lsm.merge.MergeColumnWriteMetadata;
import org.apache.asterix.column.values.IColumnValuesReaderFactory;
import org.apache.asterix.column.values.IColumnValuesWriterFactory;
import org.apache.asterix.column.values.reader.ColumnValueReaderFactory;
import org.apache.asterix.column.values.writer.ColumnValuesWriterFactory;
import org.apache.asterix.om.types.ARecordType;
import org.apache.commons.lang3.mutable.Mutable;
import org.apache.commons.lang3.mutable.MutableObject;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IValueReference;
import org.apache.hyracks.storage.am.lsm.btree.column.api.IColumnManager;
import org.apache.hyracks.storage.am.lsm.btree.column.api.IColumnMetadata;
import org.apache.hyracks.storage.am.lsm.btree.column.api.IColumnTupleIterator;
import org.apache.hyracks.storage.am.lsm.btree.column.api.IColumnWriteMultiPageOp;
import org.apache.hyracks.storage.am.lsm.btree.column.api.projection.IColumnTupleProjector;

public final class ColumnManager implements IColumnManager {
    private final ARecordType datasetType;
    private final ARecordType metaType;
    private final List<List<String>> primaryKeys;
    private final List<Integer> keySourceIndicator;
    private final MergeColumnTupleProjector mergeColumnTupleProjector;

    ColumnManager(ARecordType datasetType, ARecordType metaType, List<List<String>> primaryKeys,
            List<Integer> keySourceIndicator) {
        this.datasetType = datasetType;
        this.metaType = metaType;
        this.primaryKeys = primaryKeys;
        this.keySourceIndicator = keySourceIndicator;
        IColumnValuesReaderFactory readerFactory = new ColumnValueReaderFactory();
        mergeColumnTupleProjector =
                new MergeColumnTupleProjector(datasetType, metaType, primaryKeys.size(), readerFactory);
    }

    @Override
    public int getNumberOfPrimaryKeys() {
        return primaryKeys.size();
    }

    @Override
    public IColumnMetadata activate() throws HyracksDataException {
        Mutable<IColumnWriteMultiPageOp> multiPageOpRef = new MutableObject<>();
        IColumnValuesWriterFactory factory = new ColumnValuesWriterFactory(multiPageOpRef);
        return new FlushColumnMetadata(datasetType, metaType, primaryKeys, keySourceIndicator, factory, multiPageOpRef);
    }

    @Override
    public IColumnMetadata activate(IValueReference metadata) throws HyracksDataException {
        try {
            Mutable<IColumnWriteMultiPageOp> multiPageOpRef = new MutableObject<>();
            IColumnValuesWriterFactory writerFactory = new ColumnValuesWriterFactory(multiPageOpRef);
            return FlushColumnMetadata.create(datasetType, metaType, primaryKeys, keySourceIndicator, writerFactory,
                    multiPageOpRef, metadata);
        } catch (IOException e) {
            throw HyracksDataException.create(e);
        }
    }

    @Override
    public IColumnMetadata createMergeColumnMetadata(IValueReference metadata,
            List<IColumnTupleIterator> componentsTuples) throws HyracksDataException {
        try {
            return MergeColumnWriteMetadata.create(datasetType, metaType, primaryKeys.size(), new MutableObject<>(),
                    metadata, componentsTuples);
        } catch (IOException e) {
            throw HyracksDataException.create(e);
        }

    }

    @Override
    public IColumnTupleProjector getMergeColumnProjector() {
        return mergeColumnTupleProjector;
    }
}
