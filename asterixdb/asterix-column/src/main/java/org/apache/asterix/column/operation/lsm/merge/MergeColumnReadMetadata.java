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

import static org.apache.hyracks.storage.am.lsm.btree.column.api.projection.ColumnProjectorType.EXISTENCE;
import static org.apache.hyracks.storage.am.lsm.btree.column.api.projection.ColumnProjectorType.MERGE;

import java.io.ByteArrayInputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.IOException;

import org.apache.asterix.column.metadata.AbstractColumnImmutableReadMetadata;
import org.apache.asterix.column.operation.lsm.flush.FlushColumnMetadata;
import org.apache.asterix.column.values.IColumnValuesReader;
import org.apache.asterix.column.values.IColumnValuesReaderFactory;
import org.apache.asterix.om.types.ARecordType;
import org.apache.hyracks.data.std.api.IValueReference;
import org.apache.hyracks.data.std.primitive.IntegerPointable;
import org.apache.hyracks.storage.am.lsm.btree.column.api.AbstractColumnTupleReader;
import org.apache.hyracks.storage.am.lsm.btree.column.api.projection.ColumnProjectorType;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMDiskComponent;

/**
 * Merge column read metadata belongs to read an {@link ILSMDiskComponent}
 * This only for reading an existing on-disk component for a merge operation. The schema here is immutable and cannot
 * be changed.
 */
public final class MergeColumnReadMetadata extends AbstractColumnImmutableReadMetadata {
    private final IColumnValuesReader[] columnReaders;
    /**
     * How many of {@link #columnReaders} are actually projected: {@code columnReaders.length} normally, and
     * {@code 0} for the existence-only view produced by {@link #createExistenceOnlyProjectionInfo()}.
     */
    private final int numberOfProjectedColumns;

    private MergeColumnReadMetadata(ARecordType datasetType, ARecordType metaType, int numberOfPrimaryKeys,
            IColumnValuesReader[] columnReaders, IValueReference serializedMetadata) {
        this(datasetType, metaType, numberOfPrimaryKeys, columnReaders, serializedMetadata, columnReaders.length,
                MERGE);
    }

    private MergeColumnReadMetadata(ARecordType datasetType, ARecordType metaType, int numberOfPrimaryKeys,
            IColumnValuesReader[] columnReaders, IValueReference serializedMetadata, int numberOfProjectedColumns,
            ColumnProjectorType projectorType) {
        super(datasetType, metaType, numberOfPrimaryKeys, serializedMetadata, columnReaders.length, projectorType);
        this.columnReaders = columnReaders;
        this.numberOfProjectedColumns = numberOfProjectedColumns;
    }

    /**
     * create ColumnMergeReadMetadata from columnMetadata
     *
     * @param serializedMetadata columnMetadata
     * @return {@link MergeColumnReadMetadata}
     * @see FlushColumnMetadata#serializeColumnsMetadata() for more information about serialization order
     */
    public static MergeColumnReadMetadata create(ARecordType datasetType, ARecordType metaType, int numberOfPrimaryKeys,
            IColumnValuesReaderFactory readerFactory, IValueReference serializedMetadata) throws IOException {
        byte[] bytes = serializedMetadata.getByteArray();
        int offset = serializedMetadata.getStartOffset();
        int length = serializedMetadata.getLength();

        int pathInfoStart = offset + IntegerPointable.getInteger(bytes, offset + PATH_INFO_POINTER);
        DataInput input = new DataInputStream(new ByteArrayInputStream(bytes, pathInfoStart, length));
        int numberOfColumns = input.readInt();
        IColumnValuesReader[] columnReaders = new IColumnValuesReader[numberOfColumns];
        for (int i = 0; i < numberOfColumns; i++) {
            IColumnValuesReader columnReader = readerFactory.createValueReader(input);
            //The order at which the path info was written is not ordered by the column index
            columnReaders[columnReader.getColumnIndex()] = columnReader;
        }

        return new MergeColumnReadMetadata(datasetType, metaType, numberOfPrimaryKeys, columnReaders,
                serializedMetadata);
    }

    public IColumnValuesReader[] getColumnReaders() {
        return columnReaders;
    }

    @Override
    public int getColumnIndex(int ordinal) {
        return ordinal;
    }

    @Override
    public int getNumberOfProjectedColumns() {
        return numberOfProjectedColumns;
    }

    @Override
    public MergeColumnReadMetadata createExistenceOnlyProjectionInfo() {
        throw new UnsupportedOperationException(
                getClass().getSimpleName() + " is never the projection an existence probe is built from");
    }

    /**
     * A key-only projection over {@code primaryKeyReaders}, for the sample cursor's newer-component liveness
     * probe. Shaped as a merge projection because {@code MergeColumnTupleReference} is already the tuple
     * reference that reads nothing but the primary keys
     *
     * @param primaryKeyReaders the component's primary-key readers, in key order (column {@code i} at index
     *                          {@code i}), as {@code MergeColumnTupleReference#getPrimaryKeyReaders} reads them
     * @see org.apache.asterix.column.operation.query.QueryColumnMetadata#createExistenceOnlyProjectionInfo()
     */
    public static MergeColumnReadMetadata createExistenceOnly(ARecordType datasetType, ARecordType metaType,
            IValueReference serializedMetadata, IColumnValuesReader[] primaryKeyReaders) {
        return new MergeColumnReadMetadata(datasetType, metaType, primaryKeyReaders.length, primaryKeyReaders,
                serializedMetadata, 0, EXISTENCE);
    }

    @Override
    public int getFilteredColumnIndex(int ordinal) {
        return -1;
    }

    @Override
    public int getNumberOfFilteredColumns() {
        return 0;
    }

    @Override
    public AbstractColumnTupleReader createTupleReader() {
        return new MergeColumnTupleReader(this);
    }
}
