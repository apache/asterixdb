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

import java.io.ByteArrayInputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.asterix.column.metadata.AbstractColumnImmutableMetadata;
import org.apache.asterix.column.operation.lsm.flush.FlushColumnMetadata;
import org.apache.asterix.column.values.IColumnValuesWriter;
import org.apache.asterix.column.values.IColumnValuesWriterFactory;
import org.apache.asterix.column.values.writer.ColumnValuesWriterFactory;
import org.apache.asterix.om.types.ARecordType;
import org.apache.commons.lang3.mutable.Mutable;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IValueReference;
import org.apache.hyracks.data.std.primitive.IntegerPointable;
import org.apache.hyracks.storage.am.lsm.btree.column.api.IColumnTupleIterator;
import org.apache.hyracks.storage.am.lsm.btree.column.api.IColumnWriteMultiPageOp;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMDiskComponent;

/**
 * Merge column write metadata belongs to write a new merge {@link ILSMDiskComponent}
 * This is for writing a new on-disk component by merging two or more on disk components. The final schema for this
 * component will the most recent schema, which belongs to the newest merged component. The schema here is immutable
 * and cannot be changed.
 */
public final class MergeColumnWriteMetadata extends AbstractColumnImmutableMetadata {
    private final Mutable<IColumnWriteMultiPageOp> multiPageOpRef;
    private final List<IColumnValuesWriter> columnWriters;
    private final List<IColumnTupleIterator> componentsTuples;

    /**
     * For LSM Merge
     */
    private MergeColumnWriteMetadata(ARecordType datasetType, ARecordType metaType, int numberOfPrimaryKeys,
            Mutable<IColumnWriteMultiPageOp> multiPageOpRef, List<IColumnValuesWriter> columnWriters,
            IValueReference serializedMetadata, List<IColumnTupleIterator> componentsTuples) {
        super(datasetType, metaType, numberOfPrimaryKeys, serializedMetadata, columnWriters.size());
        this.multiPageOpRef = multiPageOpRef;
        this.columnWriters = columnWriters;
        this.componentsTuples = componentsTuples;
    }

    /**
     * Set {@link IColumnWriteMultiPageOp} for {@link IColumnValuesWriter}
     *
     * @param multiPageOp multi-buffer allocator
     */
    public void init(IColumnWriteMultiPageOp multiPageOp) throws HyracksDataException {
        multiPageOpRef.setValue(multiPageOp);

        //Reset writer for the first write
        for (int i = 0; i < columnWriters.size(); i++) {
            columnWriters.get(i).reset();
        }
    }

    public Mutable<IColumnWriteMultiPageOp> getMultiPageOpRef() {
        return multiPageOpRef;
    }

    public IColumnValuesWriter getWriter(int columnIndex) {
        return columnWriters.get(columnIndex);
    }

    public void close() {
        multiPageOpRef.setValue(null);
        for (int i = 0; i < columnWriters.size(); i++) {
            columnWriters.get(i).close();
        }
    }

    public static MergeColumnWriteMetadata create(ARecordType datasetType, ARecordType metaType,
            int numberOfPrimaryKeys, Mutable<IColumnWriteMultiPageOp> multiPageOpRef,
            IValueReference serializedMetadata, List<IColumnTupleIterator> componentsTuples) throws IOException {
        byte[] bytes = serializedMetadata.getByteArray();
        int offset = serializedMetadata.getStartOffset();
        int length = serializedMetadata.getLength();

        int writersOffset = offset + IntegerPointable.getInteger(bytes, offset + WRITERS_POINTER);
        DataInput input = new DataInputStream(new ByteArrayInputStream(bytes, writersOffset, length));

        IColumnValuesWriterFactory writerFactory = new ColumnValuesWriterFactory(multiPageOpRef);
        List<IColumnValuesWriter> writers = new ArrayList<>();
        FlushColumnMetadata.deserializeWriters(input, writers, writerFactory);

        return new MergeColumnWriteMetadata(datasetType, metaType, numberOfPrimaryKeys, multiPageOpRef, writers,
                serializedMetadata, componentsTuples);
    }

    public List<IColumnTupleIterator> getComponentsTuples() {
        return componentsTuples;
    }
}
