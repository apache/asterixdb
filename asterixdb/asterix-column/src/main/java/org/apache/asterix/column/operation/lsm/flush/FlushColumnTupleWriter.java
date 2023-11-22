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
package org.apache.asterix.column.operation.lsm.flush;

import java.nio.ByteBuffer;

import org.apache.asterix.column.values.IColumnValuesWriter;
import org.apache.asterix.column.values.writer.ColumnBatchWriter;
import org.apache.asterix.column.values.writer.filters.AbstractColumnFilterWriter;
import org.apache.asterix.om.lazy.RecordLazyVisitablePointable;
import org.apache.asterix.om.lazy.TypedRecordLazyVisitablePointable;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.storage.am.lsm.btree.column.api.AbstractColumnTupleWriter;
import org.apache.hyracks.storage.am.lsm.btree.column.api.IColumnWriteMultiPageOp;
import org.apache.hyracks.storage.am.lsm.btree.tuples.LSMBTreeTupleReference;

public class FlushColumnTupleWriter extends AbstractColumnTupleWriter {
    protected final FlushColumnMetadata columnMetadata;
    protected final BatchFinalizerVisitor finalizer;
    protected final ColumnBatchWriter writer;

    private final ColumnTransformer transformer;
    private final RecordLazyVisitablePointable pointable;
    private final int maxNumberOfTuples;
    private final IColumnValuesWriter[] primaryKeyWriters;
    private final int maxLeafNodeSize;

    protected int primaryKeysEstimatedSize;

    public FlushColumnTupleWriter(FlushColumnMetadata columnMetadata, int pageSize, int maxNumberOfTuples,
            double tolerance, int maxLeafNodeSize) {
        this.columnMetadata = columnMetadata;
        transformer = new ColumnTransformer(columnMetadata, columnMetadata.getRoot());
        finalizer = new BatchFinalizerVisitor(columnMetadata);
        writer = new ColumnBatchWriter(columnMetadata.getMultiPageOpRef(), pageSize, tolerance);
        this.maxNumberOfTuples = maxNumberOfTuples;
        this.maxLeafNodeSize = maxLeafNodeSize;
        pointable = new TypedRecordLazyVisitablePointable(columnMetadata.getDatasetType());

        int numberOfPrimaryKeys = columnMetadata.getNumberOfPrimaryKeys();
        primaryKeyWriters = new IColumnValuesWriter[numberOfPrimaryKeys];
        for (int i = 0; i < numberOfPrimaryKeys; i++) {
            primaryKeyWriters[i] = columnMetadata.getWriter(i);
        }
    }

    @Override
    public final void init(IColumnWriteMultiPageOp multiPageOp) throws HyracksDataException {
        columnMetadata.init(multiPageOp);
    }

    @Override
    public final int getNumberOfColumns() {
        return columnMetadata.getNumberOfColumns();
    }

    @Override
    public final int bytesRequired(ITupleReference tuple) {
        int primaryKeysSize = 0;
        for (int i = 0; i < columnMetadata.getNumberOfPrimaryKeys(); i++) {
            primaryKeysSize += primaryKeyWriters[i].getEstimatedSize(tuple.getFieldLength(i));
        }

        //Mostly it is an overestimated size
        return primaryKeysSize;
    }

    @Override
    public final int getOccupiedSpace() {
        int numberOfColumns = getNumberOfColumns();
        int filterSize = numberOfColumns * AbstractColumnFilterWriter.FILTER_SIZE;
        return primaryKeysEstimatedSize + filterSize;
    }

    /**
     * TODO a better approach should be adopted
     *
     * @return the configured max number of tuples or zero if strings exceeded the maximum size
     */
    @Override
    public final int getMaxNumberOfTuples() {
        if (transformer.getStringLengths() >= maxLeafNodeSize) {
            return 0;
        }
        return maxNumberOfTuples;
    }

    @Override
    public final void close() {
        columnMetadata.close();
    }

    @Override
    public void writeTuple(ITupleReference tuple) throws HyracksDataException {
        //This from an in-memory component, hence the cast
        LSMBTreeTupleReference btreeTuple = (LSMBTreeTupleReference) tuple;
        if (btreeTuple.isAntimatter()) {
            //Write only the primary keys of an anti-matter tuple
            primaryKeysEstimatedSize = transformer.writeAntiMatter(btreeTuple);
            return;
        }
        writeRecord(tuple);
        writeMeta(btreeTuple);
    }

    @Override
    public final int flush(ByteBuffer pageZero) throws HyracksDataException {
        writer.setPageZeroBuffer(pageZero, getNumberOfColumns(), columnMetadata.getNumberOfPrimaryKeys());
        transformer.resetStringLengths();
        return finalizer.finalizeBatch(writer, columnMetadata);
    }

    protected void writeRecord(ITupleReference tuple) throws HyracksDataException {
        int recordFieldId = columnMetadata.getRecordFieldIndex();
        pointable.set(tuple.getFieldData(recordFieldId), tuple.getFieldStart(recordFieldId),
                tuple.getFieldLength(recordFieldId));
        primaryKeysEstimatedSize = transformer.transform(pointable);
    }

    protected void writeMeta(LSMBTreeTupleReference btreeTuple) throws HyracksDataException {
        //NoOp
    }
}
