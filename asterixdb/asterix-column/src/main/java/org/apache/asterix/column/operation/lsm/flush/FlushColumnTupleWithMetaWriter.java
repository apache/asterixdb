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

import org.apache.asterix.om.lazy.RecordLazyVisitablePointable;
import org.apache.asterix.om.lazy.TypedRecordLazyVisitablePointable;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.storage.am.lsm.btree.tuples.LSMBTreeTupleReference;

public class FlushColumnTupleWithMetaWriter extends FlushColumnTupleWriter {
    private final ColumnTransformer metaColumnTransformer;
    private final RecordLazyVisitablePointable metaPointable;

    public FlushColumnTupleWithMetaWriter(FlushColumnMetadata columnMetadata, int pageSize, int maxNumberOfTuples,
            double tolerance, int maxLeafNodeSize) {
        super(columnMetadata, pageSize, maxNumberOfTuples, tolerance, maxLeafNodeSize);
        metaColumnTransformer = new ColumnTransformer(columnMetadata, columnMetadata.getMetaRoot());
        metaPointable = new TypedRecordLazyVisitablePointable(columnMetadata.getMetaType());
    }

    @Override
    protected void writeMeta(LSMBTreeTupleReference btreeTuple) throws HyracksDataException {
        if (btreeTuple.isAntimatter()) {
            return;
        }

        int metaFieldId = columnMetadata.getMetaRecordFieldIndex();
        metaPointable.set(btreeTuple.getFieldData(metaFieldId), btreeTuple.getFieldStart(metaFieldId),
                btreeTuple.getFieldLength(metaFieldId));
        //In case the primary key is not in the meta part, we take the maximum
        primaryKeysEstimatedSize = Math.max(metaColumnTransformer.transform(metaPointable), primaryKeysEstimatedSize);
    }
}
