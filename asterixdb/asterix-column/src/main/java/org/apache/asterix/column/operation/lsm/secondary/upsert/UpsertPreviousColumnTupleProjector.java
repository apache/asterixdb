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
package org.apache.asterix.column.operation.lsm.secondary.upsert;

import static org.apache.hyracks.storage.am.lsm.btree.column.api.projection.ColumnProjectorType.MODIFY;

import java.io.DataOutput;
import java.io.IOException;
import java.util.Collections;

import org.apache.asterix.column.filter.NoOpColumnFilterEvaluatorFactory;
import org.apache.asterix.column.operation.query.QueryColumnTupleProjector;
import org.apache.asterix.common.exceptions.NoOpWarningCollector;
import org.apache.asterix.om.types.ARecordType;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IValueReference;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.storage.am.lsm.btree.column.api.projection.IColumnProjectionInfo;
import org.apache.hyracks.storage.am.lsm.btree.column.api.projection.IColumnTupleProjector;

final class UpsertPreviousColumnTupleProjector implements IColumnTupleProjector {
    private final ArrayTupleBuilder builder;
    private final QueryColumnTupleProjector projector;

    public UpsertPreviousColumnTupleProjector(ARecordType datasetType, int numberOfPrimaryKeys,
            ARecordType requestedType) {
        builder = new ArrayTupleBuilder(numberOfPrimaryKeys + 1);
        projector = new QueryColumnTupleProjector(datasetType, numberOfPrimaryKeys, requestedType,
                Collections.emptyMap(), NoOpColumnFilterEvaluatorFactory.INSTANCE,
                NoOpColumnFilterEvaluatorFactory.INSTANCE, NoOpWarningCollector.INSTANCE, null, MODIFY);
    }

    @Override
    public IColumnProjectionInfo createProjectionInfo(IValueReference serializedMetadata) throws HyracksDataException {
        return projector.createProjectionInfo(serializedMetadata);
    }

    @Override
    public ITupleReference project(ITupleReference tuple, DataOutput dos, ArrayTupleBuilder tb) throws IOException {
        builder.reset();
        return projector.project(tuple, builder.getDataOutput(), builder);
    }
}
