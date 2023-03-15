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

import java.io.DataOutput;
import java.io.IOException;

import org.apache.asterix.column.values.IColumnValuesReaderFactory;
import org.apache.asterix.om.types.ARecordType;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IValueReference;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.storage.am.lsm.btree.column.api.projection.IColumnProjectionInfo;
import org.apache.hyracks.storage.am.lsm.btree.column.api.projection.IColumnTupleProjector;

public class MergeColumnTupleProjector implements IColumnTupleProjector {
    private final ARecordType datasetType;
    private final ARecordType metaType;
    private final int numberOfPrimaryKeys;
    private final IColumnValuesReaderFactory readerFactory;

    public MergeColumnTupleProjector(ARecordType datasetType, ARecordType metaType, int numberOfPrimaryKeys,
            IColumnValuesReaderFactory readerFactory) {
        this.datasetType = datasetType;
        this.metaType = metaType;
        this.numberOfPrimaryKeys = numberOfPrimaryKeys;
        this.readerFactory = readerFactory;
    }

    @Override
    public IColumnProjectionInfo createProjectionInfo(IValueReference columnMetadata) throws HyracksDataException {
        try {
            return MergeColumnReadMetadata.create(datasetType, metaType, numberOfPrimaryKeys, readerFactory,
                    columnMetadata);
        } catch (IOException e) {
            throw HyracksDataException.create(e);
        }
    }

    @Override
    public ITupleReference project(ITupleReference tuple, DataOutput dos, ArrayTupleBuilder tb) throws IOException {
        throw new IllegalAccessError(getClass().getName());
    }
}
