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
package org.apache.asterix.column.metadata;

import org.apache.asterix.om.types.ARecordType;
import org.apache.hyracks.data.std.api.IValueReference;
import org.apache.hyracks.storage.am.lsm.btree.column.api.AbstractColumnTupleReader;
import org.apache.hyracks.storage.am.lsm.btree.column.api.projection.ColumnProjectorType;
import org.apache.hyracks.storage.am.lsm.btree.column.api.projection.IColumnProjectionInfo;

public abstract class AbstractColumnImmutableReadMetadata extends AbstractColumnImmutableMetadata
        implements IColumnProjectionInfo {
    private final ColumnProjectorType projectorType;

    protected AbstractColumnImmutableReadMetadata(ARecordType datasetType, ARecordType metaType,
            int numberOfPrimaryKeys, IValueReference serializedMetadata, int numberOfColumns,
            ColumnProjectorType projectorType) {
        super(datasetType, metaType, numberOfPrimaryKeys, serializedMetadata, numberOfColumns);
        this.projectorType = projectorType;
    }

    /**
     * @return the corresponding reader (merge reader or query reader) given <code>this</code> metadata
     */
    public abstract AbstractColumnTupleReader createTupleReader();

    @Override
    public final ColumnProjectorType getProjectorType() {
        return projectorType;
    }
}
