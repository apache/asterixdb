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
package org.apache.hyracks.storage.am.lsm.btree.column.utils;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.util.HyracksConstants;
import org.apache.hyracks.data.std.api.IValueReference;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.storage.am.common.freepage.MutableArrayValueReference;
import org.apache.hyracks.storage.am.lsm.btree.column.api.IColumnManager;
import org.apache.hyracks.storage.am.lsm.btree.column.api.projection.IColumnTupleProjector;
import org.apache.hyracks.storage.am.lsm.btree.column.impls.lsm.LSMColumnBTree;
import org.apache.hyracks.storage.am.lsm.common.api.IComponentMetadata;
import org.apache.hyracks.storage.common.IIndexAccessParameters;

public class ColumnUtil {
    /**
     * Used to get the columns info from {@link IComponentMetadata#get(IValueReference, ArrayBackedValueStorage)}
     *
     * @see LSMColumnBTree#activate()
     * @see IColumnManager#activate(IValueReference)
     */
    private static final MutableArrayValueReference COLUMNS_METADATA_KEY =
            new MutableArrayValueReference("COLUMNS_METADATA".getBytes());

    private ColumnUtil() {
    }

    public static IValueReference getColumnMetadataCopy(IComponentMetadata src) throws HyracksDataException {
        ArrayBackedValueStorage storage = new ArrayBackedValueStorage();
        src.get(COLUMNS_METADATA_KEY, storage);
        return storage;
    }

    public static void putColumnsMetadataValue(IValueReference columnsMetadataValue, IComponentMetadata dest)
            throws HyracksDataException {
        dest.put(COLUMNS_METADATA_KEY, columnsMetadataValue);
    }

    public static IColumnTupleProjector getTupleProjector(IIndexAccessParameters iap,
            IColumnTupleProjector defaultProjector) {
        IColumnTupleProjector projector =
                iap.getParameter(HyracksConstants.TUPLE_PROJECTOR, IColumnTupleProjector.class);
        return projector == null ? defaultProjector : projector;
    }
}
