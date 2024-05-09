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
package org.apache.hyracks.storage.am.lsm.btree.column.cloud.sweep;

import java.util.BitSet;
import java.util.List;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IValueReference;
import org.apache.hyracks.storage.am.lsm.btree.column.api.projection.IColumnProjectionInfo;
import org.apache.hyracks.storage.am.lsm.btree.column.api.projection.IColumnTupleProjector;
import org.apache.hyracks.storage.am.lsm.btree.column.impls.btree.ColumnBTree;
import org.apache.hyracks.storage.am.lsm.btree.column.impls.btree.ColumnBTreeLeafFrameFactory;
import org.apache.hyracks.storage.am.lsm.btree.column.impls.btree.ColumnBTreeReadLeafFrame;
import org.apache.hyracks.storage.am.lsm.btree.column.utils.ColumnUtil;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMDiskComponent;

public class ColumnSweeperUtil {
    private ColumnSweeperUtil() {
    }

    public static final BitSet EMPTY = new BitSet();

    static IColumnProjectionInfo createColumnProjectionInfo(List<ILSMDiskComponent> diskComponents,
            IColumnTupleProjector projector) throws HyracksDataException {
        // Get the newest disk component, which has the newest column metadata
        ILSMDiskComponent newestComponent = diskComponents.get(0);
        IValueReference columnMetadata = ColumnUtil.getColumnMetadataCopy(newestComponent.getMetadata());

        return projector.createProjectionInfo(columnMetadata);
    }

    static ColumnBTreeReadLeafFrame createLeafFrame(IColumnProjectionInfo projectionInfo,
            ILSMDiskComponent diskComponent) {
        ColumnBTree columnBTree = (ColumnBTree) diskComponent.getIndex();
        ColumnBTreeLeafFrameFactory leafFrameFactory = (ColumnBTreeLeafFrameFactory) columnBTree.getLeafFrameFactory();
        return leafFrameFactory.createReadFrame(projectionInfo);
    }
}
