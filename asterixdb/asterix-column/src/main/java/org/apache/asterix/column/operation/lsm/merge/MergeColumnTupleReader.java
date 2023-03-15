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

import org.apache.asterix.column.metadata.AbstractColumnImmutableReadMetadata;
import org.apache.asterix.column.tuple.MergeColumnTupleReference;
import org.apache.hyracks.storage.am.lsm.btree.column.api.AbstractColumnTupleReader;
import org.apache.hyracks.storage.am.lsm.btree.column.api.IColumnReadMultiPageOp;
import org.apache.hyracks.storage.am.lsm.btree.column.api.IColumnTupleIterator;
import org.apache.hyracks.storage.am.lsm.btree.column.impls.btree.ColumnBTreeReadLeafFrame;

public class MergeColumnTupleReader extends AbstractColumnTupleReader {
    private final MergeColumnReadMetadata columnMetadata;

    public MergeColumnTupleReader(AbstractColumnImmutableReadMetadata columnMetadata) {
        this.columnMetadata = (MergeColumnReadMetadata) columnMetadata;
    }

    @Override
    public IColumnTupleIterator createTupleIterator(ColumnBTreeReadLeafFrame frame, int componentIndex,
            IColumnReadMultiPageOp multiPageOp) {
        return new MergeColumnTupleReference(componentIndex, frame, columnMetadata, multiPageOp);
    }
}
