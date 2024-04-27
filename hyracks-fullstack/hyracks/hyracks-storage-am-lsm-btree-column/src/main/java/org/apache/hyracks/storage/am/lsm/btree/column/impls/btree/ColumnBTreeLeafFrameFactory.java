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
package org.apache.hyracks.storage.am.lsm.btree.column.impls.btree;

import org.apache.hyracks.storage.am.btree.frames.BTreeNSMLeafFrame;
import org.apache.hyracks.storage.am.common.api.ITreeIndexFrame;
import org.apache.hyracks.storage.am.common.api.ITreeIndexFrameFactory;
import org.apache.hyracks.storage.am.common.api.ITreeIndexTupleWriter;
import org.apache.hyracks.storage.am.common.api.ITreeIndexTupleWriterFactory;
import org.apache.hyracks.storage.am.lsm.btree.column.api.AbstractColumnTupleReader;
import org.apache.hyracks.storage.am.lsm.btree.column.api.AbstractColumnTupleReaderWriterFactory;
import org.apache.hyracks.storage.am.lsm.btree.column.api.AbstractColumnTupleWriter;
import org.apache.hyracks.storage.am.lsm.btree.column.api.IColumnMetadata;
import org.apache.hyracks.storage.am.lsm.btree.column.api.projection.IColumnProjectionInfo;
import org.apache.hyracks.storage.am.lsm.btree.column.cloud.buffercache.IColumnWriteContext;

public class ColumnBTreeLeafFrameFactory implements ITreeIndexFrameFactory {
    private static final long serialVersionUID = 4136035898137820322L;
    private final ITreeIndexTupleWriterFactory rowTupleWriterFactory;
    private final AbstractColumnTupleReaderWriterFactory columnTupleWriterFactory;

    public ColumnBTreeLeafFrameFactory(ITreeIndexTupleWriterFactory rowTupleWriterFactory,
            AbstractColumnTupleReaderWriterFactory columnTupleWriterFactory) {
        this.rowTupleWriterFactory = rowTupleWriterFactory;
        this.columnTupleWriterFactory = columnTupleWriterFactory;
    }

    @Override
    public ITreeIndexFrame createFrame() {
        //Create a dummy leaf frame
        return new BTreeNSMLeafFrame(rowTupleWriterFactory.createTupleWriter());
    }

    @Override
    public ITreeIndexTupleWriterFactory getTupleWriterFactory() {
        return rowTupleWriterFactory;
    }

    public ColumnBTreeWriteLeafFrame createWriterFrame(IColumnMetadata columnMetadata,
            IColumnWriteContext writeContext) {
        ITreeIndexTupleWriter rowTupleWriter = rowTupleWriterFactory.createTupleWriter();
        AbstractColumnTupleWriter columnTupleWriter =
                columnTupleWriterFactory.createColumnWriter(columnMetadata, writeContext);
        return new ColumnBTreeWriteLeafFrame(rowTupleWriter, columnTupleWriter);
    }

    public ColumnBTreeReadLeafFrame createReadFrame(IColumnProjectionInfo columnProjectionInfo) {
        ITreeIndexTupleWriter rowTupleWriter = rowTupleWriterFactory.createTupleWriter();
        AbstractColumnTupleReader columnTupleReader = columnTupleWriterFactory.createColumnReader(columnProjectionInfo);
        return new ColumnBTreeReadLeafFrame(rowTupleWriter, columnTupleReader);
    }
}
