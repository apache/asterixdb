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

import org.apache.asterix.column.metadata.AbstractColumnImmutableReadMetadata;
import org.apache.hyracks.storage.am.lsm.btree.column.api.AbstractColumnTupleReader;
import org.apache.hyracks.storage.am.lsm.btree.column.api.AbstractColumnTupleReaderWriterFactory;
import org.apache.hyracks.storage.am.lsm.btree.column.api.AbstractColumnTupleWriter;
import org.apache.hyracks.storage.am.lsm.btree.column.api.IColumnMetadata;
import org.apache.hyracks.storage.am.lsm.btree.column.api.projection.IColumnProjectionInfo;

public class FlushColumnTupleReaderWriterFactory extends AbstractColumnTupleReaderWriterFactory {
    private static final long serialVersionUID = -9197679192729634493L;

    public FlushColumnTupleReaderWriterFactory(int pageSize, int maxNumberOfTuples, double tolerance,
            int maxLeafNodeSize) {
        super(pageSize, maxNumberOfTuples, tolerance, maxLeafNodeSize);
    }

    @Override
    public AbstractColumnTupleWriter createColumnWriter(IColumnMetadata columnMetadata) {
        FlushColumnMetadata flushColumnMetadata = (FlushColumnMetadata) columnMetadata;
        if (flushColumnMetadata.getMetaType() == null) {
            //no meta
            return new FlushColumnTupleWriter(flushColumnMetadata, pageSize, maxNumberOfTuples, tolerance,
                    maxLeafNodeSize);
        }
        return new FlushColumnTupleWithMetaWriter(flushColumnMetadata, pageSize, maxNumberOfTuples, tolerance,
                maxLeafNodeSize);
    }

    @Override
    public AbstractColumnTupleReader createColumnReader(IColumnProjectionInfo columnProjectionInfo) {
        return ((AbstractColumnImmutableReadMetadata) columnProjectionInfo).createTupleReader();
    }
}
