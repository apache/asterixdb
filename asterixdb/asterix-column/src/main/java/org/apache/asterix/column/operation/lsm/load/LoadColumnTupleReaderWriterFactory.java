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
package org.apache.asterix.column.operation.lsm.load;

import org.apache.asterix.column.operation.lsm.flush.FlushColumnMetadata;
import org.apache.asterix.column.operation.lsm.flush.FlushColumnTupleReaderWriterFactory;
import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.storage.am.lsm.btree.column.api.AbstractColumnTupleWriter;
import org.apache.hyracks.storage.am.lsm.btree.column.api.IColumnMetadata;
import org.apache.hyracks.storage.common.MultiComparator;

public class LoadColumnTupleReaderWriterFactory extends FlushColumnTupleReaderWriterFactory {
    private static final long serialVersionUID = -7583574057314353873L;
    private final IBinaryComparatorFactory[] cmpFactories;

    public LoadColumnTupleReaderWriterFactory(int pageSize, int maxNumberOfTuples, double tolerance,
            int maxLeafNodeSize, IBinaryComparatorFactory[] cmpFactories) {
        super(pageSize, maxNumberOfTuples, tolerance, maxLeafNodeSize);
        this.cmpFactories = cmpFactories;
    }

    @Override
    public AbstractColumnTupleWriter createColumnWriter(IColumnMetadata columnMetadata) {
        return new LoadColumnTupleWriter((FlushColumnMetadata) columnMetadata, pageSize, maxNumberOfTuples, tolerance,
                maxLeafNodeSize, MultiComparator.create(cmpFactories));
    }
}
