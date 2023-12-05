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
package org.apache.hyracks.storage.am.lsm.btree.column.api;

import java.io.Serializable;

import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.io.IJsonSerializable;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperation.LSMIOOperationType;

public interface IColumnManagerFactory extends Serializable, IJsonSerializable {
    /**
     * @return a new instance of {@link IColumnManager}
     */
    IColumnManager createColumnManager();

    /**
     * Get column tuple reader/writer for the {@link LSMIOOperationType#LOAD}
     *
     * @param cmpFactories Primary keys comparators' factories
     */
    AbstractColumnTupleReaderWriterFactory getLoadColumnTupleReaderWriterFactory(
            IBinaryComparatorFactory[] cmpFactories);

    /**
     * Get column tuple reader/writer for the {@link LSMIOOperationType#FLUSH}
     */
    AbstractColumnTupleReaderWriterFactory getFlushColumnTupleReaderWriterFactory();

    /**
     * Get column tuple reader/writer for the {@link LSMIOOperationType#MERGE}
     */
    AbstractColumnTupleReaderWriterFactory createMergeColumnTupleReaderWriterFactory();

}
