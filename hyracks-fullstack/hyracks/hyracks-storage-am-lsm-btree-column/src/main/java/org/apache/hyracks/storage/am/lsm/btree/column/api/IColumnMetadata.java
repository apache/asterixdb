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

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IValueReference;
import org.apache.hyracks.util.annotations.NotThreadSafe;

/**
 * A holder for the columnar metadata.
 * Modifications on the columnar metadata are not thread safe.
 */
@NotThreadSafe
public interface IColumnMetadata {
    /**
     * @return a serialized version of the columns metadata
     */
    IValueReference serializeColumnsMetadata() throws HyracksDataException;

    /**
     * @return number of columns
     */
    int getNumberOfColumns();

    /**
     * abort in case of an error. This should clean up any artifact
     */
    void abort() throws HyracksDataException;
}
