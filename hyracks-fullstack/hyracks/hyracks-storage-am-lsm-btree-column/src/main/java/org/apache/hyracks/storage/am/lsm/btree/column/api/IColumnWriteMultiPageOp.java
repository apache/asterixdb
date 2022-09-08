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

import java.nio.ByteBuffer;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.storage.common.buffercache.IBufferCache;

/**
 * A proxy to call {@link IBufferCache} writing methods
 * <p>
 * An instance of this interface is responsible for returning all confiscated pages back to {@link IBufferCache} upon
 * failures. Temporary buffers should be returned to the {@link IBufferCache} once the multi-page operation is finished.
 * <p>
 * Users of an instance of this interface should not expect the temporary buffers will last after the multi-page
 * operation is finished.
 */
public interface IColumnWriteMultiPageOp {
    /**
     * @return a buffer that correspond to a page in a file
     */
    ByteBuffer confiscatePersistent() throws HyracksDataException;

    /**
     * Persist all confiscated persistent buffers to disk
     */
    void persist() throws HyracksDataException;

    /**
     * @return the number confiscated persistent pages
     */
    int getNumberOfPersistentBuffers();

    /**
     * @return a {@link IBufferCache}-backed buffer for temporary use
     */
    ByteBuffer confiscateTemporary() throws HyracksDataException;
}
