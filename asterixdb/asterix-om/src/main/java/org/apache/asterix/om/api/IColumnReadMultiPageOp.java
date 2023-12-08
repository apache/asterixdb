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
package org.apache.asterix.om.api;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.storage.common.buffercache.IBufferCache;
import org.apache.hyracks.storage.common.buffercache.ICachedPage;

/**
 * A proxy to call {@link IBufferCache} read columns' pages
 * Implementer should be aware to unpin all pages in case of an error
 */
public interface IColumnReadMultiPageOp {
    /**
     * Pin a column page
     *
     * @return a page that belongs to a column
     */
    ICachedPage pin(int pageId) throws HyracksDataException;

    /**
     * Unpin a pinned column page
     */
    void unpin(ICachedPage page) throws HyracksDataException;

    /**
     * Return {@link IBufferCache} page size
     *
     * @see IBufferCache#getPageSize()
     */
    int getPageSize();
}
