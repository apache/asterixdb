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
package org.apache.hyracks.storage.common.buffercache;

import org.apache.hyracks.api.exceptions.HyracksDataException;

public interface IPageReplacementStrategy {
    public Object createPerPageStrategyObject(int cpid);

    public void setBufferCache(IBufferCacheInternal bufferCache);

    public IBufferCacheInternal getBufferCache();

    public void notifyCachePageReset(ICachedPageInternal cPage);

    public void notifyCachePageAccess(ICachedPageInternal cPage);

    public void adviseWontNeed(ICachedPageInternal cPage);

    public ICachedPageInternal findVictim();

    public ICachedPageInternal findVictim(int multiplier);

    public int getNumPages();

    void fixupCapacityOnLargeRead(ICachedPageInternal cPage) throws HyracksDataException;

    public int getPageSize();

    public int getMaxAllowedNumPages();

    void resizePage(ICachedPageInternal page, int multiplier, IExtraPageBlockHelper extraPageBlockHelper)
            throws HyracksDataException;
}
