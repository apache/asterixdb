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
package org.apache.hyracks.cloud.cache.unit;

import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndex;
import org.apache.hyracks.storage.common.LocalResource;

/**
 * A row-based index. It is not swept column by column, but its files can still be punched by a
 * {@link org.apache.hyracks.cloud.sweeper.RowIndexSweeper} where they allow holes.
 */
public final class UnsweepableIndexUnit extends AbstractIndexUnit {
    private final ILSMIndex index;

    public UnsweepableIndexUnit(LocalResource localResource, ILSMIndex index) {
        super(localResource);
        this.index = index;
    }

    @Override
    public void drop() {
        // NoOp
    }

    @Override
    protected boolean isSweepable() {
        return false;
    }

    public ILSMIndex getIndex() {
        return index;
    }

    public String getPath() {
        return localResource.getPath();
    }
}
