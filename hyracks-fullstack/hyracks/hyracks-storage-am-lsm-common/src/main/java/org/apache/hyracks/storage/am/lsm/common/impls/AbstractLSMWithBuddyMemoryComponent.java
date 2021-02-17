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
package org.apache.hyracks.storage.am.lsm.common.impls;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.storage.am.common.impls.AbstractTreeIndex;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMComponentFilter;
import org.apache.hyracks.storage.am.lsm.common.api.IVirtualBufferCache;

public abstract class AbstractLSMWithBuddyMemoryComponent extends AbstractLSMMemoryComponent {

    public AbstractLSMWithBuddyMemoryComponent(AbstractLSMIndex lsmIndex, IVirtualBufferCache vbc,
            ILSMComponentFilter filter) {
        super(lsmIndex, vbc, filter);
    }

    public abstract AbstractTreeIndex getBuddyIndex();

    @Override
    public void cleanup() throws HyracksDataException {
        if (allocated.get()) {
            super.cleanup();
            getBuddyIndex().deactivate();
            getBuddyIndex().destroy();
        }
    }

    @Override
    public void doAllocate() throws HyracksDataException {
        super.doAllocate();
        getBuddyIndex().create();
        getBuddyIndex().activate();
    }

    @Override
    public void doDeallocate() throws HyracksDataException {
        if (allocated.get()) {
            super.doDeallocate();
            getBuddyIndex().deactivate();
            getBuddyIndex().destroy();
        }
    }

    @Override
    public void validate() throws HyracksDataException {
        super.validate();
        getBuddyIndex().validate();
    }
}
