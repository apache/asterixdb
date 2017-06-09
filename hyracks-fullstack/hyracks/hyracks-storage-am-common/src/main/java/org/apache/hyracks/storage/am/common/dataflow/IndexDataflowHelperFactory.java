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
package org.apache.hyracks.storage.am.common.dataflow;

import org.apache.hyracks.api.application.INCServiceContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.api.io.FileSplit;
import org.apache.hyracks.dataflow.std.file.IFileSplitProvider;
import org.apache.hyracks.storage.am.common.api.IIndexDataflowHelper;
import org.apache.hyracks.storage.common.IStorageManager;

public class IndexDataflowHelperFactory implements IIndexDataflowHelperFactory {

    private static final long serialVersionUID = 1L;
    private final IStorageManager storageMgr;
    private final IFileSplitProvider fileSplitProvider;

    public IndexDataflowHelperFactory(IStorageManager storageMgr, IFileSplitProvider fileSplitProvider) {
        this.storageMgr = storageMgr;
        this.fileSplitProvider = fileSplitProvider;
    }

    @Override
    public IIndexDataflowHelper create(INCServiceContext ctx, int partition) throws HyracksDataException {
        FileSplit fileSplit = fileSplitProvider.getFileSplits()[partition];
        FileReference resourceRef = fileSplit.getFileReference(ctx.getIoManager());
        return new IndexDataflowHelper(ctx, storageMgr, resourceRef);
    }
}
