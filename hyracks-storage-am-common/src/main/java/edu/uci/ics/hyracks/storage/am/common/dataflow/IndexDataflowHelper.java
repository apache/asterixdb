/*
 * Copyright 2009-2010 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package edu.uci.ics.hyracks.storage.am.common.dataflow;

import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.io.FileReference;
import edu.uci.ics.hyracks.storage.am.common.api.IIndexDataflowHelper;
import edu.uci.ics.hyracks.storage.common.file.IIndexArtifactMap;

public abstract class IndexDataflowHelper implements IIndexDataflowHelper {

    protected final IIndexOperatorDescriptor opDesc;
    protected final IHyracksTaskContext ctx;
    protected final IIndexArtifactMap indexArtifactMap;
    protected final FileReference file;

    protected IIndex index;

    public IndexDataflowHelper(IIndexOperatorDescriptor opDesc, final IHyracksTaskContext ctx, int partition) {
        this.opDesc = opDesc;
        this.ctx = ctx;
        this.indexArtifactMap = opDesc.getStorageManager().getIndexArtifactMap(ctx);
        this.file = opDesc.getFileSplitProvider().getFileSplits()[partition].getLocalFile();
    }

    public abstract IIndex getIndexInstance() throws HyracksDataException;

    @Override
    public FileReference getFileReference() {
        return file;
    }

    public IIndex getIndex() {
        return index;
    }

    public long getResourceID() {
        return indexArtifactMap.get(file.getFile().getPath());
    }

    public IIndexOperatorDescriptor getOperatorDescriptor() {
        return opDesc;
    }

    public IHyracksTaskContext getHyracksTaskContext() {
        return ctx;
    }
}
