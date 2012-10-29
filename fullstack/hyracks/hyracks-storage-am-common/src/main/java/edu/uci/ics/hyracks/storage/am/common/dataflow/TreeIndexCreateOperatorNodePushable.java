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

import edu.uci.ics.hyracks.api.comm.IFrameWriter;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractOperatorNodePushable;

public class TreeIndexCreateOperatorNodePushable extends AbstractOperatorNodePushable {
    protected final TreeIndexDataflowHelper treeIndexHelper;

    public TreeIndexCreateOperatorNodePushable(AbstractTreeIndexOperatorDescriptor opDesc, IHyracksTaskContext ctx,
            int partition) {
        treeIndexHelper = (TreeIndexDataflowHelper) opDesc.getIndexDataflowHelperFactory().createIndexDataflowHelper(
                opDesc, ctx, partition);
    }

    @Override
    public void deinitialize() throws HyracksDataException {
    }

    @Override
    public int getInputArity() {
        return 0;
    }

    @Override
    public IFrameWriter getInputFrameWriter(int index) {
        return null;
    }

    @Override
    public void initialize() throws HyracksDataException {
        try {
        	treeIndexHelper.init(true);
        } finally {
        	treeIndexHelper.deinit();
        }
    }

    @Override
    public void setOutputFrameWriter(int index, IFrameWriter writer, RecordDescriptor recordDesc) {
    }
}