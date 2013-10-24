/*
 * Copyright 2009-2013 by The Regents of the University of California
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
package edu.uci.ics.asterix.transaction.management.opcallbacks;

import edu.uci.ics.asterix.common.api.IAsterixAppRuntimeContext;
import edu.uci.ics.asterix.common.context.BaseOperationTracker;
import edu.uci.ics.asterix.common.context.DatasetLifecycleManager;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMOperationTracker;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMOperationTrackerProvider;

public class SecondaryIndexOperationTrackerProvider implements ILSMOperationTrackerProvider {

    private static final long serialVersionUID = 1L;

    private final int datasetID;

    public SecondaryIndexOperationTrackerProvider(int datasetID) {
        this.datasetID = datasetID;
    }

    @Override
    public ILSMOperationTracker getOperationTracker(IHyracksTaskContext ctx) {
        DatasetLifecycleManager dslcManager = (DatasetLifecycleManager) ((IAsterixAppRuntimeContext) ctx
                .getJobletContext().getApplicationContext().getApplicationObject()).getIndexLifecycleManager();
        return new BaseOperationTracker(dslcManager, datasetID);
    }

}
