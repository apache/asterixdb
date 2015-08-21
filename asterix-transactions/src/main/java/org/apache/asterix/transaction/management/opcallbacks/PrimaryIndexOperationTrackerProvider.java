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

package org.apache.asterix.transaction.management.opcallbacks;

import org.apache.asterix.common.api.IAsterixAppRuntimeContext;
import org.apache.asterix.common.context.DatasetLifecycleManager;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMOperationTracker;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMOperationTrackerProvider;

public class PrimaryIndexOperationTrackerProvider implements ILSMOperationTrackerProvider {

    private static final long serialVersionUID = 1L;

    private final int datasetID;

    public PrimaryIndexOperationTrackerProvider(int datasetID) {
        this.datasetID = datasetID;
    }

    @Override
    public ILSMOperationTracker getOperationTracker(IHyracksTaskContext ctx) {
        DatasetLifecycleManager dslcManager = (DatasetLifecycleManager) ((IAsterixAppRuntimeContext) ctx
                .getJobletContext().getApplicationContext().getApplicationObject()).getIndexLifecycleManager();
        return dslcManager.getOperationTracker(datasetID);
    }

}
