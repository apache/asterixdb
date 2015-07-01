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

package edu.uci.ics.asterix.common.context;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import edu.uci.ics.asterix.common.api.IAsterixAppRuntimeContext;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.storage.am.common.api.IIndexLifecycleManager;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMMergePolicy;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMMergePolicyFactory;

public class CorrelatedPrefixMergePolicyFactory implements ILSMMergePolicyFactory {

    private static final long serialVersionUID = 1L;

    private static final String[] SET_VALUES = new String[] { "max-mergable-component-size",
            "max-tolerance-component-count" };
    private static final Set<String> PROPERTIES_NAMES = new HashSet<String>(Arrays.asList(SET_VALUES));

    private int datasetID;

    @Override
    public ILSMMergePolicy createMergePolicy(Map<String, String> properties, IHyracksTaskContext ctx) {
        DatasetLifecycleManager dslcManager = (DatasetLifecycleManager) ((IAsterixAppRuntimeContext) ctx
                .getJobletContext().getApplicationContext().getApplicationObject()).getIndexLifecycleManager();
        ILSMMergePolicy policy = new CorrelatedPrefixMergePolicy(dslcManager, datasetID);
        policy.configure(properties);
        return policy;
    }

    @Override
    public String getName() {
        return "correlated-prefix";
    }

    @Override
    public Set<String> getPropertiesNames() {
        return PROPERTIES_NAMES;
    }

    @Override
    public ILSMMergePolicy createMergePolicy(Map<String, String> properties, IIndexLifecycleManager ilcm) {
        ILSMMergePolicy policy = new CorrelatedPrefixMergePolicy(ilcm, datasetID);
        policy.configure(properties);
        return policy;
    }

    public void setDatasetID(int datasetID) {
        this.datasetID = datasetID;
    }
}
