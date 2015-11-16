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

package org.apache.asterix.common.context;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.asterix.common.api.IAsterixAppRuntimeContext;
import org.apache.asterix.common.api.IDatasetLifecycleManager;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.storage.am.common.api.IIndexLifecycleManager;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMMergePolicy;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMMergePolicyFactory;

public class CorrelatedPrefixMergePolicyFactory implements ILSMMergePolicyFactory {

    private static final long serialVersionUID = 1L;

    private static final String[] SET_VALUES = new String[] { "max-mergable-component-size",
            "max-tolerance-component-count" };
    private static final Set<String> PROPERTIES_NAMES = new HashSet<String>(Arrays.asList(SET_VALUES));

    private int datasetID;

    @Override
    public ILSMMergePolicy createMergePolicy(Map<String, String> properties, IHyracksTaskContext ctx) {
        IDatasetLifecycleManager dslcManager = ((IAsterixAppRuntimeContext) ctx.getJobletContext()
                .getApplicationContext().getApplicationObject()).getDatasetLifecycleManager();
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
