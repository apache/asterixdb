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

package edu.uci.ics.asterix.metadata.declared;

import java.util.ArrayList;
import java.util.Map;

import edu.uci.ics.hyracks.algebricks.common.constraints.AlgebricksAbsolutePartitionConstraint;
import edu.uci.ics.hyracks.algebricks.common.constraints.AlgebricksPartitionConstraint;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalPlan;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalPlanAndMetadata;

public class AqlLogicalPlanAndMetadataImpl implements ILogicalPlanAndMetadata {

    private ILogicalPlan plan;
    private AqlMetadataProvider metadataProvider;

    public AqlLogicalPlanAndMetadataImpl(ILogicalPlan plan, AqlMetadataProvider metadataProvider) {
        this.plan = plan;
        this.metadataProvider = metadataProvider;
    }

    @Override
    public ILogicalPlan getPlan() {
        return plan;
    }

    @Override
    public AqlMetadataProvider getMetadataProvider() {
        return metadataProvider;
    }

    @Override
    public AlgebricksPartitionConstraint getClusterLocations() {
        Map<String, String[]> stores = metadataProvider.getAllStores();
        ArrayList<String> locs = new ArrayList<String>();
        for (String k : stores.keySet()) {
            String[] nodeStores = stores.get(k);
            for (int j = 0; j < nodeStores.length; j++) {
                locs.add(k);
            }
        }
        String[] cluster = new String[locs.size()];
        cluster = locs.toArray(cluster);
        return new AlgebricksAbsolutePartitionConstraint(cluster);
    }
}
