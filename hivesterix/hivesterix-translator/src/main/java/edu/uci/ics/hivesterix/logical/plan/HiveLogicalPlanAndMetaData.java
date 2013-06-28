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
package edu.uci.ics.hivesterix.logical.plan;

import edu.uci.ics.hyracks.algebricks.common.constraints.AlgebricksPartitionConstraint;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalPlan;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalPlanAndMetadata;
import edu.uci.ics.hyracks.algebricks.core.algebra.metadata.IMetadataProvider;

@SuppressWarnings({ "rawtypes", "unchecked" })
public class HiveLogicalPlanAndMetaData implements ILogicalPlanAndMetadata {

    IMetadataProvider metadata;
    ILogicalPlan plan;

    public HiveLogicalPlanAndMetaData(ILogicalPlan plan, IMetadataProvider metadata) {
        this.plan = plan;
        this.metadata = metadata;
    }

    @Override
    public IMetadataProvider getMetadataProvider() {
        return metadata;
    }

    @Override
    public ILogicalPlan getPlan() {
        return plan;
    }

    @Override
    public AlgebricksPartitionConstraint getClusterLocations() {
        // TODO Auto-generated method stub
        return null;
    }

}
