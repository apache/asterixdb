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

package org.apache.asterix.metadata.declared;

import org.apache.hyracks.algebricks.common.constraints.AlgebricksPartitionConstraint;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalPlan;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalPlanAndMetadata;

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
        return metadataProvider.getClusterLocations();
    }
}
