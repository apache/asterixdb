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

package org.apache.asterix.experiment.builder;

import org.apache.asterix.experiment.client.LSMExperimentSetRunner.LSMExperimentSetRunnerConfig;

public class SpatialIndexExperiment4DhbtreeBuilder extends AbstractSpatialIndexExperiment3SIdxCreateAndQueryBuilder {

    //SpatialIndexExperiment4XXX is exactly the same as SpatialIndexExperiment3XXX except queries are non-index only plan queries.
    public SpatialIndexExperiment4DhbtreeBuilder(LSMExperimentSetRunnerConfig config) {
        super("SpatialIndexExperiment4Dhbtree", config, "8node.xml", "base_8_ingest.aql", "8.dqgen", "count.aql",
                "spatial_3_create_dhbtree.aql", false);
    }
}
