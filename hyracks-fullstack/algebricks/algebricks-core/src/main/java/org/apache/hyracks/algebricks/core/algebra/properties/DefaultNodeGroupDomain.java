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
package org.apache.hyracks.algebricks.core.algebra.properties;

import java.util.List;

import org.apache.commons.collections4.MultiSet;
import org.apache.commons.collections4.multiset.HashMultiSet;
import org.apache.hyracks.algebricks.common.constraints.AlgebricksAbsolutePartitionConstraint;
import org.apache.hyracks.algebricks.common.constraints.AlgebricksPartitionConstraint;
import org.apache.hyracks.algebricks.common.constraints.AlgebricksPartitionConstraint.PartitionConstraintType;

public class DefaultNodeGroupDomain implements INodeDomain {

    private MultiSet<String> nodes = new HashMultiSet<>();

    public DefaultNodeGroupDomain(List<String> nodes) {
        this.nodes.addAll(nodes);
    }

    public DefaultNodeGroupDomain(DefaultNodeGroupDomain domain) {
        this.nodes.addAll(domain.nodes);
    }

    public DefaultNodeGroupDomain(AlgebricksPartitionConstraint clusterLocations) {
        if (clusterLocations.getPartitionConstraintType() == PartitionConstraintType.ABSOLUTE) {
            AlgebricksAbsolutePartitionConstraint absPc = (AlgebricksAbsolutePartitionConstraint) clusterLocations;
            String[] locations = absPc.getLocations();
            for (String location : locations) {
                nodes.add(location);
            }
        } else {
            throw new IllegalStateException("A node domain can only take absolute location constraints.");
        }
    }

    @Override
    public boolean sameAs(INodeDomain domain) {
        if (!(domain instanceof DefaultNodeGroupDomain)) {
            return false;
        }
        DefaultNodeGroupDomain nodeDomain = (DefaultNodeGroupDomain) domain;
        return nodes.equals(nodeDomain.nodes);
    }

    @Override
    public String toString() {
        return nodes.toString();
    }

    @Override
    public Integer cardinality() {
        return nodes.size();
    }

    public String[] getNodes() {
        return nodes.toArray(new String[0]);
    }
}
