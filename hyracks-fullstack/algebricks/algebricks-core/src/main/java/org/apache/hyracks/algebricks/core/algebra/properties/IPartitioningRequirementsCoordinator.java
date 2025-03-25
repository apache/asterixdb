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

import java.util.Map;
import java.util.Set;

import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.common.exceptions.NotImplementedException;
import org.apache.hyracks.algebricks.common.utils.ListSet;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.algebricks.core.algebra.base.EquivalenceClass;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.api.exceptions.ErrorCode;

/**
 * Implements constraints in between requirements for the children of the same
 * operator.
 */

public interface IPartitioningRequirementsCoordinator {

    public static IPartitioningRequirementsCoordinator NO_COORDINATION = new IPartitioningRequirementsCoordinator() {

        @Override
        public Pair<Boolean, IPartitioningProperty> coordinateRequirements(IPartitioningProperty requirements,
                IPartitioningProperty firstDeliveredPartitioning, ILogicalOperator op, IOptimizationContext context) {
            return new Pair<>(true, requirements);
        }
    };

    public static IPartitioningRequirementsCoordinator EQCLASS_PARTITIONING_COORDINATOR =
            new IPartitioningRequirementsCoordinator() {

                @Override
                public Pair<Boolean, IPartitioningProperty> coordinateRequirements(IPartitioningProperty rqdpp,
                        IPartitioningProperty firstDeliveredPartitioning, ILogicalOperator op,
                        IOptimizationContext context) throws AlgebricksException {
                    if (firstDeliveredPartitioning != null && rqdpp != null
                            && firstDeliveredPartitioning.getPartitioningType() == rqdpp.getPartitioningType()) {
                        switch (rqdpp.getPartitioningType()) {
                            case UNORDERED_PARTITIONED: {
                                UnorderedPartitionedProperty unorderedRequired = (UnorderedPartitionedProperty) rqdpp;
                                UnorderedPartitionedProperty unorderedFirstDelivered =
                                        (UnorderedPartitionedProperty) firstDeliveredPartitioning;
                                Set<LogicalVariable> firstDeliveredVars = unorderedFirstDelivered.getColumnSet();
                                Set<LogicalVariable> originalRequiredVars = unorderedRequired.getColumnSet();
                                Set<LogicalVariable> modifiedRequiredVars = new ListSet<>();
                                Map<LogicalVariable, EquivalenceClass> eqmap = context.getEquivalenceClassMap(op);
                                Set<LogicalVariable> coveredVars = new ListSet<>();

                                // coordinate from an existing partition property (firstDeliveredPartitioning)
                                for (LogicalVariable v : firstDeliveredVars) {
                                    EquivalenceClass ecFirst = eqmap.get(v);
                                    for (LogicalVariable r : originalRequiredVars) {
                                        if (!modifiedRequiredVars.contains(r)) {
                                            EquivalenceClass ec = eqmap.get(r);
                                            if (ecFirst == ec) {
                                                coveredVars.add(v);
                                                modifiedRequiredVars.add(r);
                                                break;
                                            }
                                        }
                                    }
                                }

                                if (!coveredVars.equals(firstDeliveredVars)) {
                                    throw new AlgebricksException(ErrorCode.ILLEGAL_STATE,
                                            "Could not modify the original required partitioning " + rqdpp
                                                    + " to agree with the partitioning property "
                                                    + firstDeliveredPartitioning
                                                    + " delivered by previous input operator. Covered vars: "
                                                    + coveredVars + ". Modified required vars: "
                                                    + modifiedRequiredVars);
                                }

                                if (modifiedRequiredVars.size() != firstDeliveredVars.size()) {
                                    throw new AlgebricksException(ErrorCode.ILLEGAL_STATE,
                                            "The number of variables are not equal in both partitioning sides. First delivered: "
                                                    + firstDeliveredVars + ". Modified required: "
                                                    + modifiedRequiredVars);
                                }

                                UnorderedPartitionedProperty unorderedFinalRequired;
                                if (unorderedRequired.usesPartitionsMap()) {
                                    unorderedFinalRequired = UnorderedPartitionedProperty.ofPartitionsMap(
                                            modifiedRequiredVars, unorderedRequired.getNodeDomain(),
                                            unorderedRequired.getPartitionsMap());
                                } else {
                                    unorderedFinalRequired = UnorderedPartitionedProperty.of(modifiedRequiredVars,
                                            unorderedRequired.getNodeDomain());
                                }
                                return new Pair<>(false, unorderedFinalRequired);
                            }
                            case ORDERED_PARTITIONED: {
                                throw new NotImplementedException();
                            }
                        }
                    }
                    return new Pair<>(true, rqdpp);
                }

            };

    public Pair<Boolean, IPartitioningProperty> coordinateRequirements(IPartitioningProperty requirements,
            IPartitioningProperty firstDeliveredPartitioning, ILogicalOperator op, IOptimizationContext context)
            throws AlgebricksException;
}
