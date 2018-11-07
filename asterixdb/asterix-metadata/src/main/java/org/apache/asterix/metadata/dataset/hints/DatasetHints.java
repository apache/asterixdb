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
package org.apache.asterix.metadata.dataset.hints;

import java.util.HashSet;
import java.util.Set;

import org.apache.asterix.common.dataflow.ICcApplicationContext;
import org.apache.hyracks.algebricks.common.utils.Pair;

/**
 * Collection of hints supported by create dataset statement.
 * Includes method to validate a hint and its associated value
 * as provided in a create dataset statement.
 */
public class DatasetHints {

    private DatasetHints() {
    }

    /**
     * validate the use of a hint
     *
     * @param hintName
     *            name of the hint
     * @param value
     *            value associated with the hint
     * @return a Pair with
     *         first element as a boolean that represents the validation result.
     *         second element as the error message if the validation result is false
     */
    public static Pair<Boolean, String> validate(ICcApplicationContext appCtx, String hintName, String value) {
        for (IHint h : hints) {
            if (h.getName().equalsIgnoreCase(hintName.trim())) {
                return h.validateValue(appCtx, value);
            }
        }
        return new Pair<>(false, "Unknown hint :" + hintName);
    }

    private static Set<IHint> hints = initHints();

    private static Set<IHint> initHints() {
        Set<IHint> hints = new HashSet<>();
        hints.add(new DatasetCardinalityHint());
        hints.add(new DatasetNodegroupCardinalityHint());
        return hints;
    }

    /**
     * Hint representing the expected number of tuples in the dataset.
     */
    public static class DatasetCardinalityHint implements IHint {
        public static final String NAME = "CARDINALITY";

        public static final long DEFAULT = 1000000L;

        @Override
        public String getName() {
            return NAME;
        }

        @Override
        public Pair<Boolean, String> validateValue(ICcApplicationContext appCtx, String value) {
            boolean valid = true;
            long longValue;
            try {
                longValue = Long.parseLong(value);
                if (longValue < 0) {
                    return new Pair<>(false, "Value must be >= 0");
                }
            } catch (NumberFormatException nfe) {
                valid = false;
                return new Pair<>(valid, "Inappropriate value");
            }
            return new Pair<>(true, null);
        }

    }

    /**
     * Hint representing the cardinality of nodes forming the nodegroup for the dataset.
     */
    public static class DatasetNodegroupCardinalityHint implements IHint {
        public static final String NAME = "NODEGROUP_CARDINALITY";

        public static final int DEFAULT = 1;

        @Override
        public String getName() {
            return NAME;
        }

        @Override
        public Pair<Boolean, String> validateValue(ICcApplicationContext appCtx, String value) {
            int intValue;
            try {
                intValue = Integer.parseInt(value);
                if (intValue < 0) {
                    return new Pair<>(false, "Value must be >= 0");
                }
                int numNodesInCluster = appCtx.getClusterStateManager().getParticipantNodes(true).size();
                if (numNodesInCluster < intValue) {
                    return new Pair<>(false,
                            "Value must be less than or equal to the available number of nodes in cluster ("
                                    + numNodesInCluster + ")");
                }
            } catch (NumberFormatException nfe) {
                return new Pair<>(false, "Inappropriate value");
            }
            return new Pair<>(true, null);
        }

    }

}
