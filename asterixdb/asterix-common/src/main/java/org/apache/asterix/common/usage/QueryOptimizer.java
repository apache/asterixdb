/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 *
 */
package org.apache.asterix.common.usage;

import java.util.Locale;

/** Which optimizer chose the join order and access paths of a statement's plan. */
public enum QueryOptimizer {
    /** the cost-based optimizer */
    CBO,
    /** the rule-based optimizer, which CBO falls back to for a {@link RboReason} */
    RBO;

    private final String label = name().toLowerCase(Locale.ROOT);

    public String label() {
        return label;
    }

    /**
     * Why the cost-based optimizer left a plan to the rule-based one. Declared in order of precedence: a statement
     * whose plan fell back for more than one reason is reported with the first.
     */
    public enum RboReason {
        /** CBO is turned off */
        CBO_DISABLED,
        /** a collection the plan reads has no samples, which ANALYZE creates */
        MISSING_SAMPLES,
        /** the plan joins more collections than the configured maximum for CBO */
        TOO_MANY_JOINS,
        /** CBO gave up on the plan for some other reason, such as a join shape it cannot enumerate */
        OTHER;

        private final String label = name().toLowerCase(Locale.ROOT);

        public String label() {
            return label;
        }
    }
}
