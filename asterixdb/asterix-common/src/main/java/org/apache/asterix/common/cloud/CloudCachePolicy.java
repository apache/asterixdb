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
package org.apache.asterix.common.cloud;

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

public enum CloudCachePolicy {
    EAGER("eager"),
    LAZY("lazy"),
    SELECTIVE("selective");
    private static final Map<String, CloudCachePolicy> partitioningSchemes =
            Collections.unmodifiableMap(Arrays.stream(CloudCachePolicy.values())
                    .collect(Collectors.toMap(CloudCachePolicy::getPolicyName, Function.identity())));

    private final String policyName;

    CloudCachePolicy(String policyName) {
        this.policyName = policyName;
    }

    public String getPolicyName() {
        return policyName;
    }

    public static CloudCachePolicy fromName(String policyName) {
        CloudCachePolicy partitioningScheme = partitioningSchemes.get(policyName.toLowerCase());
        if (partitioningScheme == null) {
            throw new IllegalArgumentException("unknown cloud cache policy: " + policyName);
        }
        return partitioningScheme;
    }
}
