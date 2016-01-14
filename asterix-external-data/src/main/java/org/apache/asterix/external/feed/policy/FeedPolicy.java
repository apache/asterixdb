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
package org.apache.asterix.external.feed.policy;

import java.io.Serializable;
import java.util.Map;

public class FeedPolicy implements Serializable {

    private static final long serialVersionUID = 1L;
    private final String dataverseName;
    // Enforced to be unique within a dataverse.
    private final String policyName;
    // A description of the policy
    private final String description;
    // The policy properties associated with the feed dataset
    private Map<String, String> properties;

    public FeedPolicy(String dataverseName, String policyName, String description, Map<String, String> properties) {
        this.dataverseName = dataverseName;
        this.policyName = policyName;
        this.description = description;
        this.properties = properties;
    }

    public String getDataverseName() {
        return dataverseName;
    }

    public String getPolicyName() {
        return policyName;
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (!(other instanceof FeedPolicy)) {
            return false;
        }
        FeedPolicy otherPolicy = (FeedPolicy) other;
        if (!otherPolicy.dataverseName.equals(dataverseName)) {
            return false;
        }
        if (!otherPolicy.policyName.equals(policyName)) {
            return false;
        }
        return true;
    }

    public String getDescription() {
        return description;
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    public void setProperties(Map<String, String> properties) {
        this.properties = properties;
    }

}
