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

package org.apache.asterix.metadata.entities;

import org.apache.asterix.metadata.MetadataCache;
import org.apache.asterix.metadata.api.IMetadataEntity;

/**
 * Metadata describing a compaction policy record.
 */
public class CompactionPolicy implements IMetadataEntity<CompactionPolicy> {

    private static final long serialVersionUID = 1L;

    private final String dataverseName;
    // Enforced to be unique within a dataverse.
    private final String policyName;
    private final String className;

    public CompactionPolicy(String dataverseName, String policyName, String className) {
        this.dataverseName = dataverseName;
        this.policyName = policyName;
        this.className = className;
    }

    public String getDataverseName() {
        return dataverseName;
    }

    public String getPolicyName() {
        return policyName;
    }

    public String getClassName() {
        return className;
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (!(other instanceof CompactionPolicy)) {
            return false;
        }
        CompactionPolicy otherPolicy = (CompactionPolicy) other;
        if (!otherPolicy.dataverseName.equals(dataverseName)) {
            return false;
        }
        if (!otherPolicy.policyName.equals(policyName)) {
            return false;
        }
        return true;
    }

    @Override
    public CompactionPolicy addToCache(MetadataCache cache) {
        return cache.addCompactionPolicyIfNotExists(this);
    }

    @Override
    public CompactionPolicy dropFromCache(MetadataCache cache) {
        return cache.dropCompactionPolicy(this);
    }
}
