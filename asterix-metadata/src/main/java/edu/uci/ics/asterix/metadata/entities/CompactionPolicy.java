/*
 * Copyright 2009-2010 by The Regents of the University of California
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

package edu.uci.ics.asterix.metadata.entities;

import edu.uci.ics.asterix.metadata.MetadataCache;
import edu.uci.ics.asterix.metadata.api.IMetadataEntity;

/**
 * Metadata describing a compaction policy record.
 */
public class CompactionPolicy implements IMetadataEntity {

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
    public Object addToCache(MetadataCache cache) {
        return cache.addCompactionPolicyIfNotExists(this);
    }

    @Override
    public Object dropFromCache(MetadataCache cache) {
        return cache.dropCompactionPolicy(this);
    }
}