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
package org.apache.asterix.app.external;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.asterix.common.external.IExternalStatsTracker;
import org.apache.asterix.external.util.ExternalDataConstants;

public class ExternalStatsTracker implements IExternalStatsTracker {

    private final Map<String, Map<String, AtomicLong>> totalAwsAssumeRoleFailures;

    public ExternalStatsTracker() {
        totalAwsAssumeRoleFailures = new ConcurrentHashMap<>();
    }

    @Override
    public String resolveName(Map<String, String> configuration) {
        // if dataset name is not in the configuration, it means we are still creating the dataset, return empty string
        if (!configuration.containsKey(ExternalDataConstants.KEY_DATASET)) {
            return "";
        }

        String database = configuration.get(ExternalDataConstants.KEY_DATASET_DATABASE);
        String dataverse = configuration.get(ExternalDataConstants.KEY_DATASET_DATAVERSE);
        String dataset = configuration.get(ExternalDataConstants.KEY_DATASET);
        String name;
        if (database != null && !database.isEmpty()) {
            name = database + "." + dataverse + "." + dataset;
        } else {
            name = dataverse + "." + dataset;
        }
        return name;
    }

    @Override
    public void incrementAwsAssumeRoleFailure(String name, String roleArn) {
        // empty name means we are creating the external collection still
        if (name.isEmpty()) {
            return;
        }
        Map<String, AtomicLong> map =
                totalAwsAssumeRoleFailures.computeIfAbsent(name, key -> new ConcurrentHashMap<>());
        map.computeIfAbsent(roleArn, key -> new AtomicLong()).incrementAndGet();
    }
}
