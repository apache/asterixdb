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

package org.apache.hyracks.api.job;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.hyracks.api.deployment.DeploymentId;

public class JobSerializerDeserializerContainer implements IJobSerializerDeserializerContainer {

    private IJobSerializerDeserializer defaultJobSerDe = new JobSerializerDeserializer();
    private Map<DeploymentId, IJobSerializerDeserializer> jobSerializerDeserializerMap =
            new ConcurrentHashMap<DeploymentId, IJobSerializerDeserializer>();

    @Override
    public synchronized IJobSerializerDeserializer getJobSerializerDeserializer(DeploymentId deploymentId) {
        if (deploymentId == null) {
            return defaultJobSerDe;
        }
        IJobSerializerDeserializer jobSerDe = jobSerializerDeserializerMap.get(deploymentId);
        return jobSerDe;
    }

    @Override
    public synchronized void addJobSerializerDeserializer(DeploymentId deploymentId,
            IJobSerializerDeserializer jobSerDe) {
        jobSerializerDeserializerMap.put(deploymentId, jobSerDe);
    }

    @Override
    public synchronized void removeJobSerializerDeserializer(DeploymentId deploymentId) {
        jobSerializerDeserializerMap.remove(deploymentId);
    }

    @Override
    public String toString() {
        return jobSerializerDeserializerMap.toString();
    }

}
