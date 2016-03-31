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

import org.apache.hyracks.api.deployment.DeploymentId;

public interface IJobSerializerDeserializerContainer {

    /**
     * Get the IJobSerializerDeserializer implementation instance for a specific deployment id
     *
     * @param deploymentId
     * @return
     */
    public IJobSerializerDeserializer getJobSerializerDeserializer(DeploymentId deploymentId);

    /**
     * Add a deployment with the job serializer deserializer
     *
     * @param deploymentId
     * @param jobSerDe
     */
    public void addJobSerializerDeserializer(DeploymentId deploymentId, IJobSerializerDeserializer jobSerDe);

    /**
     * Remove a deployment
     *
     * @param deploymentId
     */
    public void removeJobSerializerDeserializer(DeploymentId deploymentId);

}
