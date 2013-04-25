/*
 * Copyright 2009-2013 by The Regents of the University of California
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

package edu.uci.ics.hyracks.api.job;

import edu.uci.ics.hyracks.api.deployment.DeploymentId;

public interface IJobSerializerDeserializerContainer {

    /**
     * Get the IJobSerializerDeserializer implementation instance for a specific deployment id
     * 
     * @param deploymentId
     * @return
     */
    public IJobSerializerDeserializer getJobSerializerDeerializer(DeploymentId deploymentId);

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
