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
package org.apache.asterix.test.active;

import org.apache.asterix.active.ActiveEvent;
import org.apache.asterix.active.ActiveEvent.Kind;
import org.apache.asterix.active.ActiveRuntimeId;
import org.apache.asterix.active.EntityId;
import org.apache.asterix.active.message.ActivePartitionMessage;
import org.apache.asterix.metadata.declared.MetadataProvider;
import org.apache.hyracks.api.job.JobId;

public class TestNodeControllerActor extends Actor {

    private final String id;
    private final TestClusterControllerActor clusterController;

    public TestNodeControllerActor(String name, TestClusterControllerActor clusterController) {
        super("NC: " + name, null);
        this.id = name;
        this.clusterController = clusterController;
    }

    public Action registerRuntime(JobId jobId, EntityId entityId, int partition) {
        Action registration = new Action() {
            @Override
            protected void doExecute(MetadataProvider actorMdProvider) throws Exception {
                ActiveEvent event = new ActiveEvent(jobId, Kind.PARTITION_EVENT, entityId,
                        new ActivePartitionMessage(new ActiveRuntimeId(entityId, id, partition), jobId,
                                ActivePartitionMessage.ACTIVE_RUNTIME_REGISTERED, null));
                clusterController.activeEvent(event);
            }
        };
        add(registration);
        return registration;
    }

    public Action deRegisterRuntime(JobId jobId, EntityId entityId, int partition) {
        Action registration = new Action() {
            @Override
            protected void doExecute(MetadataProvider actorMdProvider) throws Exception {
                ActiveEvent event = new ActiveEvent(jobId, Kind.PARTITION_EVENT, entityId,
                        new ActivePartitionMessage(new ActiveRuntimeId(entityId, id, partition), jobId,
                                ActivePartitionMessage.ACTIVE_RUNTIME_DEREGISTERED, null));
                clusterController.activeEvent(event);
            }
        };
        add(registration);
        return registration;
    }
}
