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

import java.util.List;

import org.apache.asterix.active.ActiveEvent;
import org.apache.asterix.active.EntityId;
import org.apache.asterix.app.active.ActiveNotificationHandler;
import org.apache.asterix.metadata.declared.MetadataProvider;
import org.apache.asterix.metadata.entities.Dataset;
import org.apache.hyracks.api.job.JobId;
import org.apache.hyracks.api.job.JobSpecification;
import org.apache.hyracks.api.job.JobStatus;
import org.mockito.Mockito;

public class TestClusterControllerActor extends Actor {

    private final ActiveNotificationHandler handler;
    private final List<Dataset> allDatasets;

    public TestClusterControllerActor(String name, ActiveNotificationHandler handler, List<Dataset> allDatasets) {
        super(name, null);
        this.handler = handler;
        this.allDatasets = allDatasets;
    }

    public Action startActiveJob(JobId jobId, EntityId entityId) {
        Action startJob = new Action() {
            @Override
            protected void doExecute(MetadataProvider actorMdProvider) throws Exception {
                // succeed
                JobSpecification jobSpecification = Mockito.mock(JobSpecification.class);
                Mockito.when(jobSpecification.getProperty(ActiveNotificationHandler.ACTIVE_ENTITY_PROPERTY_NAME))
                        .thenReturn(entityId);
                handler.notifyJobCreation(jobId, jobSpecification);
                handler.notifyJobStart(jobId);
            }
        };
        add(startJob);
        return startJob;
    }

    public Action activeEvent(ActiveEvent event) {
        Action delivery = new Action() {
            @Override
            protected void doExecute(MetadataProvider actorMdProvider) throws Exception {
                handler.add(event);
            }
        };
        add(delivery);
        return delivery;
    }

    public Action jobFinish(JobId jobId, JobStatus jobStatus, List<Exception> exceptions) {
        Action delivery = new Action() {
            @Override
            protected void doExecute(MetadataProvider actorMdProvider) throws Exception {
                handler.notifyJobFinish(jobId, jobStatus, exceptions);
            }
        };
        add(delivery);
        return delivery;
    }

    public List<Dataset> getAllDatasets() {
        return allDatasets;
    }
}
