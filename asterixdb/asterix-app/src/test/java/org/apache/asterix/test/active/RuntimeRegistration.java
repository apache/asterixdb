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

import java.util.Objects;

import org.apache.asterix.active.ActiveEvent;
import org.apache.asterix.active.ActiveEvent.Kind;
import org.apache.asterix.active.ActiveRuntimeId;
import org.apache.asterix.active.EntityId;
import org.apache.asterix.active.message.ActivePartitionMessage;
import org.apache.asterix.active.message.ActivePartitionMessage.Event;
import org.apache.asterix.metadata.declared.MetadataProvider;
import org.apache.hyracks.api.job.JobId;

public class RuntimeRegistration extends Action {

    private final TestNodeControllerActor nc;
    private final JobId jobId;
    private final EntityId entityId;
    private final int partition;

    public RuntimeRegistration(TestNodeControllerActor nc, JobId jobId, EntityId entityId, int partition) {
        this.nc = nc;
        this.jobId = jobId;
        this.entityId = entityId;
        this.partition = partition;
    }

    @Override
    protected void doExecute(MetadataProvider mdProvider) throws Exception {
        for (ActionSubscriber subscriber : nc.getSubscribers()) {
            subscriber.beforeExecute();
        }
        ActiveEvent event = new ActiveEvent(jobId, Kind.PARTITION_EVENT, entityId, new ActivePartitionMessage(
                new ActiveRuntimeId(entityId, nc.getId(), partition), jobId, Event.RUNTIME_REGISTERED, null));
        nc.getClusterController().activeEvent(event);
    }

    public Action deregister() {
        return nc.doDeRegisterRuntime(jobId, entityId, partition);
    }

    @Override
    public int hashCode() {
        return Objects.hash(jobId, entityId, partition);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (!(obj instanceof RuntimeRegistration)) {
            return false;
        }
        RuntimeRegistration o = (RuntimeRegistration) obj;
        return Objects.equals(jobId, o.jobId) && Objects.equals(entityId, o.entityId) && partition == o.partition;
    }
}
