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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.asterix.active.ActiveEvent;
import org.apache.asterix.active.ActiveEvent.Kind;
import org.apache.asterix.active.ActiveRuntimeId;
import org.apache.asterix.active.EntityId;
import org.apache.asterix.active.message.ActivePartitionMessage;
import org.apache.asterix.active.message.ActivePartitionMessage.Event;
import org.apache.asterix.metadata.declared.MetadataProvider;
import org.apache.hyracks.api.job.JobId;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class TestNodeControllerActor extends Actor {
    private static final Logger LOGGER = LogManager.getLogger();

    private final String id;
    private final TestClusterControllerActor clusterController;
    private final Set<RuntimeRegistration> registrations = new HashSet<>();
    private final List<ActionSubscriber> subscribers = new ArrayList<>();

    public TestNodeControllerActor(String name, TestClusterControllerActor clusterController) {
        super("NC: " + name, null);
        this.id = name;
        this.clusterController = clusterController;
    }

    public Action registerRuntime(JobId jobId, EntityId entityId, int partition) {
        RuntimeRegistration registration = new RuntimeRegistration(this, jobId, entityId, partition);
        for (ActionSubscriber subscriber : subscribers) {
            subscriber.beforeSchedule(registration);
        }
        registrations.add(registration);
        add(registration);
        return registration;
    }

    public Action deRegisterRuntime(JobId jobId, EntityId entityId, int partition) {
        RuntimeRegistration registration = new RuntimeRegistration(this, jobId, entityId, partition);
        if (registrations.remove(registration)) {
            return registration.deregister();
        } else {
            LOGGER.warn("Request to stop runtime: " + new ActiveRuntimeId(entityId, "Test", partition)
                    + " that is not registered. Could be that the runtime completed execution on"
                    + " this node before the cluster controller sent the stop request");
            return new Action() {
                @Override
                protected void doExecute(MetadataProvider mdProvider) throws Exception {
                }

                @Override
                public void sync() throws InterruptedException {
                    return;
                }

                @Override
                public boolean isDone() {
                    return true;
                }
            };
        }
    }

    public Action doDeRegisterRuntime(JobId jobId, EntityId entityId, int partition) {
        Action deregistration = new Action() {
            @Override
            protected void doExecute(MetadataProvider actorMdProvider) throws Exception {
                for (ActionSubscriber subscriber : subscribers) {
                    subscriber.beforeExecute();
                }
                ActiveEvent event = new ActiveEvent(jobId, Kind.PARTITION_EVENT, entityId, new ActivePartitionMessage(
                        new ActiveRuntimeId(entityId, id, partition), jobId, Event.RUNTIME_DEREGISTERED, null));
                clusterController.activeEvent(event);
            }
        };
        for (ActionSubscriber subscriber : subscribers) {
            subscriber.beforeSchedule(deregistration);
        }
        add(deregistration);
        return deregistration;
    }

    public void subscribe(ActionSubscriber subscriber) {
        subscribers.add(subscriber);
    }

    public void unsubscribe() {
        subscribers.clear();
    }

    public List<ActionSubscriber> getSubscribers() {
        return subscribers;
    }

    public String getId() {
        return id;
    }

    public TestClusterControllerActor getClusterController() {
        return clusterController;
    }
}
