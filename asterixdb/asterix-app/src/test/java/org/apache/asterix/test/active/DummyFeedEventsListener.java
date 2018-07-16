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

import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.asterix.active.ActivityState;
import org.apache.asterix.active.EntityId;
import org.apache.asterix.active.IActiveEntityEventSubscriber;
import org.apache.asterix.active.IRetryPolicyFactory;
import org.apache.asterix.app.active.FeedEventsListener;
import org.apache.asterix.common.dataflow.ICcApplicationContext;
import org.apache.asterix.external.feed.watch.WaitForStateSubscriber;
import org.apache.asterix.metadata.declared.MetadataProvider;
import org.apache.asterix.metadata.entities.Dataset;
import org.apache.asterix.metadata.entities.Feed;
import org.apache.asterix.metadata.entities.FeedConnection;
import org.apache.asterix.translator.IStatementExecutor;
import org.apache.hyracks.algebricks.common.constraints.AlgebricksAbsolutePartitionConstraint;
import org.apache.hyracks.api.client.IHyracksClientConnection;
import org.apache.hyracks.api.exceptions.HyracksDataException;

public class DummyFeedEventsListener extends FeedEventsListener {

    public DummyFeedEventsListener(IStatementExecutor statementExecutor, ICcApplicationContext appCtx,
            IHyracksClientConnection hcc, EntityId entityId, List<Dataset> datasets,
            AlgebricksAbsolutePartitionConstraint locations, String runtimeName, IRetryPolicyFactory retryPolicyFactory,
            Feed feed, List<FeedConnection> feedConnections) throws HyracksDataException {
        super(statementExecutor, appCtx, hcc, entityId, datasets, locations, runtimeName, retryPolicyFactory, feed,
                feedConnections);
    }

    @Override
    protected void doStart(MetadataProvider metadataProvider) throws HyracksDataException {
        WaitForStateSubscriber eventSubscriber =
                new WaitForStateSubscriber(this, Collections.singleton(ActivityState.RUNNING));
        try {
            eventSubscriber.sync();
            if (eventSubscriber.getFailure() != null) {
                throw eventSubscriber.getFailure();
            }
        } catch (Exception e) {
            throw HyracksDataException.create(e);
        }
    }

    @Override
    protected void doStop(MetadataProvider metadataProvider, long timeout, TimeUnit unit) throws HyracksDataException {
        IActiveEntityEventSubscriber eventSubscriber =
                new WaitForStateSubscriber(this, Collections.singleton(ActivityState.STOPPED));
        try {
            eventSubscriber.sync();
        } catch (Exception e) {
            throw HyracksDataException.create(e);
        }
    }
}
