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
package org.apache.asterix.common.replication;

import org.apache.asterix.common.transactions.ILogRecord;
import org.apache.hyracks.api.replication.IIOReplicationManager;

public interface IReplicationManager extends IIOReplicationManager {

    /**
     * Registers {@code replica}. After registration, the replica will be included in all replication events
     *
     * @param replica
     */
    void register(IPartitionReplica replica);

    /**
     * Unregisters {@code replica} from replication events.
     *
     * @param replica
     */
    void unregister(IPartitionReplica replica);

    /**
     * Notifies that failure {@code failure} occurred on {@code dest}
     *
     * @param dest
     * @param failure
     */
    void notifyFailure(IReplicationDestination dest, Exception failure);

    /**
     * Asynchronously sends a serialized version of the record to remote replicas.
     *
     * @param logRecord The log record to be replicated
     * @throws InterruptedException
     */
    void replicate(ILogRecord logRecord) throws InterruptedException;

    /**
     * @return the replication strategy
     */
    IReplicationStrategy getReplicationStrategy();

}
