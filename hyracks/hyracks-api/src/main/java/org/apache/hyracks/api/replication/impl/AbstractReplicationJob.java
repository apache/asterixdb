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
package org.apache.hyracks.api.replication.impl;

import java.util.Set;

import org.apache.hyracks.api.replication.IReplicationJob;

public abstract class AbstractReplicationJob implements IReplicationJob {
    private final Set<String> filesToReplicate;
    private final ReplicationOperation operation;
    private final ReplicationExecutionType executionType;
    private final ReplicationJobType jobType;

    public AbstractReplicationJob(ReplicationJobType jobType, ReplicationOperation operation,
            ReplicationExecutionType executionType, Set<String> filesToReplicate) {
        this.jobType = jobType;
        this.operation = operation;
        this.executionType = executionType;
        this.filesToReplicate = filesToReplicate;
    }

    @Override
    public Set<String> getJobFiles() {
        return filesToReplicate;
    }

    @Override
    public ReplicationOperation getOperation() {
        return operation;
    }

    @Override
    public ReplicationExecutionType getExecutionType() {
        return executionType;
    }

    @Override
    public ReplicationJobType getJobType() {
        return jobType;
    }
}
