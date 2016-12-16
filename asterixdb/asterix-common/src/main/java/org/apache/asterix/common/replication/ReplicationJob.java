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

import java.util.Set;

import org.apache.hyracks.api.replication.impl.AbstractReplicationJob;

/**
 * LSMIndexReplicationJob is used for LSM Components only in Hyracks level.
 * ReplicationJob is used for everything else.
 * Currently it is used to transfer indexes metadata files.
 */
public class ReplicationJob extends AbstractReplicationJob {

    public ReplicationJob(ReplicationJobType jobType, ReplicationOperation operation,
            ReplicationExecutionType executionType, Set<String> filesToReplicate) {
        super(jobType, operation, executionType, filesToReplicate);
    }

}
