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
package org.apache.asterix.replication.functions;

import java.io.DataInput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.HashSet;
import java.util.Set;

public class ReplicaIndexFlushRequest {
    Set<Long> laggingRescouresIds;

    public ReplicaIndexFlushRequest(Set<Long> laggingRescouresIds) {
        this.laggingRescouresIds = laggingRescouresIds;
    }

    public void serialize(OutputStream out) throws IOException {
        DataOutputStream dos = new DataOutputStream(out);
        dos.writeInt(laggingRescouresIds.size());
        for (Long resourceId : laggingRescouresIds) {
            dos.writeLong(resourceId);
        }
    }

    public static ReplicaIndexFlushRequest create(DataInput input) throws IOException {
        int numOfResources = input.readInt();
        Set<Long> laggingRescouresIds = new HashSet<Long>(numOfResources);
        for (int i = 0; i < numOfResources; i++) {
            laggingRescouresIds.add(input.readLong());
        }
        return new ReplicaIndexFlushRequest(laggingRescouresIds);
    }

    public Set<Long> getLaggingRescouresIds() {
        return laggingRescouresIds;
    }

    public void setLaggingRescouresIds(Set<Long> laggingRescouresIds) {
        this.laggingRescouresIds = laggingRescouresIds;
    }

}
