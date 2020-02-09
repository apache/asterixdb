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
package org.apache.asterix.common.storage;

import java.net.InetSocketAddress;

import org.apache.hyracks.util.NetworkUtil;

public class ReplicaIdentifier {

    private final int partition;
    private final String id;
    private volatile InetSocketAddress location;

    private ReplicaIdentifier(int partition, InetSocketAddress location) {
        this.partition = partition;
        this.location = location;
        id = partition + "@" + location.getHostString() + ":" + location.getPort();
    }

    public static ReplicaIdentifier of(int partition, InetSocketAddress location) {
        return new ReplicaIdentifier(partition, location);
    }

    public int getPartition() {
        return partition;
    }

    public InetSocketAddress getLocation() {
        return location;
    }

    public InetSocketAddress refreshLocation() {
        //noinspection NonAtomicOperationOnVolatileField
        location = NetworkUtil.refresh(location);
        return location;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ReplicaIdentifier that = (ReplicaIdentifier) o;
        return id.equals(that.id);
    }

    @Override
    public int hashCode() {
        return id.hashCode();
    }

    @Override
    public String toString() {
        return id;
    }
}
