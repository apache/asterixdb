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
package org.apache.hyracks.dataflow.std.base;

import java.io.Serializable;

import org.apache.hyracks.api.context.IHyracksTaskContext;

/**
 * Represents a range id in a logical plan.
 */
public final class RangeId implements Serializable {
    private static final long serialVersionUID = 1L;
    private final int id;
    private int partition = -1;

    public RangeId(int id, int partition) {
        this.id = id;
        this.partition = partition;
    }

    public RangeId(int id, IHyracksTaskContext ctx) {
        this.id = id;
        this.partition = ctx.getTaskAttemptId().getTaskId().getPartition();
    }

    public RangeId(int id) {
        this.id = id;
    }

    public int getId() {
        return id;
    }

    public int getPartition() {
        return partition;
    }

    public void setPartition(int partition) {
        this.partition = partition;
    }

    @Override
    public String toString() {
        return "RangeId(" + id + (partition >= 0 ? "," + partition : "") + ")";
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof RangeId)) {
            return false;
        } else {
            return id == ((RangeId) obj).getId() && partition == ((RangeId) obj).getPartition();
        }
    }

    @Override
    public int hashCode() {
        return id;
    }

}
