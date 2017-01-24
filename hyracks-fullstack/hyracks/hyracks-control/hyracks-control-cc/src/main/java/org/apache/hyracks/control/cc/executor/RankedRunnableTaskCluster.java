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
package org.apache.hyracks.control.cc.executor;

import org.apache.hyracks.control.cc.job.TaskCluster;

public class RankedRunnableTaskCluster implements Comparable<RankedRunnableTaskCluster> {
    private final int rank;
    private final TaskCluster taskCluster;

    public RankedRunnableTaskCluster(int rank, TaskCluster taskCluster) {
        this.rank = rank;
        this.taskCluster = taskCluster;
    }

    public int getRank() {
        return rank;
    }

    public TaskCluster getTaskCluster() {
        return taskCluster;
    }

    @Override
    public String toString() {
        return "[" + rank + ":" + taskCluster + "]";
    }

    @Override
    public int compareTo(RankedRunnableTaskCluster o) {
        int cmp = rank - o.rank;
        return cmp < 0 ? -1 : (cmp > 0 ? 1 : 0);
    }

    @Override
    public int hashCode() {
        return rank;
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof RankedRunnableTaskCluster)) {
            return false;
        }
        RankedRunnableTaskCluster target = (RankedRunnableTaskCluster) o;
        return rank == target.rank;
    }
}
