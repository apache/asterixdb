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
package org.apache.hyracks.dataflow.std.join;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class MergeJoinLocks implements Serializable {
    private static final long serialVersionUID = 1L;

    private final List<Lock> lock = new ArrayList<>();
    private final List<Condition> left = new ArrayList<>();
    private final List<Condition> right = new ArrayList<>();

    public synchronized void setPartitions(int partitions) {
        for (int i = lock.size(); i < partitions; ++i) {
            lock.add(new ReentrantLock());
            left.add(lock.get(i).newCondition());
            right.add(lock.get(i).newCondition());
        }
    }

    public Lock getLock(int partition) {
        return lock.get(partition);
    }

    public Condition getLeft(int partition) {
        return left.get(partition);
    }

    public Condition getRight(int partition) {
        return right.get(partition);
    }
}
