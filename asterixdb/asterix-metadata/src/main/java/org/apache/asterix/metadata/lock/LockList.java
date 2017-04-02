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
package org.apache.asterix.metadata.lock;

import java.util.ArrayList;
import java.util.List;

import org.apache.asterix.metadata.lock.IMetadataLock.Mode;
import org.apache.commons.lang3.tuple.Pair;

public class LockList {
    List<Pair<IMetadataLock.Mode, IMetadataLock>> locks = new ArrayList<>();

    public void add(IMetadataLock.Mode mode, IMetadataLock lock) {
        lock.acquire(mode);
        locks.add(Pair.of(mode, lock));
    }

    public void unlock() {
        for (int i = locks.size() - 1; i >= 0; i--) {
            Pair<IMetadataLock.Mode, IMetadataLock> pair = locks.get(i);
            pair.getRight().release(pair.getLeft());
        }
    }

    public void reset() {
        locks.clear();
    }
}
