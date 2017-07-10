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
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.commons.lang3.tuple.Pair;

/**
 * The LockList is used for two phase locking.
 */
public class LockList {
    private final List<Pair<IMetadataLock.Mode, IMetadataLock>> locks = new ArrayList<>();
    private final Set<String> lockSet = new HashSet<>();
    private boolean lockPhase = true;

    /**
     * Acquires a lock.
     *
     * @param mode
     *            the lock mode.
     * @param lock
     *            the lock object.
     */
    public void add(IMetadataLock.Mode mode, IMetadataLock lock) throws AsterixException {
        if (!lockPhase) {
            throw new AsterixException(ErrorCode.COMPILATION_TWO_PHASE_LOCKING_VIOLATION);
        }
        if (lockSet.contains(lock.getKey())) {
            return;
        }
        lock.acquire(mode);
        locks.add(Pair.of(mode, lock));
        lockSet.add(lock.getKey());
    }

    /**
     * Once unlock() is called, no caller can call add(IMetadataLock.Mode mode, IMetadataLock lock),
     * except that reset() is called.
     */
    public void unlock() {
        for (int i = locks.size() - 1; i >= 0; i--) {
            Pair<IMetadataLock.Mode, IMetadataLock> pair = locks.get(i);
            pair.getRight().release(pair.getLeft());
        }
        locks.clear();
        lockSet.clear();
        lockPhase = false;
    }

    /**
     * Clears the state and starts another pass of two phase locking again.
     */
    public void reset() {
        unlock();
        lockPhase = true;
    }
}
