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
package org.apache.asterix.common.metadata;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.common.metadata.IMetadataLock.Mode;
import org.apache.commons.lang3.tuple.MutablePair;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;

/**
 * The LockList is used for two phase locking.
 */
public class LockList {
    private final List<MutablePair<IMetadataLock, IMetadataLock.Mode>> locks = new ArrayList<>();
    private final HashMap<String, Integer> indexes = new HashMap<>();
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
        if (isContained(mode, lock)) {
            return;
        }
        lock.lock(mode);
        indexes.put(lock.getKey(), locks.size());
        locks.add(MutablePair.of(lock, mode));
    }

    private boolean isContained(Mode mode, IMetadataLock lock) throws AsterixException {
        if (!lockPhase) {
            throw new AsterixException(ErrorCode.COMPILATION_TWO_PHASE_LOCKING_VIOLATION);
        }
        Integer index = indexes.get(lock.getKey());
        if (index != null) {
            Mode acquired = locks.get(index).right;
            if (!acquired.contains(mode)) {
                throw new AsterixException(ErrorCode.LOCK_WAS_ACQUIRED_DIFFERENT_OPERATION, mode, acquired);
            }
            return true;
        }
        return false;
    }

    public void upgrade(Mode to, IMetadataLock lock) throws AlgebricksException {
        if (!lockPhase) {
            throw new AsterixException(ErrorCode.COMPILATION_TWO_PHASE_LOCKING_VIOLATION);
        }
        Integer index = indexes.get(lock.getKey());
        if (index == null) {
            throw new AsterixException(ErrorCode.UPGRADE_FAILED_LOCK_WAS_NOT_ACQUIRED);
        }
        MutablePair<IMetadataLock, Mode> pair = locks.get(index);
        Mode from = pair.getRight();
        if (from == to) {
            return;
        }
        lock.upgrade(from, to);
        pair.setRight(to);
    }

    public void downgrade(Mode mode, IMetadataLock lock) throws AlgebricksException {
        if (!lockPhase) {
            throw new AsterixException(ErrorCode.COMPILATION_TWO_PHASE_LOCKING_VIOLATION);
        }
        Integer index = indexes.get(lock.getKey());
        if (index == null) {
            throw new AsterixException(ErrorCode.DOWNGRADE_FAILED_LOCK_WAS_NOT_ACQUIRED);
        }
        MutablePair<IMetadataLock, Mode> pair = locks.get(index);
        Mode acquired = pair.getRight();
        lock.downgrade(acquired, mode);
        pair.setRight(mode);
    }

    /**
     * Once unlock() is called, no caller can call add(IMetadataLock.Mode mode, IMetadataLock lock),
     * except that reset() is called.
     */
    public void unlock() {
        for (int i = locks.size() - 1; i >= 0; i--) {
            MutablePair<IMetadataLock, Mode> pair = locks.get(i);
            pair.getLeft().unlock(pair.getRight());
        }
        locks.clear();
        indexes.clear();
        lockPhase = false;
    }

    /**
     * Clears the state and starts another pass of two phase locking again.
     */
    public void reset() {
        unlock();
        lockPhase = true;
    }

    @Override
    public String toString() {
        return "{\"phase\" : \"" + (lockPhase ? "lock" : "unlock") + "\", \"locks\" : " + locks + "}";
    }
}
