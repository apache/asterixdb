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

import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.common.exceptions.MetadataException;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;

/**
 * A Metadata lock local to compilation node
 */
public interface IMetadataLock {

    enum Mode {
        READ,
        MODIFY,
        INDEX_BUILD,
        EXCLUSIVE_MODIFY,
        UPGRADED_WRITE,
        WRITE;

        public boolean contains(Mode mode) {
            if (mode == this) {
                return true;
            }
            if (this == Mode.WRITE || this == Mode.UPGRADED_WRITE) {
                return true;
            }
            if (this == Mode.EXCLUSIVE_MODIFY && (mode == Mode.MODIFY || mode == Mode.INDEX_BUILD)) {
                return true;
            }
            return mode == Mode.READ;
        }
    }

    /**
     * Acquire a lock
     *
     * @param mode
     *            lock mode
     */
    void lock(IMetadataLock.Mode mode);

    /**
     * Release a lock
     *
     * @param mode
     *            lock mode
     */
    void unlock(IMetadataLock.Mode mode);

    /**
     * Get the lock's key
     *
     * @return the key identiying the lock
     */
    String getKey();

    /**
     * upgrade the lock
     *
     * @param from
     * @param to
     * @throws AlgebricksException
     */
    default void upgrade(Mode from, Mode to) throws AlgebricksException {
        throw new MetadataException(ErrorCode.ILLEGAL_LOCK_UPGRADE_OPERATION, from, to);
    }

    /**
     * downgrade the lock
     *
     * @param from
     * @param to
     * @throws AlgebricksException
     */
    default void downgrade(Mode from, Mode to) throws AlgebricksException {
        throw new MetadataException(ErrorCode.ILLEGAL_LOCK_DOWNGRADE_OPERATION, from, to);
    }
}
