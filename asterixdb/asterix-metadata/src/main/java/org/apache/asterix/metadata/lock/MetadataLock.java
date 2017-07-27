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

import java.util.Objects;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.asterix.common.metadata.IMetadataLock;

public class MetadataLock implements IMetadataLock {
    private final String key;
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock(true);

    public MetadataLock(String key) {
        this.key = key;
    }

    @Override
    public void lock(IMetadataLock.Mode mode) {
        switch (mode) {
            case WRITE:
                lock.writeLock().lock();
                break;
            default:
                lock.readLock().lock();
                break;
        }
    }

    @Override
    public void unlock(IMetadataLock.Mode mode) {
        switch (mode) {
            case WRITE:
                lock.writeLock().unlock();
                break;
            default:
                lock.readLock().unlock();
                break;
        }
    }

    @Override
    public String getKey() {
        return key;
    }

    @Override
    public int hashCode() {
        return key.hashCode();
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof MetadataLock)) {
            return false;
        }
        if (o == this) {
            return true;
        }
        return Objects.equals(key, ((MetadataLock) o).key);
    }

    @Override
    public String toString() {
        return key;
    }
}
