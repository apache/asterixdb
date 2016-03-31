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
package org.apache.asterix.metadata.utils;

import java.util.concurrent.locks.ReentrantReadWriteLock;

public class ExternalDatasetAccessManager {
    // a version to indicate the current version of the dataset
    private int version;
    // a lock to allow concurrent build index operation and serialize refresh operations
    private ReentrantReadWriteLock datasetLock;
    // a lock per version of the dataset to keep a version alive while queries are still assigned to it
    private ReentrantReadWriteLock v0Lock;
    private ReentrantReadWriteLock v1Lock;

    public ExternalDatasetAccessManager() {
        this.version = 0;
        this.v0Lock = new ReentrantReadWriteLock(false);
        this.v1Lock = new ReentrantReadWriteLock(false);
        this.datasetLock = new ReentrantReadWriteLock(true);
    }

    public int getVersion() {
        return version;
    }

    public void setVersion(int version) {
        this.version = version;
    }

    public ReentrantReadWriteLock getV0Lock() {
        return v0Lock;
    }

    public void setV0Lock(ReentrantReadWriteLock v0Lock) {
        this.v0Lock = v0Lock;
    }

    public ReentrantReadWriteLock getV1Lock() {
        return v1Lock;
    }

    public void setV1Lock(ReentrantReadWriteLock v1Lock) {
        this.v1Lock = v1Lock;
    }

    public int refreshBegin() {
        datasetLock.writeLock().lock();
        if (version == 0) {
            v1Lock.writeLock().lock();
        } else {
            v0Lock.writeLock().lock();
        }
        return version;
    }

    public void refreshEnd(boolean success) {
        if (version == 0) {
            v1Lock.writeLock().unlock();
            if (success) {
                version = 1;
            }
        } else {
            v0Lock.writeLock().unlock();
            if (success) {
                version = 0;
            }
        }
        datasetLock.writeLock().unlock();
    }

    public synchronized int buildIndexBegin(boolean isFirstIndex) {
        if (isFirstIndex) {
            datasetLock.writeLock().lock();
        } else {
            datasetLock.readLock().lock();
        }
        return version;
    }

    public void buildIndexEnd(boolean isFirstIndex) {
        if (isFirstIndex) {
            datasetLock.writeLock().unlock();
        } else {
            datasetLock.readLock().unlock();
        }
    }

    public int queryBegin() {
        if (version == 0) {
            v0Lock.readLock().lock();
            return 0;
        } else {
            v1Lock.readLock().lock();
            return 1;
        }
    }

    public void queryEnd(int version) {
        if (version == 0) {
            v0Lock.readLock().unlock();
        } else {
            v1Lock.readLock().unlock();
        }
    }
}
