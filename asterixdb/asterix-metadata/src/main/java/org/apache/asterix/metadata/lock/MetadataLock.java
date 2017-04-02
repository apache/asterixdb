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

import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.asterix.metadata.lock.IMetadataLock.Mode;

public class MetadataLock implements IMetadataLock {
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock(true);

    @Override
    public void acquire(IMetadataLock.Mode mode) {
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
    public void release(IMetadataLock.Mode mode) {
        switch (mode) {
            case WRITE:
                lock.writeLock().unlock();
                break;
            default:
                lock.readLock().unlock();
                break;
        }
    }
}
