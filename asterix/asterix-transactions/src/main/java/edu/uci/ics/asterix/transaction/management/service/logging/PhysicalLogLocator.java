/*
 * Copyright 2009-2010 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.uci.ics.asterix.transaction.management.service.logging;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Represents the physical location of a log record. The physical location i
 * deciphered from the contained lsn that is broken down into a file id and an
 * offset within the file. The mapping between fileId and the path on the local
 * file system is maintained by the log manager (@see ILogManager) path on the
 * local file system.
 */
public class PhysicalLogLocator {

    // The log sequence number corresponding to the log record that is being
    // referred to.
    private final AtomicLong lsn;

    // Handle to the log manager that wrote the log record.
    private final ILogManager logManager;

    public static long getLsnValue(long fileId, long offset, ILogManager logManager) {
        return fileId * logManager.getLogManagerProperties().getLogPartitionSize() + offset;
    }

    public PhysicalLogLocator(long lsn, ILogManager logManager) {
        this.lsn = new AtomicLong(lsn);
        this.logManager = logManager;
    }

    public PhysicalLogLocator(long fileId, long offset, ILogManager logManager) {
        this.lsn = new AtomicLong(getLsnValue(fileId, offset, logManager));
        this.logManager = logManager;
    }

    @Override
    public String toString() {
        return "lsn :" + lsn.get();
    }

    public long getFileId() {
        return LogUtil.getFileId(logManager, lsn.get());
    }

    public boolean compareAndSet(long expect, long update) {
        return lsn.compareAndSet(expect, update);
    }

    public long getFileOffset() {
        return LogUtil.getFileOffset(logManager, lsn.get());
    }

    public long getLsn() {
        return lsn.get();
    }

    public long incrementLsn() {
        return lsn.incrementAndGet();
    }

    public long incrementLsn(long delta) {
        return lsn.addAndGet(delta);
    }

    public void setLsn(long lsn) {
        this.lsn.set(lsn);
    }

}
