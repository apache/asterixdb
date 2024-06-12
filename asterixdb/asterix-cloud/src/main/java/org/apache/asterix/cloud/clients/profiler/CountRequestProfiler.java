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
package org.apache.asterix.cloud.clients.profiler;

import java.util.concurrent.atomic.AtomicLong;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class CountRequestProfiler implements IRequestProfiler {
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final Logger LOGGER = LogManager.getLogger();
    private static final Level LOG_LEVEL = Level.TRACE;
    private final long logInterval;
    private final AtomicLong listObjectsCounter;
    private final AtomicLong getObjectCounter;
    private final AtomicLong writeObjectCounter;
    private final AtomicLong deleteObjectCounter;
    private final AtomicLong copyObjectCounter;
    private final AtomicLong multipartUploadCounter;
    private final AtomicLong multipartDownloadCounter;
    private long lastLogTimestamp;

    public CountRequestProfiler(long logIntervalNanoSec) {
        this.logInterval = logIntervalNanoSec;
        listObjectsCounter = new AtomicLong();
        getObjectCounter = new AtomicLong();
        writeObjectCounter = new AtomicLong();
        deleteObjectCounter = new AtomicLong();
        copyObjectCounter = new AtomicLong();
        multipartUploadCounter = new AtomicLong();
        multipartDownloadCounter = new AtomicLong();
        lastLogTimestamp = System.nanoTime();
    }

    @Override
    public void objectsList() {
        listObjectsCounter.incrementAndGet();
        log();
    }

    @Override
    public void objectGet() {
        getObjectCounter.incrementAndGet();
        log();
    }

    @Override
    public void objectWrite() {
        writeObjectCounter.incrementAndGet();
        log();
    }

    @Override
    public void objectDelete() {
        deleteObjectCounter.incrementAndGet();
        log();
    }

    @Override
    public void objectCopy() {
        copyObjectCounter.incrementAndGet();
        log();
    }

    @Override
    public void objectMultipartUpload() {
        multipartUploadCounter.incrementAndGet();
        log();
    }

    @Override
    public void objectMultipartDownload() {
        multipartDownloadCounter.incrementAndGet();
        log();
    }

    @Override
    public long objectsListCount() {
        return listObjectsCounter.get();
    }

    @Override
    public long objectGetCount() {
        return getObjectCounter.get();
    }

    @Override
    public long objectWriteCount() {
        return writeObjectCounter.get();
    }

    @Override
    public long objectDeleteCount() {
        return deleteObjectCounter.get();
    }

    @Override
    public long objectCopyCount() {
        return copyObjectCounter.get();
    }

    @Override
    public long objectMultipartUploadCount() {
        return multipartUploadCounter.get();
    }

    @Override
    public long objectMultipartDownloadCount() {
        return multipartDownloadCounter.get();
    }

    private void log() {
        if (LOGGER.isEnabled(LOG_LEVEL)) {
            long currentTime = System.nanoTime();
            if (currentTime - lastLogTimestamp >= logInterval) {
                // Might log multiple times
                lastLogTimestamp = currentTime;
                ObjectNode countersNode = OBJECT_MAPPER.createObjectNode();
                countersNode.put("listObjectsCounter", listObjectsCounter.get());
                countersNode.put("getObjectCounter", getObjectCounter.get());
                countersNode.put("writeObjectCounter", writeObjectCounter.get());
                countersNode.put("deleteObjectCounter", deleteObjectCounter.get());
                countersNode.put("copyObjectCounter", copyObjectCounter.get());
                countersNode.put("multipartUploadCounter", multipartUploadCounter.get());
                countersNode.put("multipartDownloadCounter", multipartDownloadCounter.get());
                LOGGER.log(LOG_LEVEL, "Cloud request counters: {}", countersNode.toString());
            }
        }
    }
}
