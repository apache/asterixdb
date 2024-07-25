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
package org.apache.asterix.app.result;

public class ResponseMetrics {

    private long elapsedTime;
    private long executionTime;
    private long resultCount;
    private long resultSize;
    private long processedObjects;
    private long errorCount;
    private long warnCount;
    private long diskIoCount;
    private long compileTime;
    private long queueWaitTime;
    private double bufferCacheHitRatio;
    private long bufferCachePageReadCount;

    private ResponseMetrics() {
    }

    public static ResponseMetrics of(long elapsedTime, long executionTime, long resultCount, long resultSize,
            long processedObjects, long errorCount, long warnCount, long compileTime, long queueWaitTime,
            double bufferCacheHitRatio, long bufferCachePageReadCount) {
        ResponseMetrics metrics = new ResponseMetrics();
        metrics.elapsedTime = elapsedTime;
        metrics.executionTime = executionTime;
        metrics.resultCount = resultCount;
        metrics.resultSize = resultSize;
        metrics.processedObjects = processedObjects;
        metrics.errorCount = errorCount;
        metrics.warnCount = warnCount;
        metrics.compileTime = compileTime;
        metrics.queueWaitTime = queueWaitTime;
        metrics.bufferCacheHitRatio = bufferCacheHitRatio;
        metrics.bufferCachePageReadCount = bufferCachePageReadCount;
        return metrics;
    }

    public long getElapsedTime() {
        return elapsedTime;
    }

    public long getExecutionTime() {
        return executionTime;
    }

    public long getResultCount() {
        return resultCount;
    }

    public long getResultSize() {
        return resultSize;
    }

    public long getProcessedObjects() {
        return processedObjects;
    }

    public long getErrorCount() {
        return errorCount;
    }

    public long getWarnCount() {
        return warnCount;
    }

    public long getCompileTime() {
        return compileTime;
    }

    public long getQueueWaitTime() {
        return queueWaitTime;
    }

    public double getBufferCacheHitRatio() {
        return bufferCacheHitRatio;
    }

    public long getBufferCachePageReadCount() {
        return bufferCachePageReadCount;
    }
}
