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
package org.apache.asterix.translator;

import java.util.List;
import java.util.Set;

import org.apache.hyracks.api.exceptions.Warning;
import org.apache.hyracks.api.result.IResultMetadata;

import com.fasterxml.jackson.databind.node.ObjectNode;

public class ResultMetadata implements IResultMetadata {
    private static final long serialVersionUID = 1905367559307369036L;

    private final SessionConfig.OutputFormat format;
    private long jobDuration;
    private long processedObjects;
    private long diskIoCount;
    private double bufferCacheHitRatio;
    private ObjectNode profile;
    private Set<Warning> warnings;
    private long totalWarningsCount;
    private transient List<Object> outputTypes;
    private long queueWaitTimeInNanos;
    private long bufferCachePageReadCount;

    public ResultMetadata(SessionConfig.OutputFormat format) {
        this.format = format;
    }

    public SessionConfig.OutputFormat getFormat() {
        return format;
    }

    public long getProcessedObjects() {
        return processedObjects;
    }

    public void setProcessedObjects(long processedObjects) {
        this.processedObjects = processedObjects;
    }

    public void setJobDuration(long jobDuration) {
        this.jobDuration = jobDuration;
    }

    public void setBufferCacheHitRatio(double bufferCacheHitRatio) {
        this.bufferCacheHitRatio = bufferCacheHitRatio;
    }

    public double getBufferCacheHitRatio() {
        return bufferCacheHitRatio;
    }

    public void setBufferCachePageReadCount(long bufferCachePageReadCount) {
        this.bufferCachePageReadCount = bufferCachePageReadCount;
    }

    public long getBufferCachePageReadCount() {
        return bufferCachePageReadCount;
    }

    public void setWarnings(Set<Warning> warnings) {
        this.warnings = warnings;
    }

    /**
     * Sets the count of all warnings generated including unreported ones.
     */
    public void setTotalWarningsCount(long totalWarningsCount) {
        this.totalWarningsCount = totalWarningsCount;
    }

    public long getJobDuration() {
        return jobDuration;
    }

    public void setJobProfile(ObjectNode profile) {
        this.profile = profile;
    }

    public ObjectNode getJobProfile() {
        return profile;
    }

    /**
     * @return The reported warnings.
     */
    public Set<Warning> getWarnings() {
        return warnings;
    }

    /**
     * @return Total count of all warnings generated including unreported ones.
     */
    public long getTotalWarningsCount() {
        return totalWarningsCount;
    }

    @Override
    public void setOutputTypes(List<Object> typeList) {
        this.outputTypes = typeList;
    }

    public List<Object> getOutputTypes() {
        return outputTypes;
    }

    public long getQueueWaitTimeInNanos() {
        return queueWaitTimeInNanos;
    }

    public void setQueueWaitTimeInNanos(long queueWaitTimeInNanos) {
        this.queueWaitTimeInNanos = queueWaitTimeInNanos;
    }

    @Override
    public String toString() {
        return "ResultMetadata{" + "format=" + format + ", jobDuration=" + jobDuration + ", processedObjects="
                + processedObjects + ", diskIoCount=" + diskIoCount + '}';
    }
}
