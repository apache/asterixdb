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
package org.apache.hyracks.tests.integration;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.hyracks.api.exceptions.HyracksException;
import org.apache.hyracks.api.job.IJobLifecycleListener;
import org.apache.hyracks.api.job.JobId;
import org.apache.hyracks.api.job.JobSpecification;
import org.apache.hyracks.api.job.JobStatus;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class TestJobLifecycleListener implements IJobLifecycleListener {

    private static final Logger LOGGER = LogManager.getLogger();
    private final Map<JobId, JobSpecification> created = new HashMap<>();
    private final Set<JobId> started = new HashSet<>();
    private final Set<JobId> finished = new HashSet<>();
    private final Map<JobId, Integer> doubleCreated = new HashMap<>();
    private final Map<JobId, Integer> doubleStarted = new HashMap<>();
    private final Map<JobId, Integer> doubleFinished = new HashMap<>();
    private final Set<JobId> startWithoutCreate = new HashSet<>();
    private final Set<JobId> finishWithoutStart = new HashSet<>();

    @Override
    public void notifyJobCreation(JobId jobId, JobSpecification spec) throws HyracksException {
        if (created.containsKey(jobId)) {
            LOGGER.log(Level.WARN, "Job " + jobId + "has been created before");
            increment(doubleCreated, jobId);
        }
        created.put(jobId, spec);
    }

    private void increment(Map<JobId, Integer> map, JobId jobId) {
        Integer count = map.get(jobId);
        count = count == null ? 2 : count + 1;
        map.put(jobId, count);
    }

    @Override
    public void notifyJobStart(JobId jobId) throws HyracksException {
        if (!created.containsKey(jobId)) {
            LOGGER.log(Level.WARN, "Job " + jobId + "has not been created");
            startWithoutCreate.add(jobId);
        }
        if (started.contains(jobId)) {
            LOGGER.log(Level.WARN, "Job " + jobId + "has been started before");
            increment(doubleStarted, jobId);
        }
        started.add(jobId);
    }

    @Override
    public void notifyJobFinish(JobId jobId, JobStatus jobStatus, List<Exception> exceptions) throws HyracksException {
        if (!started.contains(jobId)) {
            LOGGER.log(Level.WARN, "Job " + jobId + "has not been started");
            finishWithoutStart.add(jobId);
        }
        if (finished.contains(jobId)) {
            // TODO: job finish should be called once only when it has really completed
            // throw new HyracksDataException("Job " + jobId + "has been finished before");
            LOGGER.log(Level.WARN, "Dangerous: Duplicate Job: " + jobId + " has finished with status: " + jobStatus);
            increment(doubleFinished, jobId);
        }
        finished.add(jobId);
    }

    public void check() throws Exception {
        LOGGER.log(Level.WARN, "Checking all created jobs have started");
        for (JobId jobId : created.keySet()) {
            if (!started.contains(jobId)) {
                LOGGER.log(Level.WARN, "JobId " + jobId + " has been created but never started");
            }
        }
        LOGGER.log(Level.WARN, "Checking all started jobs have terminated");
        for (JobId jobId : started) {
            if (!finished.contains(jobId)) {
                LOGGER.log(Level.WARN, "JobId " + jobId + " has started but not finished");
            }
        }
        LOGGER.log(Level.WARN, "Checking multiple creates");
        for (Entry<JobId, Integer> entry : doubleCreated.entrySet()) {
            LOGGER.log(Level.WARN, "job " + entry.getKey() + " has been created " + entry.getValue() + " times");
        }
        LOGGER.log(Level.WARN, "Checking multiple starts");
        for (Entry<JobId, Integer> entry : doubleStarted.entrySet()) {
            LOGGER.log(Level.WARN, "job " + entry.getKey() + " has been started " + entry.getValue() + " times");
        }
        LOGGER.log(Level.WARN, "Checking multiple finishes");
        for (Entry<JobId, Integer> entry : doubleFinished.entrySet()) {
            LOGGER.log(Level.WARN, "job " + entry.getKey() + " has been finished " + entry.getValue() + " times");
        }
        LOGGER.log(Level.WARN, "Done checking!");
    }
}
