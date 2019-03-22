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
package org.apache.asterix.runtime.job.listener;

import static org.apache.hyracks.api.constraints.expressions.ConstraintExpression.ExpressionTag;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.asterix.common.api.INodeJobTracker;
import org.apache.hyracks.api.config.IOption;
import org.apache.hyracks.api.constraints.Constraint;
import org.apache.hyracks.api.constraints.expressions.ConstantExpression;
import org.apache.hyracks.api.job.JobId;
import org.apache.hyracks.api.job.JobSpecification;
import org.apache.hyracks.api.job.JobStatus;
import org.apache.hyracks.util.annotations.ThreadSafe;

@ThreadSafe
public class NodeJobTracker implements INodeJobTracker {

    private final Map<String, Set<JobId>> nodeJobs = new HashMap<>();

    @Override
    public synchronized void notifyJobCreation(JobId jobId, JobSpecification spec) {
        getJobParticipatingNodes(spec).stream().map(nodeJobs::get).forEach(jobsSet -> jobsSet.add(jobId));
    }

    @Override
    public synchronized void notifyJobStart(JobId jobId) {
        // nothing to do
    }

    @Override
    public synchronized void notifyJobFinish(JobId jobId, JobStatus jobStatus, List<Exception> exceptions) {
        nodeJobs.values().forEach(jobsSet -> jobsSet.remove(jobId));
    }

    @Override
    public synchronized void notifyNodeJoin(String nodeId, Map<IOption, Object> ncConfiguration) {
        nodeJobs.computeIfAbsent(nodeId, key -> new HashSet<>());
    }

    @Override
    public synchronized void notifyNodeFailure(Collection<String> deadNodeIds) {
        deadNodeIds.forEach(nodeJobs::remove);
    }

    @Override
    public synchronized Set<JobId> getPendingJobs(String nodeId) {
        return nodeJobs.containsKey(nodeId) ? Collections.unmodifiableSet(nodeJobs.get(nodeId))
                : Collections.emptySet();
    }

    @Override
    public Set<String> getJobParticipatingNodes(JobSpecification spec) {
        return spec.getUserConstraints().stream().map(Constraint::getRValue)
                .filter(ce -> ce.getTag() == ExpressionTag.CONSTANT).map(ConstantExpression.class::cast)
                .map(ConstantExpression::getValue).map(Object::toString).filter(nodeJobs::containsKey)
                .collect(Collectors.toSet());
    }
}
