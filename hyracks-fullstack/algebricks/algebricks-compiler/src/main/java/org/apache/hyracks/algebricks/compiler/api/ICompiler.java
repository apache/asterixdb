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
package org.apache.hyracks.algebricks.compiler.api;

import java.util.EnumSet;

import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.api.job.IJobletEventListenerFactory;
import org.apache.hyracks.api.job.JobFlag;
import org.apache.hyracks.api.job.JobSpecification;

public interface ICompiler {
    public void optimize() throws AlgebricksException;

    public JobSpecification createJob(Object appContext, IJobletEventListenerFactory jobEventListenerFactory)
            throws AlgebricksException;

    JobSpecification createJob(Object appContext, IJobletEventListenerFactory jobletEventListenerFactory,
            EnumSet<JobFlag> runtimeFlags) throws AlgebricksException;

    boolean skipJobCapacityAssignment();
}
