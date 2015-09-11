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
package org.apache.hyracks.test.support;

import org.apache.hyracks.api.application.INCApplicationContext;
import org.apache.hyracks.api.context.IHyracksRootContext;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.ActivityId;
import org.apache.hyracks.api.dataflow.OperatorDescriptorId;
import org.apache.hyracks.api.dataflow.TaskAttemptId;
import org.apache.hyracks.api.dataflow.TaskId;
import org.apache.hyracks.api.exceptions.HyracksException;
import org.apache.hyracks.api.job.JobId;

public class TestUtils {
    public static IHyracksTaskContext create(int frameSize) {
        try {
            IHyracksRootContext rootCtx = new TestRootContext();
            INCApplicationContext appCtx = new TestNCApplicationContext(rootCtx, null);
            TestJobletContext jobletCtx = new TestJobletContext(frameSize, appCtx, new JobId(0));
            TaskAttemptId tid = new TaskAttemptId(new TaskId(new ActivityId(new OperatorDescriptorId(0), 0), 0), 0);
            IHyracksTaskContext taskCtx = new TestTaskContext(jobletCtx, tid);
            return taskCtx;
        } catch (HyracksException e) {
            throw new RuntimeException(e);
        }
    }
}