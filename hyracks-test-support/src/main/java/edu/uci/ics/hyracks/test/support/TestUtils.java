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
package edu.uci.ics.hyracks.test.support;

import java.util.UUID;

import edu.uci.ics.hyracks.api.application.INCApplicationContext;
import edu.uci.ics.hyracks.api.context.IHyracksJobletContext;
import edu.uci.ics.hyracks.api.context.IHyracksRootContext;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.ActivityId;
import edu.uci.ics.hyracks.api.dataflow.OperatorDescriptorId;
import edu.uci.ics.hyracks.api.dataflow.TaskAttemptId;
import edu.uci.ics.hyracks.api.dataflow.TaskId;
import edu.uci.ics.hyracks.api.exceptions.HyracksException;

public class TestUtils {
    public static IHyracksTaskContext create(int frameSize) {
        try {
            IHyracksRootContext rootCtx = new TestRootContext(frameSize);
            INCApplicationContext appCtx = new TestNCApplicationContext(rootCtx, null);
            IHyracksJobletContext jobletCtx = new TestJobletContext(appCtx, UUID.randomUUID());
            TaskAttemptId tid = new TaskAttemptId(new TaskId(new ActivityId(
                    new OperatorDescriptorId(UUID.randomUUID()), 0), 0), 0);
            IHyracksTaskContext taskCtx = new TestTaskContext(jobletCtx, tid);
            return taskCtx;
        } catch (HyracksException e) {
            throw new RuntimeException(e);
        }
    }
}