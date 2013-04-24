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
package edu.uci.ics.hyracks.control.nc.work;

import java.util.List;

import edu.uci.ics.hyracks.control.common.work.AbstractWork;
import edu.uci.ics.hyracks.control.nc.NodeControllerService;
import edu.uci.ics.hyracks.control.nc.Task;

public class NotifyTaskFailureWork extends AbstractWork {
    private final NodeControllerService ncs;
    private final Task task;
    private final String details;
    private final List<Throwable> caughtExceptions;

    public NotifyTaskFailureWork(NodeControllerService ncs, Task task, String details, List<Throwable> caughtExceptions) {
        this.ncs = ncs;
        this.task = task;
        this.details = details;
        this.caughtExceptions = caughtExceptions;
    }

    @Override
    public void run() {
        try {
            ncs.getClusterController().notifyTaskFailure(task.getJobletContext().getJobId(), task.getTaskAttemptId(),
                    ncs.getId(), details, caughtExceptions);
        } catch (Exception e) {
            e.printStackTrace();
        }
        task.getJoblet().removeTask(task);
    }
}