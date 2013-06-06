/*
 * Copyright 2009-2013 by The Regents of the University of California
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
package edu.uci.ics.hivesterix.runtime.exec;

import java.io.Serializable;
import java.util.List;

import org.apache.hadoop.hive.ql.exec.Task;

public interface IExecutionEngine {

    /**
     * compile the job
     * 
     * @param rootTasks
     *            : Hive MapReduce plan
     * @return 0 pass, 1 fail
     */
    public int compileJob(List<Task<? extends Serializable>> rootTasks);

    /**
     * execute the job with latest compiled plan
     * 
     * @return
     */
    public int executeJob();
}
