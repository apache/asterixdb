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

package edu.uci.ics.pregelix.core.jobgen;

import edu.uci.ics.pregelix.api.job.PregelixJob;
import edu.uci.ics.pregelix.core.base.IDriver.Plan;

public class JobGenFactory {

    public static JobGen createJobGen(Plan planChoice, PregelixJob currentJob) {
        JobGen jobGen = null;
        switch (planChoice) {
            case INNER_JOIN:
                jobGen = new JobGenInnerJoin(currentJob);
                break;
            case OUTER_JOIN:
                jobGen = new JobGenOuterJoin(currentJob);
                break;
            case OUTER_JOIN_SORT:
                jobGen = new JobGenOuterJoinSort(currentJob);
                break;
            case OUTER_JOIN_SINGLE_SORT:
                jobGen = new JobGenOuterJoinSingleSort(currentJob);
                break;
            default:
                jobGen = new JobGenInnerJoin(currentJob);
        }
        return jobGen;
    }

}
