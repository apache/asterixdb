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
package edu.uci.ics.pregelix.api.job;

import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;

/**
 * Interface for an object whose {@link completeIteration} method is called at the end
 * of each pregelix job iteration.
 * 
 * This class can be used to extend/replace the simple reporting in pregelix or to
 * implement aggregation across iterations of a job (rather than having the values
 * reset after each iteration).
 * One object is created for each job.
 * 
 * @author jake.biesinger
 */
public interface IIterationCompleteReporterHook {

    public void completeIteration(int superstep, PregelixJob job) throws HyracksDataException;

}
