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

package edu.uci.ics.hyracks.storage.am.lsm.btree.perf;

import edu.uci.ics.hyracks.storage.am.common.datagen.DataGenThread;

public interface IExperimentRunner {
    public static int DEFAULT_MAX_OUTSTANDING = 100000;
    
    public void init() throws Exception;
    
    public long runExperiment(DataGenThread dataGen, int numThreads) throws Exception;
    
    public void reset() throws Exception;
    
    public void deinit() throws Exception;
}
