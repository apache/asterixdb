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
package edu.uci.ics.hyracks.algebricks.runtime.base;

import java.io.DataOutput;

import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.IFrameTupleReference;

public interface ICopySerializableAggregateFunction {
    /**
     * initialize the space occupied by internal state
     * 
     * @param state
     * @throws AlgebricksException
     * @return length of the intermediate state
     */
    public void init(DataOutput state) throws AlgebricksException;

    /**
     * update the internal state
     * 
     * @param tuple
     * @param state
     * @throws AlgebricksException
     */
    public void step(IFrameTupleReference tuple, byte[] data, int start, int len) throws AlgebricksException;

    /**
     * output the state to result
     * 
     * @param state
     * @param result
     * @throws AlgebricksException
     */
    public void finish(byte[] data, int start, int len, DataOutput result) throws AlgebricksException;

    /**
     * output the partial state to partial result
     * 
     * @param state
     * @param partialResult
     * @throws AlgebricksException
     */
    public void finishPartial(byte[] data, int start, int len, DataOutput partialResult) throws AlgebricksException;
}
