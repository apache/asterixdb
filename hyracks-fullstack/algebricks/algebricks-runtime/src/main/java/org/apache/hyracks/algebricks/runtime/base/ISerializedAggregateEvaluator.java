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
package org.apache.hyracks.algebricks.runtime.base;

import java.io.DataOutput;

import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.data.accessors.IFrameTupleReference;

public interface ISerializedAggregateEvaluator {
    /**
     * initialize the space occupied by internal state
     *
     * @param state
     * @throws AlgebricksException
     */
    public void init(DataOutput state) throws HyracksDataException;

    /**
     * update the internal state
     *
     * @param state
     * @param tuple
     * @throws AlgebricksException
     */
    public void step(IFrameTupleReference tuple, byte[] data, int start, int len) throws HyracksDataException;

    /**
     * output the state to result
     *
     * @param state
     * @param result
     * @throws AlgebricksException
     */
    public void finish(byte[] data, int start, int len, DataOutput result) throws HyracksDataException;

    /**
     * output the partial state to partial result
     *
     * @param state
     * @param partialResult
     * @throws AlgebricksException
     */
    public void finishPartial(byte[] data, int start, int len, DataOutput partialResult) throws HyracksDataException;
}
