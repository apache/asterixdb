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
package org.apache.asterix.common.parse;

import java.util.Map;

import org.apache.hyracks.api.comm.IFrameWriter;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;

public interface ITupleForwardPolicy {

    public static final String PARSER_POLICY = "parser-policy";
    
    public enum TupleForwardPolicyType {
        FRAME_FULL,
        COUNTER_TIMER_EXPIRED,
        RATE_CONTROLLED
    }

    public void configure(Map<String, String> configuration);

    public void initialize(IHyracksTaskContext ctx, IFrameWriter frameWriter) throws HyracksDataException;

    public TupleForwardPolicyType getType();

    public void addTuple(ArrayTupleBuilder tb) throws HyracksDataException;

    public void close() throws HyracksDataException;

}
