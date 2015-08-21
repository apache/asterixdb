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
package edu.uci.ics.asterix.common.parse;

import java.util.Map;

import edu.uci.ics.hyracks.api.comm.IFrameWriter;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;

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
