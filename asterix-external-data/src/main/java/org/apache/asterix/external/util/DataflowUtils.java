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
package org.apache.asterix.external.util;

import java.util.Map;

import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.common.parse.ITupleForwarder;
import org.apache.asterix.common.parse.ITupleForwarder.TupleForwardPolicy;
import org.apache.asterix.external.dataflow.CounterTimerTupleForwarder;
import org.apache.asterix.external.dataflow.FrameFullTupleForwarder;
import org.apache.asterix.external.dataflow.RateControlledTupleForwarder;
import org.apache.hyracks.api.comm.IFrameWriter;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAppender;

public class DataflowUtils {
    public static void addTupleToFrame(FrameTupleAppender appender, ArrayTupleBuilder tb, IFrameWriter writer)
            throws HyracksDataException {
        if (!appender.append(tb.getFieldEndOffsets(), tb.getByteArray(), 0, tb.getSize())) {
            appender.flush(writer, true);
            if (!appender.append(tb.getFieldEndOffsets(), tb.getByteArray(), 0, tb.getSize())) {
                throw new IllegalStateException();
            }
        }
    }

    public static ITupleForwarder getTupleForwarder(Map<String, String> configuration) throws AsterixException {
        ITupleForwarder policy = null;
        ITupleForwarder.TupleForwardPolicy policyType = null;
        String propValue = configuration.get(ITupleForwarder.FORWARD_POLICY);
        if (propValue == null) {
            policyType = TupleForwardPolicy.FRAME_FULL;
        } else {
            policyType = TupleForwardPolicy.valueOf(propValue.trim().toUpperCase());
        }
        switch (policyType) {
            case FRAME_FULL:
                policy = new FrameFullTupleForwarder();
                break;
            case COUNTER_TIMER_EXPIRED:
                policy = new CounterTimerTupleForwarder();
                break;
            case RATE_CONTROLLED:
                policy = new RateControlledTupleForwarder();
                break;
            default:
                throw new AsterixException("Unknown tuple forward policy");
        }
        return policy;
    }
}
