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

import org.apache.asterix.external.api.ITupleForwarder;
import org.apache.asterix.external.api.ITupleForwarder.TupleForwardPolicy;
import org.apache.asterix.external.dataflow.CounterTimerTupleForwarder;
import org.apache.asterix.external.dataflow.FeedTupleForwarder;
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
            appender.write(writer, true);
            if (!appender.append(tb.getFieldEndOffsets(), tb.getByteArray(), 0, tb.getSize())) {
                throw new HyracksDataException("Tuple is too large for a frame");
            }
        }
    }

    public static ITupleForwarder getTupleForwarder(Map<String, String> configuration, FeedLogManager feedLogManager)
            throws HyracksDataException {
        ITupleForwarder.TupleForwardPolicy policyType = null;
        String propValue = configuration.get(ITupleForwarder.FORWARD_POLICY);
        if (ExternalDataUtils.isFeed(configuration)) {
            // TODO pass this value in the configuration and avoid this check for feeds
            policyType = TupleForwardPolicy.FEED;
        } else if (propValue == null) {
            policyType = TupleForwardPolicy.FRAME_FULL;
        } else {
            policyType = TupleForwardPolicy.valueOf(propValue.trim().toUpperCase());
        }
        switch (policyType) {
            case FEED:
                return new FeedTupleForwarder(feedLogManager);
            case FRAME_FULL:
                return new FrameFullTupleForwarder();
            case COUNTER_TIMER_EXPIRED:
                return CounterTimerTupleForwarder.create(configuration);
            case RATE_CONTROLLED:
                return RateControlledTupleForwarder.create(configuration);
            default:
                throw new HyracksDataException("Unknown tuple forward policy");
        }
    }
}
