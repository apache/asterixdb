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
package org.apache.asterix.common.feeds.api;

import java.io.Serializable;

import org.apache.hyracks.api.dataflow.value.JSONSerializable;

/**
 * A control message exchanged between {@Link IFeedManager} and {@Link CentralFeedManager} that requests for an action or reporting of an event
 */
public interface IFeedMessage extends Serializable, JSONSerializable {

    public enum MessageType {
        END,
        XAQL,
        FEED_REPORT,
        NODE_REPORT,
        STORAGE_REPORT,
        CONGESTION,
        PREPARE_STALL,
        TERMINATE_FLOW,
        SCALE_IN_REQUEST,
        COMMIT_ACK,
        COMMIT_ACK_RESPONSE,
        THROTTLING_ENABLED
    }

    /**
     * Gets the type associated with this message
     * 
     * @return MessageType type associated with this message
     */
    public MessageType getMessageType();

}
