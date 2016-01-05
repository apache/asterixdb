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
package org.apache.asterix.common.messaging.api;

import org.apache.hyracks.api.messages.IMessage;

public interface IApplicationMessage extends IMessage {

    public enum ApplicationMessageType {
        RESOURCE_ID_REQUEST,
        RESOURCE_ID_RESPONSE,
        REPORT_MAX_RESOURCE_ID_REQUEST,
        REPORT_MAX_RESOURCE_ID_RESPONSE
    }

    public abstract ApplicationMessageType getMessageType();

    /**
     * Sets a unique message id that identifies this message within an NC.
     * This id is set by {@link INCMessageBroker#sendMessage(IApplicationMessage, IApplicationMessageCallback)}
     * when the callback is not null to notify the sender when the response to that message is received.
     *
     * @param messageId
     */
    public void setId(long messageId);

    /**
     * @return The unique message id if it has been set, otherwise 0.
     */
    public long getId();
}
