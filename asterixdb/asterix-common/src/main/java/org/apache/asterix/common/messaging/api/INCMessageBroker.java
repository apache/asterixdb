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

import org.apache.hyracks.api.control.CcId;
import org.apache.hyracks.api.messages.IMessageBroker;

public interface INCMessageBroker extends IMessageBroker {

    /**
     * Sends application message from this NC to the primary CC.
     *
     * @param message
     * @throws Exception
     */
    public void sendMessageToPrimaryCC(ICcAddressedMessage message) throws Exception;

    /**
     * Sends application message from this NC to the CC.
     *
     * @param message
     * @throws Exception
     */
    public void sendMessageToCC(CcId ccId, ICcAddressedMessage message) throws Exception;

    /**
     * Sends application message from this NC to another NC.
     *
     * @param message
     * @throws Exception
     */
    public void sendMessageToNC(String nodeId, INcAddressedMessage message) throws Exception;

    /**
     * Queue a message to this {@link INCMessageBroker} for processing
     *
     * @param msg
     */
    public void queueReceivedMessage(INcAddressedMessage msg);

    /**
     * Creates and registers a Future for a message that will be send through this broker
     * @return new Future
     */
    MessageFuture registerMessageFuture();

    /**
     * Removes a previously registered Future
     * @param futureId future identifier
     * @return existing Future or {@code null} if there was no Future associated with this identifier
     */
    MessageFuture deregisterMessageFuture(long futureId);
}
