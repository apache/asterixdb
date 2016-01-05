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

import org.apache.hyracks.api.messages.IMessageBroker;

public interface INCMessageBroker extends IMessageBroker {

    /**
     * Sends application message from this NC to the CC.
     *
     * @param message
     * @param callback
     * @throws Exception
     */
    public void sendMessage(IApplicationMessage message, IApplicationMessageCallback callback) throws Exception;

    /**
     * Sends the maximum resource id on this NC to the CC.
     *
     * @throws Exception
     */
    public void reportMaxResourceId() throws Exception;
}