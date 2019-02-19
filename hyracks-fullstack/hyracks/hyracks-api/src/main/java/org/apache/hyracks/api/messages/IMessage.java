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
package org.apache.hyracks.api.messages;

import java.io.Serializable;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.Logger;

public interface IMessage extends Serializable {

    /**
     * @return true if the message is whispered, otherwise false.
     */
    default boolean isWhispered() {
        return false;
    }

    static void logMessage(Logger logger, IMessage msg) {
        final Level logLevel = msg.isWhispered() ? Level.TRACE : Level.INFO;
        if (logger.isEnabled(logLevel)) {
            logger.info("Received message: {}", msg);
        }
    }
}
