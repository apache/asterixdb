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

import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.common.feeds.FeedConnectionId;

public interface IFeedMetadataManager {

    /**
     * @param feedConnectionId
     *            connection id corresponding to the feed connection
     * @param tuple
     *            the erroneous tuple that raised an exception
     * @param message
     *            the message corresponding to the exception being raised
     * @param feedManager
     * @throws AsterixException
     */
    public void logTuple(FeedConnectionId feedConnectionId, String tuple, String message, IFeedManager feedManager)
            throws AsterixException;

}
