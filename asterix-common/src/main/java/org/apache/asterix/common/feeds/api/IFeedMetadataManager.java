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
package edu.uci.ics.asterix.common.feeds.api;

import edu.uci.ics.asterix.common.exceptions.AsterixException;
import edu.uci.ics.asterix.common.feeds.FeedConnectionId;

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
