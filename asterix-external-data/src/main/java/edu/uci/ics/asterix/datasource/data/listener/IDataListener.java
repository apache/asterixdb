/*
 * Copyright 2009-2011 by The Regents of the University of California
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
package edu.uci.ics.asterix.datasource.data.listener;

import java.nio.ByteBuffer;

/**
 * An interface providing a call back API for a subscriber interested in data
 * received from an external data source via the datasource adapter.
 */
public interface IDataListener {

    /**
     * This method is a call back API and is invoked by an instance of
     * IPushBasedDatasourceReadAdapter. The caller passes a frame containing new
     * data. The protocol as to when the caller shall invoke this method is
     * decided by the configured @see DataListenerProperty .
     * 
     * @param aObjects
     */

    public void dataReceived(ByteBuffer frame);

}
