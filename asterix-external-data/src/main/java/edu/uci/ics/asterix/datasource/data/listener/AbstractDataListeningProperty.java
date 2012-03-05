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

/**
 * A push-based datasource adapter allows registering a IDataListener instance.
 * Data listening property defines when data is pushed to a IDataListener.
 */

public abstract class AbstractDataListeningProperty {

    /**
     * COUNT_BASED: Data is pushed to a data listener only if the count of
     * records exceeds the configured threshold value. TIME_BASED: Data is
     * pushed to a data listener in a periodic manner at the end of each time
     * interval.
     */
    public enum listeningPropertyType {
        COUNT_BASED,
        TIME_BASED
    }

    public abstract listeningPropertyType getListeningPropretyType();
}
