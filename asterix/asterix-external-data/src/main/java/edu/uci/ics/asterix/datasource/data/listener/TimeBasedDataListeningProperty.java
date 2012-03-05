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
 * A data listening property chosen by a data listener when it needs data to be
 * pushed in a periodic manner with a configured time-interval.
 */
public class TimeBasedDataListeningProperty extends AbstractDataListeningProperty {

    // time interval in secs
    int interval;

    public int getInteval() {
        return interval;
    }

    public void setInterval(int interval) {
        this.interval = interval;
    }

    public TimeBasedDataListeningProperty(int interval) {
        this.interval = interval;
    }

    @Override
    public listeningPropertyType getListeningPropretyType() {
        return listeningPropertyType.TIME_BASED;
    }
}
