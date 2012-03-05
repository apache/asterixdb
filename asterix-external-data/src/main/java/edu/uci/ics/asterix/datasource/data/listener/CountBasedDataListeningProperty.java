/*
 * Copyright 2009-2010 by The Regents of the University of California
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
 * A data listening property chosen by a data listener when it wants data to be
 * pushed when the count of records collected by the adapter exceeds a confiured
 * count value.
 */
public class CountBasedDataListeningProperty extends AbstractDataListeningProperty {

    int numTuples;

    public int getNumTuples() {
        return numTuples;
    }

    public void setNumTuples(int numTuples) {
        this.numTuples = numTuples;
    }

    public CountBasedDataListeningProperty(int numTuples) {
        this.numTuples = numTuples;
    }

    @Override
    public listeningPropertyType getListeningPropretyType() {
        return listeningPropertyType.COUNT_BASED;
    }
}
