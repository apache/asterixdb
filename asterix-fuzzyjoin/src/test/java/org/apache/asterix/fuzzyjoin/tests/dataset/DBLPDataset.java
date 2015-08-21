/**
 * Copyright 2010-2011 The Regents of the University of California
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on
 * an "AS IS"; BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under
 * the License.
 *
 * Author: Rares Vernica <rares (at) ics.uci.edu>
 */

package edu.uci.ics.asterix.fuzzyjoin.tests.dataset;


public class DBLPDataset extends PublicationsDataset {
    private static final String NAME = "dblp";
    private static final int NO_RECORDS = 1268017;
    private static final float THRESHOLD = .8f;
    private static final String RECORD_DATA = "2,3";

    public DBLPDataset() {
        super(NAME, NO_RECORDS, THRESHOLD, RECORD_DATA, NAME, NAME);
    }

    public DBLPDataset(String recordData) {
        super(NAME, NO_RECORDS, THRESHOLD, recordData, NAME, NAME);
    }
}
