/**
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

package org.apache.asterix.fuzzyjoin.recordgroup;

import org.apache.asterix.fuzzyjoin.similarity.SimilarityFilters;

public class RecordGroupFactory {
    public static RecordGroup getRecordGroup(String recordGroup, int noGroups, SimilarityFilters fuzzyFilters,
            String lengthstatsPath) {
        if (recordGroup.equals("LengthCount")) {
            return new RecordGroupLengthCount(noGroups, fuzzyFilters, lengthstatsPath);
        } else if (recordGroup.equals("LengthIdentity")) {
            return new RecordGroupLengthIdentity(noGroups, fuzzyFilters);
        } else if (recordGroup.equals("LengthRange")) {
            return new RecordGroupLengthRange(noGroups, fuzzyFilters, lengthstatsPath);
        } else if (recordGroup.equals("Single")) {
            return new RecordGroupSingle(noGroups, fuzzyFilters);
        } else if (recordGroup.equals("TokenIdentity")) {
            return new RecordGroupTokenIdentity(noGroups, fuzzyFilters);
        } else if (recordGroup.equals("TokenFrequency")) {
            return new RecordGroupTokenFrequency(noGroups, fuzzyFilters);
        } else if (recordGroup.equals("TokenFrequencyMirror")) {
            return new RecordGroupTokenFrequencyMirror(noGroups, fuzzyFilters);
        }
        throw new RuntimeException("Unknown record group \"" + recordGroup + "\".");
    }
}
