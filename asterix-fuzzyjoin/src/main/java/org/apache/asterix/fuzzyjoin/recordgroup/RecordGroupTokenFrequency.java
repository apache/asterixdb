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

import java.util.Collection;
import java.util.LinkedList;

import org.apache.asterix.fuzzyjoin.similarity.SimilarityFilters;

public class RecordGroupTokenFrequency extends RecordGroup {

    private final Collection<Integer> groups = new LinkedList<Integer>();

    public RecordGroupTokenFrequency(int noGroups, SimilarityFilters fuzzyFilters) {
        super(noGroups, fuzzyFilters);
    }

    @Override
    public Iterable<Integer> getGroups(Integer token, Integer length) {
        groups.clear();
        groups.add(token % noGroups);
        return groups;
    }

    @Override
    public boolean isLengthOnly() {
        return false;
    }

}
