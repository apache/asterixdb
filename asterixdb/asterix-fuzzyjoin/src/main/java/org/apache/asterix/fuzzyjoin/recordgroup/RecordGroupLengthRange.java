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

import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.asterix.fuzzyjoin.similarity.SimilarityFilters;

public class RecordGroupLengthRange extends RecordGroup {
    private final int min;
    private final int max;
    private final int groupSize;

    public RecordGroupLengthRange(int noGroups, SimilarityFilters fuzzyFilters, String lengthstatsPath) {
        super(noGroups, fuzzyFilters);
        try (DataInputStream in = new DataInputStream(new FileInputStream(lengthstatsPath))) {
            min = in.readInt();
            max = in.readInt();
            groupSize = (int) Math.ceil((max - min + 1f) / noGroups);
        } catch (IOException ioe) {
            throw new RuntimeException(ioe);
        }
    }

    @Override
    public Iterable<Integer> getGroups(Integer token, Integer length) {
        int lowAbs = fuzzyFilters.getLengthLowerBound(length);
        int uppAbs = fuzzyFilters.getLengthUpperBound(length);

        int low = Math.max(lowAbs - min, 0);
        int upp = Math.min(uppAbs - min, max - min);

        ArrayList<Integer> groups = new ArrayList<Integer>(upp - low + 1);
        int prevGroup = -1;
        for (int l = low; l <= upp; ++l) {
            int group = l / groupSize;
            if (group != prevGroup) {
                groups.add(group);
                prevGroup = group;
            }
        }

        // System.out.println(length + " [" + lowAbs + "," + uppAbs + "] "
        // + groups);

        return groups;
    }

    @Override
    public boolean isLengthOnly() {
        return true;
    }

}
