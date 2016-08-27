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
import java.io.EOFException;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.asterix.fuzzyjoin.similarity.SimilarityFilters;

public class RecordGroupLengthCount extends RecordGroup {
    private final int min;
    private final int max;
    private final int[] lengthGroups;

    public RecordGroupLengthCount(int noGroups, SimilarityFilters fuzzyFilters, String lengthstatsPath) {
        super(noGroups, fuzzyFilters);

        int sum = 0;
        int range = 0;

        try (DataInputStream in = new DataInputStream(new FileInputStream(lengthstatsPath))) {
            min = in.readInt();
            max = in.readInt();
            range = max - min + 1;
            lengthGroups = new int[range]; // stores freqencies initally
            try {
                while (true) {
                    int length = in.readInt();
                    int freq = in.readInt();

                    int lowAbs = fuzzyFilters.getLengthLowerBound(length);
                    int uppAbs = fuzzyFilters.getLengthUpperBound(length);

                    int low = Math.max(lowAbs - min, 0);
                    int upp = Math.min(uppAbs - min, max - min);

                    for (int l = low; l <= upp; ++l) {
                        lengthGroups[l] += freq;
                        sum += freq;
                    }
                }
            } catch (EOFException e) {
            }
        } catch (IOException ioe) {
            throw new RuntimeException(ioe);
        }

        int countGroup = sum / noGroups;
        int count = 0;
        int group = 0;
        for (int i = 0; i < range; ++i) {
            count += lengthGroups[i];
            lengthGroups[i] = group;
            if (count >= countGroup && group < noGroups - 1) {
                count = 0;
                group++;
            }
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
            int group = lengthGroups[l];
            if (group != prevGroup) {
                groups.add(group);
                prevGroup = group;
            }
        }

        // System.out.println(noGroups + ":" + length + " [" + low + "," + upp
        // + "] " + groups);

        return groups;
    }

    @Override
    public boolean isLengthOnly() {
        return true;
    }

}
