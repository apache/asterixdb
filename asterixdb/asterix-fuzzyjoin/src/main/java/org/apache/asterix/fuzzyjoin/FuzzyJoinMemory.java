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

package org.apache.asterix.fuzzyjoin;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.List;

import org.apache.asterix.fuzzyjoin.invertedlist.InvertedListLengthList;
import org.apache.asterix.fuzzyjoin.invertedlist.InvertedListsLengthList;
import org.apache.asterix.fuzzyjoin.similarity.SimilarityFiltersJaccard;

public class FuzzyJoinMemory {
    public static void main(String[] args) throws IOException {
        if (args.length < 2) {
            System.err.println("Usage: <threshold> <file> [no runs, e.g., 1] [warm-up factor, e.g., 1]");
            System.exit(2);
        }

        float similarityThreshold = Float.parseFloat(args[0]);
        String fileName = args[1];

        int noRuns = 1, warmUpFactor = 1;
        if (args.length > 2) {
            noRuns = Integer.valueOf(args[2]);
            if (args.length > 3) {
                warmUpFactor = Integer.valueOf(args[3]);
            }
        }

        System.err.println("Document: " + fileName);
        System.err.println("... LOADING DATASET ...");

        ArrayList<int[]> records = new ArrayList<>();
        ArrayList<Integer> rids = new ArrayList<>();

        FuzzyJoinMemory fj = new FuzzyJoinMemory(similarityThreshold);

        FuzzyJoinMemory.readRecords(fileName, records, rids);

        System.err.println("Algorithm: ppjoin");
        System.err.println("Threshold: Jaccard " + similarityThreshold);

        List<ResultSelfJoin> results = fj.runs(records, noRuns, warmUpFactor);

        for (ResultSelfJoin result : results) {
            System.out.format("%d %d %.3f", rids.get(result.indexX), rids.get(result.indexY), result.similarity);
            System.out.println();
            // System.out.format("(" + result.indexX + "," + result.indexY +
            // ")");
            // System.out.println();
            // System.out.format("(" + rids.get(result.indexX) + ","
            // + rids.get(result.indexY) + ")\t" + result.similarity);
            // System.out.println();
        }
    }

    @SuppressWarnings("squid:S1166") // Either log or rethrow this exception
    public static void readRecords(String fileName, List<int[]> records, List<Integer> rids) throws IOException {
        try (LittleEndianIntInputStream in =
                new LittleEndianIntInputStream(new BufferedInputStream(new FileInputStream(fileName)))) {

            while (true) {
                int rid = 0;
                try {
                    rid = in.readInt();
                } catch (IOException e) {
                    // FILE_EXPECTED reach of EOF
                    break;
                }

                rids.add(rid);
                int[] record;

                int size = in.readInt();
                record = new int[size];
                for (int j = 0; j < size; j++) {
                    int token = in.readInt();
                    record[j] = token;
                }

                records.add(record);
            }
        }
    }

    private final InvertedListsLengthList invertedLists;
    private final SimilarityFiltersJaccard similarityFilters;

    private final ArrayList<int[]> records;

    public FuzzyJoinMemory(float similarityThreshold) {
        invertedLists = new InvertedListsLengthList();
        similarityFilters = new SimilarityFiltersJaccard(similarityThreshold);
        records = new ArrayList<>();
    }

    public void add(final int[] tokens) {
        final int index = records.size();
        final int length = tokens.length;
        final int indexPrefixLength = similarityFilters.getPrefixLength(length);

        for (int indexToken = 0; indexToken < indexPrefixLength; indexToken++) {
            invertedLists.index(tokens[indexToken], new int[] { index, indexToken, length });
        }
        records.add(tokens);
    }

    public ArrayList<ResultJoin> join(final int[] tokens, final int length) {
        final int prefixLength = similarityFilters.getPrefixLength(length);
        final int lengthLowerBound = similarityFilters.getLengthLowerBound(length);
        //
        // self join
        //
        final HashMap<Integer, Integer> counts = new HashMap<>();
        for (int indexToken = 0; indexToken < Math.min(prefixLength, tokens.length); indexToken++) {
            final int token = tokens[indexToken];
            //
            // probe index
            //
            InvertedListLengthList invertedList = invertedLists.get(token);
            if (invertedList != null) {
                // length filter
                invertedList.setMinLength(lengthLowerBound);
                for (int[] element : invertedList) {
                    final int indexProbe = element[0];
                    final int indexTokenProbe = element[1];
                    final int lengthProbe = element[2];
                    Integer count = counts.get(indexProbe);
                    if (count == null) {
                        count = 0;
                    }

                    if (count != -1) {
                        count++;
                        // position filter
                        if (!similarityFilters.passPositionFilter(count, indexToken, length, indexTokenProbe,
                                lengthProbe)) {
                            count = -1;
                        }
                        // suffix filter
                        if (count == 1 && !similarityFilters.passSuffixFilter(tokens, indexToken,
                                records.get(indexProbe), indexTokenProbe)) {
                            count = -1;
                        }
                        counts.put(indexProbe, count);
                    }
                }
            }
        }
        //
        // verify candidates
        //
        ArrayList<ResultJoin> results = new ArrayList<>();
        counts.forEach((key, value) -> {
            int count = value;
            int indexProbe = key;
            if (count > 0) {
                int tokensProbe[] = records.get(indexProbe);
                float similarity = similarityFilters.passSimilarityFilter(tokens, prefixLength, tokensProbe,
                        similarityFilters.getPrefixLength(tokensProbe.length), count);
                if (similarity > 0) {
                    results.add(new ResultJoin(indexProbe, similarity));
                }
            }
        });
        return results;
    }

    public void prune(int length) {
        final int lengthLowerBound = similarityFilters.getLengthLowerBound(length + 1);
        invertedLists.prune(lengthLowerBound);
    }

    public List<ResultSelfJoin> runs(Collection<int[]> records, int noRuns, int warmupFactor) {
        if (records.size() < 2) {
            return new ArrayList<>();
        }

        int noRunsTotal = noRuns * warmupFactor;
        float runtime = 0, runtimeAverage = 0;
        ArrayList<ResultSelfJoin> results = new ArrayList<>();

        System.err.println("# Records: " + records.size());
        System.err.print("=== BEGIN JOIN (TIMER STARTED) === ");
        for (int i = 1; i <= noRunsTotal; i++) {
            System.err.print(".");
            System.err.flush();

            results.clear();
            Runtime.getRuntime().gc();

            Date startTime = new Date();
            for (int[] record : records) {
                results.addAll(selfJoinAndAddRecord(record));
            }
            Date endTime = new Date();
            runtime = (endTime.getTime() - startTime.getTime()) / (float) 1000.0;

            if (i >= noRunsTotal - noRuns) {
                runtimeAverage += runtime;
            }
        }
        System.err.println();
        System.err.println("# Results: " + results.size());
        System.err.println("=== END JOIN (TIMER STOPPED) ===");
        System.err.println("Total Running Time:  " + runtimeAverage / noRuns + " (" + runtime + ")");
        System.err.println();
        return results;
    }

    public ArrayList<ResultSelfJoin> selfJoinAndAddRecord(final int[] tokens) {
        final int index = records.size();
        final int length = tokens.length;
        final int prefixLength = similarityFilters.getPrefixLength(length);
        final int indexPrefixLength = similarityFilters.getIndexPrefixLength(length);
        final int lengthLowerBound = similarityFilters.getLengthLowerBound(length);
        //
        // self join
        //
        final HashMap<Integer, Integer> counts = new HashMap<>();
        for (int indexToken = 0; indexToken < prefixLength; indexToken++) {
            final int token = tokens[indexToken];
            //
            // probe index
            //
            InvertedListLengthList invertedList = invertedLists.get(token);
            if (invertedList != null) {
                // length filter
                invertedList.setMinLength(lengthLowerBound);
                for (int[] element : invertedList) {
                    final int indexProbe = element[0];
                    final int indexTokenProbe = element[1];
                    final int lengthProbe = element[2];
                    Integer count = counts.get(indexProbe);
                    if (count == null) {
                        count = 0;
                    }

                    if (count != -1) {
                        count++;
                        // position filter
                        if (!similarityFilters.passPositionFilter(count, indexToken, length, indexTokenProbe,
                                lengthProbe)) {
                            count = -1;
                        }
                        // suffix filter
                        if (count == 1 && !similarityFilters.passSuffixFilter(tokens, indexToken,
                                records.get(indexProbe), indexTokenProbe)) {
                            count = -1;
                        }
                        counts.put(indexProbe, count);
                    }
                }
            }
            //
            // add to index
            //
            if (indexToken < indexPrefixLength) {
                invertedLists.index(token, new int[] { index, indexToken, length });
            }
        }
        //
        // add record
        //
        records.add(tokens);
        //
        // verify candidates
        //
        ArrayList<ResultSelfJoin> results = new ArrayList<>();
        counts.forEach((key, value) -> {
            int count = value;
            int indexProbe = key;
            if (count > 0) {
                int tokensProbe[] = records.get(indexProbe);
                float similarity = similarityFilters.passSimilarityFilter(tokens, prefixLength, tokensProbe,
                        similarityFilters.getIndexPrefixLength(tokensProbe.length), count);
                if (similarity > 0) {
                    results.add(new ResultSelfJoin(index, indexProbe, similarity));
                }
            }
        });
        return results;
    }
}
