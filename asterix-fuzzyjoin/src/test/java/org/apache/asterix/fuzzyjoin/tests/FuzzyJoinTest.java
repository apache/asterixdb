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

package edu.uci.ics.asterix.fuzzyjoin.tests;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.util.ArrayList;

import org.junit.Test;

import edu.uci.ics.asterix.fuzzyjoin.FuzzyJoinMemory;
import edu.uci.ics.asterix.fuzzyjoin.ResultSelfJoin;
import edu.uci.ics.asterix.fuzzyjoin.tests.dataset.AbstractDataset;
import edu.uci.ics.asterix.fuzzyjoin.tests.dataset.AbstractDataset.Directory;
import edu.uci.ics.asterix.fuzzyjoin.tests.dataset.DBLPSmallDataset;

public class FuzzyJoinTest {

    private static final AbstractDataset dataset = new DBLPSmallDataset();
    private static final String base = "data/";

    @Test
    public void test() throws Exception {

        ArrayList<int[]> records = new ArrayList<int[]>();
        ArrayList<Integer> rids = new ArrayList<Integer>();
        ArrayList<ResultSelfJoin> results = new ArrayList<ResultSelfJoin>();

        dataset.createDirecotries(new String[] { base });

        FuzzyJoinMemory fj = new FuzzyJoinMemory(dataset.getThreshold());

        FuzzyJoinMemory.readRecords(base + dataset.getPathPart0(Directory.SSJOININ), records, rids);

        for (int[] record : records) {
            results.addAll(fj.selfJoinAndAddRecord(record));
        }

        BufferedWriter out = new BufferedWriter(new FileWriter(base + dataset.getPathPart0(Directory.SSJOINOUT)));
        for (ResultSelfJoin result : results) {
            out.write(String.format("%d %d %.3f\n", rids.get(result.indexX), rids.get(result.indexY), result.similarity));
        }
        out.close();

        FuzzyJoinTestUtil.verifyDirectory(base + dataset.getPathPart0(Directory.SSJOINOUT),
                base + dataset.getPathExpected(Directory.SSJOINOUT));
    }
}
