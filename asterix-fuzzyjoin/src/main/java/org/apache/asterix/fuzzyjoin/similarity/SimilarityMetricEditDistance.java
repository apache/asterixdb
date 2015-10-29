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

package org.apache.asterix.fuzzyjoin.similarity;

import java.util.Arrays;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.util.string.UTF8StringUtil;

public class SimilarityMetricEditDistance implements IGenericSimilarityMetric {

    // dp implementation only needs 2 rows
    private final int rows = 2;
    private int cols;
    private int[][] matrix;

    // for letter count filtering
    private final int[] fsLcCount = new int[128];
    private final int[] ssLcCount = new int[128];

    public SimilarityMetricEditDistance() {
        cols = 100; // arbitrary default value
        matrix = new int[rows][cols];
    }

    @Override
    public float getSimilarity(IListIterator firstList, IListIterator secondList) throws HyracksDataException {
        int flLen = firstList.size();
        int slLen = secondList.size();

        // reuse existing matrix if possible
        if (slLen >= cols) {
            cols = slLen + 1;
            matrix = new int[rows][cols];
        }

        // init matrix
        for (int i = 0; i <= slLen; i++) {
            matrix[0][i] = i;
        }

        int currRow = 1;
        int prevRow = 0;

        // expand dynamic programming matrix row by row
        for (int i = 1; i <= flLen; i++) {
            matrix[currRow][0] = i;

            secondList.reset();
            for (int j = 1; j <= slLen; j++) {

                matrix[currRow][j] = Math.min(Math.min(matrix[prevRow][j] + 1, matrix[currRow][j - 1] + 1),
                        matrix[prevRow][j - 1] + (firstList.compare(secondList) == 0 ? 0 : 1));

                secondList.next();
            }

            firstList.next();

            int tmp = currRow;
            currRow = prevRow;
            prevRow = tmp;
        }

        return matrix[prevRow][slLen];
    }

    @Override
    public float getSimilarity(IListIterator firstList, IListIterator secondList, float simThresh)
            throws HyracksDataException {

        int edThresh = (int) simThresh;

        int flLen = firstList.size();
        int slLen = secondList.size();

        // length filter
        if (Math.abs(flLen - slLen) > edThresh) {
            return -1;
        }

        float ed = getSimilarity(firstList, secondList);
        if (ed > edThresh) {
            return -1;
        } else {
            return ed;
        }
    }

    public int getSimilarityContains(IListIterator exprList, IListIterator patternList, int simThresh)
            throws HyracksDataException {
        int exprLen = exprList.size();
        int patternLen = patternList.size();

        // reuse existing matrix if possible
        if (patternLen >= cols) {
            cols = patternLen + 1;
            matrix = new int[rows][cols];
        }

        // init matrix
        for (int i = 0; i <= patternLen; i++) {
            matrix[0][i] = i;
        }

        int currRow = 1;
        int prevRow = 0;
        int minEd = Integer.MAX_VALUE;
        // expand dynamic programming matrix row by row
        for (int i = 1; i <= exprLen; i++) {
            matrix[currRow][0] = 0;

            patternList.reset();
            for (int j = 1; j <= patternLen; j++) {

                matrix[currRow][j] = Math.min(Math.min(matrix[prevRow][j] + 1, matrix[currRow][j - 1] + 1),
                        matrix[prevRow][j - 1] + (exprList.compare(patternList) == 0 ? 0 : 1));

                patternList.next();

                if (j == patternLen && matrix[currRow][patternLen] < minEd) {
                    minEd = matrix[currRow][patternLen];
                }
            }

            exprList.next();

            int tmp = currRow;
            currRow = prevRow;
            prevRow = tmp;
        }

        if (minEd > simThresh) {
            return -1;
        } else {
            return minEd;
        }
    }

    // faster implementation for common case of string edit distance
    public int UTF8StringEditDistance(byte[] bytes, int fsStart, int ssStart) {

        int fsLen = UTF8StringUtil.getStringLength(bytes, fsStart);
        int ssLen = UTF8StringUtil.getStringLength(bytes, ssStart);

        int fsUtfLen = UTF8StringUtil.getUTFLength(bytes, fsStart);
        int ssUtfLen = UTF8StringUtil.getUTFLength(bytes, ssStart);
        int fsMetaLen = UTF8StringUtil.getNumBytesToStoreLength(fsUtfLen);
        int ssMetaLen = UTF8StringUtil.getNumBytesToStoreLength(ssUtfLen);

        // reuse existing matrix if possible
        if (ssLen >= cols) {
            cols = ssLen + 1;
            matrix = new int[rows][cols];
        }

        int fsDataStart = fsStart + fsMetaLen;
        int ssDataStart = ssStart + ssMetaLen;

        // init matrix
        for (int i = 0; i <= ssLen; i++) {
            matrix[0][i] = i;
        }

        int currRow = 1;
        int prevRow = 0;

        // expand dynamic programming matrix row by row
        int fsPos = fsDataStart;
        for (int i = 1; i <= fsLen; i++) {
            matrix[currRow][0] = i;
            char fsChar = Character.toLowerCase(UTF8StringUtil.charAt(bytes, fsPos));

            int ssPos = ssDataStart;
            for (int j = 1; j <= ssLen; j++) {
                char ssChar = Character.toLowerCase(UTF8StringUtil.charAt(bytes, ssPos));

                matrix[currRow][j] = Math.min(Math.min(matrix[prevRow][j] + 1, matrix[currRow][j - 1] + 1),
                        matrix[prevRow][j - 1] + (fsChar == ssChar ? 0 : 1));

                ssPos += UTF8StringUtil.charSize(bytes, ssPos);
            }

            fsPos += UTF8StringUtil.charSize(bytes, fsPos);

            int tmp = currRow;
            currRow = prevRow;
            prevRow = tmp;
        }

        return matrix[prevRow][ssLen];
    }

    public int UTF8StringEditDistance(byte[] bytes, int fsStart, int ssStart, int edThresh) {

        int fsStrLen = UTF8StringUtil.getStringLength(bytes, fsStart);
        int ssStrLen = UTF8StringUtil.getStringLength(bytes, ssStart);

        int fsUtfLen = UTF8StringUtil.getUTFLength(bytes, fsStart);
        int ssUtfLen = UTF8StringUtil.getUTFLength(bytes, ssStart);
        int fsMetaLen = UTF8StringUtil.getNumBytesToStoreLength(fsUtfLen);
        int ssMetaLen = UTF8StringUtil.getNumBytesToStoreLength(ssUtfLen);

        // length filter
        if (Math.abs(fsStrLen - ssStrLen) > edThresh) {
            return -1;
        }

        // initialize letter count filtering
        Arrays.fill(fsLcCount, 0);
        Arrays.fill(ssLcCount, 0);

        // compute letter counts for first string
        int fsPos = fsStart + fsMetaLen;
        int fsEnd = fsPos + fsUtfLen;;
        while (fsPos < fsEnd) {
            char c = Character.toLowerCase(UTF8StringUtil.charAt(bytes, fsPos));
            if (c < 128) {
                fsLcCount[c]++;
            }
            fsPos += UTF8StringUtil.charSize(bytes, fsPos);
        }

        // compute letter counts for second string
        int ssPos = ssStart + ssMetaLen;
        int ssEnd = ssPos + ssUtfLen;
        while (ssPos < ssEnd) {
            char c = Character.toLowerCase(UTF8StringUtil.charAt(bytes, ssPos));
            if (c < 128) {
                ssLcCount[c]++;
            }
            ssPos += UTF8StringUtil.charSize(bytes, ssPos);
        }

        // apply filter
        int gtSum = 0;
        int ltSum = 0;
        for (int i = 0; i < 128; i++) {
            if (fsLcCount[i] > ssLcCount[i]) {
                gtSum += fsLcCount[i] - ssLcCount[i];
                if (gtSum > edThresh) {
                    return -1;
                }
            } else {
                ltSum += ssLcCount[i] - fsLcCount[i];
                if (ltSum > edThresh) {
                    return -1;
                }
            }
        }

        int ed = UTF8StringEditDistance(bytes, fsStart, ssStart);
        if (ed > edThresh) {
            return -1;
        } else {
            return ed;
        }
    }

    // checks whether the first string contains a similar string to the second string
    public int UTF8StringEditDistanceContains(byte[] bytes, int stringStart, int patternStart, int edThresh) {

        int stringLen = UTF8StringUtil.getStringLength(bytes, stringStart);
        int patternLen = UTF8StringUtil.getStringLength(bytes, patternStart);

        int stringUTFLen = UTF8StringUtil.getUTFLength(bytes, stringStart);
        int stringMetaLen = UTF8StringUtil.getNumBytesToStoreLength(stringUTFLen);

        int patternUTFLen = UTF8StringUtil.getUTFLength(bytes, patternStart);
        int patternMetaLen = UTF8StringUtil.getNumBytesToStoreLength(patternUTFLen);

        // reuse existing matrix if possible
        if (patternLen >= cols) {
            cols = patternLen + 1;
            matrix = new int[rows][cols];
        }

        int stringDataStart = stringStart +  stringMetaLen;
        int patternDataStart = patternStart + patternMetaLen;

        // init matrix
        for (int i = 0; i <= patternLen; i++) {
            matrix[0][i] = i;
        }

        int currRow = 1;
        int prevRow = 0;
        int minEd = Integer.MAX_VALUE;
        // expand dynamic programming matrix row by row
        int stringPos = stringDataStart;
        for (int i = 1; i <= stringLen; i++) {
            matrix[currRow][0] = 0;
            char stringChar = Character.toLowerCase(UTF8StringUtil.charAt(bytes, stringPos));

            int patternPos = patternDataStart;
            for (int j = 1; j <= patternLen; j++) {
                char patternChar = Character.toLowerCase(UTF8StringUtil.charAt(bytes, patternPos));

                matrix[currRow][j] = Math.min(Math.min(matrix[prevRow][j] + 1, matrix[currRow][j - 1] + 1),
                        matrix[prevRow][j - 1] + (stringChar == patternChar ? 0 : 1));

                patternPos += UTF8StringUtil.charSize(bytes, patternPos);

                if (j == patternLen && matrix[currRow][patternLen] < minEd) {
                    minEd = matrix[currRow][patternLen];
                }
            }

            stringPos += UTF8StringUtil.charSize(bytes, stringPos);

            int tmp = currRow;
            currRow = prevRow;
            prevRow = tmp;
        }
        if (minEd > edThresh) {
            return -1;
        } else {
            return minEd;
        }
    }
}
