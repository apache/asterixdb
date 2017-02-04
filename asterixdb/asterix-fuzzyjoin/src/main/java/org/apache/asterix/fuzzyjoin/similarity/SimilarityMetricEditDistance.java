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

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.util.ISequenceIterator;
import org.apache.hyracks.data.std.util.UTF8StringCharByCharIterator;
import org.apache.hyracks.util.string.UTF8StringUtil;

public class SimilarityMetricEditDistance implements IGenericSimilarityMetric {

    // This Dynamic Programming implementation only needs 2 rows.
    private final int rows = 2;
    private int cols;
    private int[][] matrix;

    // for string edit-distance calculation
    private final UTF8StringCharByCharIterator leftIt = new UTF8StringCharByCharIterator();
    private final UTF8StringCharByCharIterator rightIt = new UTF8StringCharByCharIterator();

    public static final int SIMILARITY_THRESHOLD_NOT_SATISFIED_VALUE = -1;

    public SimilarityMetricEditDistance() {
        cols = 100; // arbitrary default value
        matrix = new int[rows][cols];
    }

    /**
     * Gets the edit distance value for the given two sequences using a Dynamic Programming approach.
     * If a positive simThresh value is provided, this method only calculates 2 * (simThresh + 1) cells per row,
     * not all the cells in a row as an optimization. Refer to https://en.wikipedia.org/wiki/Wagner–Fischer_algorithm
     * for more details. Also, as one more optimization, during the calculation steps, if this method finds out
     * that the final edit distance value cannot be within simThresh, this method stops the calculation
     * and immediately returns -1.
     * If the final edit distance value is less than or equal to simThresh, then that value will be returned.
     * If a non-positive simThresh is given, then it calculates all cells and rows and returns
     * the final edit distance value.
     *
     * @return the edit distance of the two lists. -1 if a positive simThresh value is given and the edit distance
     *         value is greater than the given simThresh.
     */
    private float computeActualSimilarity(ISequenceIterator firstSequence, ISequenceIterator secondSequence,
            float simThresh) throws HyracksDataException {
        int flLen = firstSequence.size();
        int slLen = secondSequence.size();

        // When a positive threshold is given, then we can apply two optimizations.
        int edThresh = (int) simThresh;
        boolean canTerminateEarly = edThresh >= 0;

        // Reuses the existing matrix if possible.
        if (slLen >= cols) {
            cols = slLen + 1;
            matrix = new int[rows][cols];
        }

        // Inits the matrix.
        for (int i = 0; i <= slLen; i++) {
            matrix[0][i] = i;
        }

        int currRow = 1;
        int prevRow = 0;

        int from = 1;
        int to = slLen;
        int minDistance = -1;

        // Expands the dynamic programming matrix row by row.
        for (int i = 1; i <= flLen; i++) {
            matrix[currRow][0] = i;

            secondSequence.reset();

            // Only calculates 2 * (simThresh + 1) cells per row as an optimization.
            // Also keeps minDistance to see whether the possible edit distance after
            // each row calculation is greater than the simThresh.
            if (canTerminateEarly) {
                minDistance = edThresh + 1;
                from = Math.max(i - edThresh - 1, 1);
                to = Math.min(i + edThresh + 1, slLen);
                for (int j = 1; j < from; j++) {
                    // Moves the pointer of the second list to the point where the calculation starts for this row.
                    secondSequence.next();
                }
                if (from > 1) {
                    // Sets the left Boundary cell value to make sure that the calculation is correct.
                    matrix[currRow][from - 1] = edThresh + 1;
                }
                if (to < slLen) {
                    // Sets the right Boundary cell value to make sure that the calculation is correct.
                    matrix[currRow][to + 1] = edThresh + 1;
                }
            }

            for (int j = from; j <= to; j++) {

                matrix[currRow][j] = Math.min(Math.min(matrix[prevRow][j] + 1, matrix[currRow][j - 1] + 1),
                        matrix[prevRow][j - 1] + (firstSequence.compare(secondSequence) == 0 ? 0 : 1));

                // Replaces minDistance after each cell computation if we find a smaller value than that.
                if (canTerminateEarly && matrix[currRow][j] < minDistance) {
                    minDistance = matrix[currRow][j];
                }

                secondSequence.next();
            }
            // If the minimum distance value is greater than the given threshold, no reason to process next row.
            if (canTerminateEarly && minDistance > edThresh) {
                return SIMILARITY_THRESHOLD_NOT_SATISFIED_VALUE;
            }
            firstSequence.next();

            int tmp = currRow;
            currRow = prevRow;
            prevRow = tmp;
        }

        return matrix[prevRow][slLen];
    }

    /**
     * Gets the similarity value for the given two sequences. If the value doesn't satisfy the given simThresh,
     * this method returns -1. Else, this returns the real similarity value.
     */
    @Override
    public float computeSimilarity(ISequenceIterator firstList, ISequenceIterator secondList, float simThresh)
            throws HyracksDataException {

        int edThresh = (int) simThresh;

        int flLen = firstList.size();
        int slLen = secondList.size();

        // length filter
        if (Math.abs(flLen - slLen) > edThresh) {
            return SIMILARITY_THRESHOLD_NOT_SATISFIED_VALUE;
        }

        float ed = computeActualSimilarity(firstList, secondList, simThresh);
        if (ed > edThresh || ed < 0) {
            return SIMILARITY_THRESHOLD_NOT_SATISFIED_VALUE;
        } else {
            return ed;
        }
    }

    public int getSimilarityContains(ISequenceIterator exprList, ISequenceIterator patternList, int simThresh)
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
            return SIMILARITY_THRESHOLD_NOT_SATISFIED_VALUE;
        } else {
            return minEd;
        }
    }

    // faster implementation for common case of string edit distance
    public int getActualUTF8StringEditDistanceVal(byte[] leftBytes, int fsStart, byte[] rightBytes, int ssStart,
            int edThresh) throws HyracksDataException {
        leftIt.reset(leftBytes, fsStart);
        rightIt.reset(rightBytes, ssStart);
        return (int) computeActualSimilarity(leftIt, rightIt, edThresh);
    }

    public int UTF8StringEditDistance(byte[] bytesLeft, int fsStart, byte[] bytesRight, int ssStart, int edThresh)
            throws HyracksDataException {
        int fsStrLen = UTF8StringUtil.getStringLength(bytesLeft, fsStart);
        int ssStrLen = UTF8StringUtil.getStringLength(bytesRight, ssStart);

        // length filter
        if (Math.abs(fsStrLen - ssStrLen) > edThresh) {
            return SIMILARITY_THRESHOLD_NOT_SATISFIED_VALUE;
        }

        int ed = getActualUTF8StringEditDistanceVal(bytesLeft, fsStart, bytesRight, ssStart, edThresh);
        if (ed > edThresh || ed < 0) {
            return SIMILARITY_THRESHOLD_NOT_SATISFIED_VALUE;
        } else {
            return ed;
        }
    }

    // checks whether the first string contains a similar string to the second string
    public int UTF8StringEditDistanceContains(byte[] strBytes, int stringStart, byte[] patternBytes, int patternStart,
            int edThresh) throws HyracksDataException {
        leftIt.reset(strBytes, stringStart);
        rightIt.reset(patternBytes, patternStart);
        return getSimilarityContains(leftIt, rightIt, edThresh);
    }

    @Override
    public float computeSimilarity(ISequenceIterator firstSequence, ISequenceIterator secondSequence)
            throws HyracksDataException {
        // Passes -1 as the simThresh to calculate the edit distance without applying any calculation optimizations.
        return computeActualSimilarity(firstSequence, secondSequence, -1);
    }
}
