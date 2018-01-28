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

import java.util.Set;
import java.util.TreeSet;

import org.apache.asterix.fuzzyjoin.tokenizer.Tokenizer;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.util.ISequenceIterator;

public class SimilarityMetricJaccard extends SimilarityMetric implements IGenericSimilarityMetric {

    public static float getSimilarity(int intersectSize, int lengthX, int lengthY) {
        return ((float) intersectSize) / (lengthX + lengthY - intersectSize);
    }

    public static float getSimilarityBag(int[] tokensX, int[] tokensY) {
        Set<Integer> setX = new TreeSet<Integer>();
        for (int token : tokensX) {
            setX.add(token);
        }
        Set<Integer> setY = new TreeSet<Integer>();
        for (int token : tokensY) {
            setY.add(token);
        }
        setX.retainAll(setY);
        return ((float) setX.size()) / (tokensX.length + tokensY.length - setX.size());
    }

    @Override
    public float computeSimilarity(ISequenceIterator tokensX, ISequenceIterator tokensY) throws HyracksDataException {
        int intersectionSize = SimilarityMetric.getIntersectSize(tokensX, tokensY);
        int totalSize = tokensX.size() + tokensY.size();

        return (float) intersectionSize / (totalSize - intersectionSize);
    }

    @Override
    public float computeSimilarity(ISequenceIterator firstList, ISequenceIterator secondList, float simThresh)
            throws HyracksDataException {

        // apply length filter
        int lengthLowerBound = (int) Math.ceil(simThresh * firstList.size());

        boolean passesLengthFilter =
                (lengthLowerBound <= secondList.size()) && (secondList.size() <= 1.0f / simThresh * firstList.size());
        if (!passesLengthFilter) {
            return -1f;
        }

        float jacc = computeSimilarity(firstList, secondList);
        if (jacc < simThresh) {
            return -1f;
        } else {
            return jacc;
        }
    }

    @Override
    public float getSimilarity(int[] tokensX, int startX, int lengthX, int[] tokensY, int startY, int lengthY) {
        int intersectionSize = SimilarityMetric.getIntersectSize(tokensX, startX, lengthX, tokensY, startY, lengthY);
        int totalSize = lengthX + lengthY;

        return (float) intersectionSize / (totalSize - intersectionSize);
    }

    @Override
    public float getSimilarity(int[] tokensX, int[] tokensY) {
        return getSimilarity(tokensX, 0, tokensX.length, tokensY, 0, tokensY.length);
    }

    @Override
    public float getSimilarity(String stringX, String stringY, Tokenizer tokenizer) {
        Set<String> setX = new TreeSet<String>();
        for (String token : tokenizer.tokenize(stringX)) {
            setX.add(token);
        }
        Set<String> setY = new TreeSet<String>();
        for (String token : tokenizer.tokenize(stringY)) {
            setY.add(token);
        }
        int lengthX = setX.size();
        int lengthY = setY.size();
        setX.retainAll(setY);
        return ((float) setX.size()) / (lengthX + lengthY - setX.size());
    }
}
