/*
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

package org.apache.hyracks.storage.am.common.datagen;

import java.util.Arrays;

public class ProbabilityHelper {
    public static double[] getUniformProbDist(int numChoices) {
        double[] probDist = new double[numChoices];
        for (int i = 0; i < numChoices; i++) {
            probDist[i] = 1.0 / (double) numChoices;
        }
        return probDist;
    }

    public static double[] getZipfProbDist(int numChoices, int zipfSkew) {
        double[] probDist = new double[numChoices];
        double divisor = 0;
        for (int i = 1; i <= numChoices; i++) {
            divisor += 1.0 / Math.pow((double) i, (double) zipfSkew);
        }
        for (int i = 1; i <= numChoices; i++) {
            probDist[i - 1] = (1.0 / Math.pow((double) i, (double) zipfSkew)) / divisor;
        }
        return probDist;
    }

    public static int[] getCumulIntRanges(double[] probDist) {
        int[] opRanges = new int[probDist.length];
        if (opRanges.length > 1) {
            opRanges[0] = (int) Math.floor(Integer.MAX_VALUE * probDist[0]);
            for (int i = 1; i < opRanges.length - 1; i++) {
                opRanges[i] = opRanges[i - 1] + (int) Math.floor(Integer.MAX_VALUE * probDist[i]);
            }
            opRanges[opRanges.length - 1] = Integer.MAX_VALUE;
        } else {
            opRanges[0] = Integer.MAX_VALUE;
        }
        return opRanges;
    }

    public static int choose(int[] cumulIntRanges, int randomInt) {
        int rndVal = Math.abs(randomInt);
        int ix = Arrays.binarySearch(cumulIntRanges, rndVal);
        if (ix < 0) {
            ix = -ix - 1;
        }
        return ix;
    }

}
