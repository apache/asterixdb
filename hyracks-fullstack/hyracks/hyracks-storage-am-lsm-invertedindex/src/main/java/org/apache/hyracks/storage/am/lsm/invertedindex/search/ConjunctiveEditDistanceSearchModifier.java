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

package org.apache.hyracks.storage.am.lsm.invertedindex.search;

public class ConjunctiveEditDistanceSearchModifier extends EditDistanceSearchModifier {

    public ConjunctiveEditDistanceSearchModifier(int gramLength, int edThresh) {
        super(gramLength, edThresh);
    }

    @Override
    public String toString() {
        return "Conjunctive Edit Distance Search Modifier, GramLen: " + gramLength + ", Threshold: " + edThresh;
    }

    @Override
    public short getNumTokensLowerBound(short numQueryTokens) {
        return -1;
    }

    @Override
    public short getNumTokensUpperBound(short numQueryTokens) {
        return -1;
    }
}
