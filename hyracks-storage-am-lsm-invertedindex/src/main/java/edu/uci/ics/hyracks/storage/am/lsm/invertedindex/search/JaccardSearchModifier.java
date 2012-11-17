/*
 * Copyright 2009-2010 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package edu.uci.ics.hyracks.storage.am.lsm.invertedindex.search;

import edu.uci.ics.hyracks.storage.am.lsm.invertedindex.api.IInvertedIndexSearchModifier;

public class JaccardSearchModifier implements IInvertedIndexSearchModifier {

    private float jaccThresh;

    public JaccardSearchModifier(float jaccThresh) {
        this.jaccThresh = jaccThresh;
    }

    @Override
    public int getOccurrenceThreshold(int numQueryTokens) {
        return Math.max((int) Math.floor((float) numQueryTokens * jaccThresh), 1);
    }

    @Override
    public int getNumPrefixLists(int numQueryTokens) {
        if (numQueryTokens == 0) {
            return 0;
        }
        return numQueryTokens - getOccurrenceThreshold(numQueryTokens) + 1;
    }

    @Override
    public int getNumTokensLowerBound(int numQueryTokens) {
        return (int) Math.floor(numQueryTokens * jaccThresh);
    }

    @Override
    public int getNumTokensUpperBound(int numQueryTokens) {
        return (int) Math.ceil(numQueryTokens / jaccThresh);
    }
    
    public float getJaccThresh() {
        return jaccThresh;
    }

    public void setJaccThresh(float jaccThresh) {
        this.jaccThresh = jaccThresh;
    }

    @Override
    public String toString() {
        return "Jaccard Search Modifier, Threshold: " + jaccThresh;
    }
}
