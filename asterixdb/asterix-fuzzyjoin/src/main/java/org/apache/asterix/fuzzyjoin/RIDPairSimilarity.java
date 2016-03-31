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

public class RIDPairSimilarity {
    public int rid1, rid2;
    public float similarity;

    public RIDPairSimilarity() {
    }

    public RIDPairSimilarity(int rid1, int rid2, float similarity) {
        set(rid1, rid2, similarity);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        RIDPairSimilarity r = (RIDPairSimilarity) o;
        return rid1 == r.rid1 && rid2 == r.rid2;
    }

    @Override
    public int hashCode() {
        return rid1 * rid2 * (rid1 - rid2);
    }

    public void set(int rid1, int rid2, float similarity) {
        this.rid1 = rid1;
        this.rid2 = rid2;
        this.similarity = similarity;
    }

    @Override
    public String toString() {
        return "{(" + rid1 + ", " + rid2 + "), " + similarity + "}";
    }
}
