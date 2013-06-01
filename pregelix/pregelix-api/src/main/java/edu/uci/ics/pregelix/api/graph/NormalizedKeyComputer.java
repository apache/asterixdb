/*
 * Copyright 2009-2013 by The Regents of the University of California
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
package edu.uci.ics.pregelix.api.graph;

/**
 * Users can extend this interface to speedup the performance, e.g., the Alpha-sort optimization for cache locality.
 * The normalized key is an unsigned integer (represented by a signed integer, though) obtained from the binary represetnation
 * of the corresponding vertex id.
 * Usually the normalized key can be obtained from the prefix bytes of the vertex id bytes.
 * 
 * @author yingyib
 */
public interface NormalizedKeyComputer {

    /**
     * Get the normalized key from the byte region of a vertex id.
     * The following three parameters represent the byte region of a vertex id.
     * 
     * @param data
     * @param start
     * @param len
     * @return the normalized key.
     */
    public int getNormalizedKey(byte[] data, int start, int len);
}
