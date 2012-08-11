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

package edu.uci.ics.hyracks.storage.am.lsm.invertedindex.util;

import edu.uci.ics.hyracks.api.dataflow.value.ITypeTraits;
import edu.uci.ics.hyracks.data.std.primitive.IntegerPointable;

public class InvertedIndexUtils {
    // Type traits to be appended to the token type trait which finally form the BTree field type traits.
    private static final ITypeTraits[] btreeValueTypeTraits = new ITypeTraits[4];
    static {
        // startPageId
        btreeValueTypeTraits[0] = IntegerPointable.TYPE_TRAITS;
        // endPageId
        btreeValueTypeTraits[1] = IntegerPointable.TYPE_TRAITS;
        // startOff
        btreeValueTypeTraits[2] = IntegerPointable.TYPE_TRAITS;
        // numElements
        btreeValueTypeTraits[3] = IntegerPointable.TYPE_TRAITS;
    }

    public static ITypeTraits[] getBTreeTypeTraits(ITypeTraits[] tokenTypeTraits) {
        ITypeTraits[] btreeTypeTraits = new ITypeTraits[tokenTypeTraits.length + btreeValueTypeTraits.length];
        // Set key type traits.
        for (int i = 0; i < tokenTypeTraits.length; i++) {
            btreeTypeTraits[i] = tokenTypeTraits[i];
        }
        // Set value type traits.
        for (int i = 0; i < btreeValueTypeTraits.length; i++) {
            btreeTypeTraits[i + tokenTypeTraits.length] = btreeValueTypeTraits[i];
        }
        return btreeTypeTraits;
    }
}
