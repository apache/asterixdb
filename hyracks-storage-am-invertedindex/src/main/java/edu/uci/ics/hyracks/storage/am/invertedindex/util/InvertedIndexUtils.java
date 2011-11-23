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

package edu.uci.ics.hyracks.storage.am.invertedindex.util;

import edu.uci.ics.hyracks.api.dataflow.value.ITypeTrait;
import edu.uci.ics.hyracks.api.dataflow.value.TypeTrait;

public class InvertedIndexUtils {
	// Type traits to be appended to the token type trait which finally form the BTree field type traits.
	private static final ITypeTrait[] btreeValueTypeTraits = new ITypeTrait[4];
	static {
		// startPageId
		btreeValueTypeTraits[0] = new TypeTrait(4);
        // endPageId
		btreeValueTypeTraits[1] = new TypeTrait(4);
        // startOff
		btreeValueTypeTraits[2] = new TypeTrait(4);
        // numElements
		btreeValueTypeTraits[3] = new TypeTrait(4);
	}
	
	public static ITypeTrait[] getBTreeTypeTraits(ITypeTrait[] tokenTypeTraits) {
		ITypeTrait[] btreeTypeTraits = new ITypeTrait[tokenTypeTraits.length + btreeValueTypeTraits.length];
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
