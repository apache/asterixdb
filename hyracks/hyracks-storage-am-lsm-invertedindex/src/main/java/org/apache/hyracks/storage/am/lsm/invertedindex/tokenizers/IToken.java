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

package edu.uci.ics.hyracks.storage.am.lsm.invertedindex.tokenizers;

import java.io.IOException;

import edu.uci.ics.hyracks.data.std.util.GrowableArray;

public interface IToken {
	public byte[] getData();

	public int getLength();

	public int getStart();

	public int getTokenLength();

	public void reset(byte[] data, int start, int length, int tokenLength,
			int tokenCount);

	public void serializeToken(GrowableArray out) throws IOException;

	public void serializeTokenCount(GrowableArray out) throws IOException;
}
