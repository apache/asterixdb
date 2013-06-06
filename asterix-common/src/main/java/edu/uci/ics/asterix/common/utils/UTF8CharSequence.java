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
package edu.uci.ics.asterix.common.utils;

import edu.uci.ics.hyracks.data.std.api.IValueReference;
import edu.uci.ics.hyracks.data.std.primitive.UTF8StringPointable;

public class UTF8CharSequence implements CharSequence {

	private int start;
	private int len;
	private char[] buf;

	public UTF8CharSequence(IValueReference valueRef, int start) {
		reset(valueRef, start);
	}

	public UTF8CharSequence() {
	}

	@Override
	public char charAt(int index) {
		if (index >= len || index < 0) {
			throw new IndexOutOfBoundsException("No index " + index
					+ " for string of length " + len);
		}
		return buf[index];
	}

	@Override
	public int length() {
		return len;
	}

	@Override
	public CharSequence subSequence(int start, int end) {
		UTF8CharSequence carSeq = new UTF8CharSequence();
		carSeq.len = end - start;
		if (end != start) {
			carSeq.buf = new char[carSeq.len];
			System.arraycopy(buf, start, carSeq.buf, 0, carSeq.len);
		}
		return carSeq;
	}

	public void reset(IValueReference valueRef, int start) {
		this.start = start;
		resetLength(valueRef);
		if (buf == null || buf.length < len) {
			buf = new char[len];
		}
		int sStart = start + 2;
		int c = 0;
		int i = 0;
		byte[] bytes = valueRef.getByteArray();
		while (c < len) {
			buf[i++] = UTF8StringPointable.charAt(bytes, sStart + c);
			c += UTF8StringPointable.charSize(bytes, sStart + c);
		}

	}

	private void resetLength(IValueReference valueRef) {
		this.len = UTF8StringPointable.getUTFLength(valueRef.getByteArray(),
				start);
	}

	@Override
	public String toString() {
		StringBuffer bf = new StringBuffer();
		if (buf != null) {
			for (int i = 0; i < buf.length; i++) {
				bf.append(buf[i]);
			}
		}
		return new String(bf);
	}

}
