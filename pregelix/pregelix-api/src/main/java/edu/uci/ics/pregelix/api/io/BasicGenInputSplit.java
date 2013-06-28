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

package edu.uci.ics.pregelix.api.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

/**
 * This InputSplit will not give any ordering or location data. It is used
 * internally by BspInputFormat (which determines how many tasks to run the
 * application on). Users should not use this directly.
 */
public class BasicGenInputSplit extends FileSplit implements Writable,
		Serializable {
	private static final long serialVersionUID = 1L;
	/** Number of splits */
	private int numSplits = -1;
	/** Split index */
	private int splitIndex = -1;

	public BasicGenInputSplit() {
		super(null, 0, 0, null);
	}

	public BasicGenInputSplit(int splitIndex, int numSplits) {
		super(null, 0, 0, null);
		this.splitIndex = splitIndex;
		this.numSplits = numSplits;
	}

	@Override
	public long getLength() {
		return 0;
	}

	@Override
	public String[] getLocations() throws IOException {
		return new String[] {};
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		splitIndex = in.readInt();
		numSplits = in.readInt();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(splitIndex);
		out.writeInt(numSplits);
	}

	public int getSplitIndex() {
		return splitIndex;
	}

	public int getNumSplits() {
		return numSplits;
	}

	@Override
	public String toString() {
		return "'" + getClass().getCanonicalName() + ", index="
				+ getSplitIndex() + ", num=" + getNumSplits();
	}
}
