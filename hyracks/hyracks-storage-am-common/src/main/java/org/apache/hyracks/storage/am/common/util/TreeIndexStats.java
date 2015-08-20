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
package edu.uci.ics.hyracks.storage.am.common.util;

import java.text.DecimalFormat;

import edu.uci.ics.hyracks.storage.am.common.api.IFreePageManager;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexFrame;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexMetaDataFrame;

public class TreeIndexStats {

	private TreeIndexNodeTypeStats rootStats = new TreeIndexNodeTypeStats();
	private TreeIndexNodeTypeStats interiorStats = new TreeIndexNodeTypeStats();
	private TreeIndexNodeTypeStats leafStats = new TreeIndexNodeTypeStats();

	private int freePages = 0;
	private int metaPages = 0;
	private int treeLevels = 0;

	public void begin() {
		rootStats.clear();
		interiorStats.clear();
		leafStats.clear();
		freePages = 0;
		metaPages = 0;
		treeLevels = 0;
	}

	public void addRoot(ITreeIndexFrame frame) {
		treeLevels = frame.getLevel() + 1;
		rootStats.add(frame);
	}

	public void add(ITreeIndexFrame frame) {
		if (frame.isLeaf()) {
			leafStats.add(frame);
		} else if (frame.isInterior()) {
			interiorStats.add(frame);
		}
	}

	public void add(ITreeIndexMetaDataFrame metaFrame,
			IFreePageManager freePageManager) {
		if (freePageManager.isFreePage(metaFrame)) {
			freePages++;
		} else if (freePageManager.isMetaPage(metaFrame)) {
			metaPages++;
		}
	}

	public void end() {
		// nothing here currently
	}

	@Override
	public String toString() {
		StringBuilder strBuilder = new StringBuilder();
		DecimalFormat df = new DecimalFormat("#####.##");

		strBuilder.append("TREE LEVELS:  " + treeLevels + "\n");
		strBuilder.append("FREE PAGES :  " + freePages + "\n");
		strBuilder.append("META PAGES :  " + metaPages + "\n");
		long totalPages = interiorStats.getNumPages() + leafStats.getNumPages()
				+ freePages + metaPages;
		strBuilder.append("TOTAL PAGES : " + totalPages + "\n");

		strBuilder.append("\n");
		strBuilder.append("ROOT STATS" + "\n");
		strBuilder
				.append("NUM TUPLES:      " + rootStats.getNumTuples() + "\n");
		strBuilder.append("FILL FACTOR    : "
				+ df.format(rootStats.getAvgFillFactor()) + "\n");

		if (interiorStats.getNumPages() > 0) {
			strBuilder.append("\n");
			strBuilder.append("INTERIOR STATS" + "\n");
			strBuilder.append("NUM PAGES:       " + interiorStats.getNumPages()
					+ "\n");
			strBuilder.append("NUM TUPLES:      "
					+ interiorStats.getNumTuples() + "\n");
			strBuilder.append("AVG TUPLES/PAGE: "
					+ df.format(interiorStats.getAvgNumTuples()) + "\n");
			strBuilder.append("AVG FILL FACTOR: "
					+ df.format(interiorStats.getAvgFillFactor()) + "\n");
		}

		if (leafStats.getNumPages() > 0) {
			strBuilder.append("\n");
			strBuilder.append("LEAF STATS" + "\n");
			strBuilder.append("NUM PAGES:       "
					+ df.format(leafStats.getNumPages()) + "\n");
			strBuilder.append("NUM TUPLES:      "
					+ df.format(leafStats.getNumTuples()) + "\n");
			strBuilder.append("AVG TUPLES/PAGE: "
					+ df.format(leafStats.getAvgNumTuples()) + "\n");
			strBuilder.append("AVG FILL FACTOR: "
					+ df.format(leafStats.getAvgFillFactor()) + "\n");
		}

		return strBuilder.toString();
	}

	public class TreeIndexNodeTypeStats {
		private long numTuples;
		private long sumTuplesSizes;
		private long numPages;
		private double sumFillFactors;

		public void clear() {
			numTuples = 0;
			sumTuplesSizes = 0;
			numPages = 0;
		}

		public void add(ITreeIndexFrame frame) {
			numPages++;
			numTuples += frame.getTupleCount();
			sumFillFactors += (double) (frame.getBuffer().capacity() - frame
					.getTotalFreeSpace())
					/ (double) frame.getBuffer().capacity();
		}

		public long getNumTuples() {
			return numTuples;
		}

		public long getSumTupleSizes() {
			return sumTuplesSizes;
		}

		public long getNumPages() {
			return numPages;
		}

		public double getAvgNumTuples() {
			return (double) numTuples / (double) numPages;
		}

		public double getAvgTupleSize() {
			return (double) sumTuplesSizes / (double) numTuples;
		}

		public double getAvgFillFactor() {
			return sumFillFactors / numPages;
		}
	}

}
