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
package org.apache.hyracks.storage.am.lsm.btree.impls;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.storage.am.common.api.ICursorInitialState;
import org.apache.hyracks.storage.am.common.api.ISearchPredicate;
import org.apache.hyracks.storage.am.common.api.IndexException;
import org.apache.hyracks.storage.am.common.tuples.PermutingTupleReference;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndexOperationContext;

public class LSMBTreeWithBuddySortedCursor extends
		LSMBTreeWithBuddyAbstractCursor {
	// TODO: This class can be removed and instead use a search cursor that uses
	// a logic similar
	// to the one in LSMRTreeWithAntiMatterTuplesSearchCursor
	private boolean[] depletedBtreeCursors;
	private int foundIn = -1;
	private PermutingTupleReference buddyBtreeTuple;

	public LSMBTreeWithBuddySortedCursor(ILSMIndexOperationContext opCtx,
			int[] buddyBTreeFields) throws HyracksDataException {
		super(opCtx);
		this.buddyBtreeTuple = new PermutingTupleReference(buddyBTreeFields);
		reset();
	}

	public ILSMIndexOperationContext getOpCtx() {
		return opCtx;
	}

	@Override
	public void reset() throws HyracksDataException {
		depletedBtreeCursors = new boolean[numberOfTrees];
		foundNext = false;
		try {
			for (int i = 0; i < numberOfTrees; i++) {
				btreeCursors[i].reset();
				try {
					btreeAccessors[i].search(btreeCursors[i],
							btreeRangePredicate);
				} catch (IndexException e) {
					throw new HyracksDataException(e);
				}
				if (btreeCursors[i].hasNext()) {
					btreeCursors[i].next();
				} else {
					depletedBtreeCursors[i] = true;
				}
			}
		} catch (IndexException e) {
			e.printStackTrace();
			throw new HyracksDataException(
					"error while reseting the btrees of the lsm btree with buddy btree",
					e);
		} finally {
			if (open) {
				lsmHarness.endSearch(opCtx);
			}
		}
	}

	@Override
	public boolean hasNext() throws HyracksDataException, IndexException {
		while (!foundNext) {
			frameTuple = null;

			if (foundIn != -1) {
				if (btreeCursors[foundIn].hasNext()) {
					btreeCursors[foundIn].next();
				} else {
					depletedBtreeCursors[foundIn] = true;
				}
			}

			foundIn = -1;
			for (int i = 0; i < numberOfTrees; i++) {
				if (depletedBtreeCursors[i])
					continue;

				if (frameTuple == null) {
					frameTuple = btreeCursors[i].getTuple();
					foundIn = i;
					continue;
				}

				if (btreeCmp.compare(frameTuple, btreeCursors[i].getTuple()) > 0) {
					frameTuple = btreeCursors[i].getTuple();
					foundIn = i;
				}
			}

			if (foundIn == -1)
				return false;

			boolean killed = false;
			buddyBtreeTuple.reset(frameTuple);
			for (int i = 0; i < foundIn; i++) {
				try {
					buddyBtreeCursors[i].reset();
					buddyBtreeRangePredicate.setHighKey(buddyBtreeTuple, true);
					btreeRangePredicate.setLowKey(buddyBtreeTuple, true);
					btreeAccessors[i].search(btreeCursors[i],
							btreeRangePredicate);
				} catch (IndexException e) {
					throw new HyracksDataException(e);
				}
				try {
					if (btreeCursors[i].hasNext()) {
						killed = true;
						break;
					}
				} finally {
					btreeCursors[i].close();
				}
			}
			if (!killed) {
				foundNext = true;
			}
		}

		return true;
	}

	@Override
	public void next() throws HyracksDataException {
		foundNext = false;
	}

	@Override
	public void open(ICursorInitialState initialState,
			ISearchPredicate searchPred) throws HyracksDataException,
			IndexException {
		super.open(initialState, searchPred);

		depletedBtreeCursors = new boolean[numberOfTrees];
		foundNext = false;
		for (int i = 0; i < numberOfTrees; i++) {
			btreeCursors[i].reset();
			try {
				btreeAccessors[i].search(btreeCursors[i], btreeRangePredicate);
			} catch (IndexException e) {
				throw new HyracksDataException(e);
			}
			if (btreeCursors[i].hasNext()) {
				btreeCursors[i].next();
			} else {
				depletedBtreeCursors[i] = true;
			}
		}
	}

}